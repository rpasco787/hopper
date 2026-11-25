package accounting

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Outcomes of a job cancelled while leased (docs/semantics.md §10), as its
// worker logged them (docs/design/conformance.md, "Cancellation").
const (
	CancelStoppedAtRenewal = "stopped at its first renewal"
	CancelAckRefused       = "ack refused"
	CancelNeverStarted     = "never started"
	// CancelRenewalOrNeverStarted is what an Expect asks for when the cancel
	// was sent at lease time, before the start or after it.
	CancelRenewalOrNeverStarted = "stopped at its first renewal, or never started"
)

// CancelOutcome reports which outcome the logs show for job id, cancelled
// while leased with grant G, and every way they break §10: a confirmation, a
// second lease, a refused renewal outside [G/3 − 1 s, G/3 + 2 s] after the
// start, a renewal refused without stopping the handler, or no outcome at all.
func CancelOutcome(logs []Log, id string, grant time.Duration) (string, []string) {
	var p []string
	leases := map[string]bool{}
	var started, rejected time.Time
	var finished, ranOut, ackRefused, notConfirmed bool
	for _, l := range logs {
		for _, line := range l.Lines {
			if line.JobID != id {
				continue
			}
			if line.LeaseID != "" {
				leases[line.LeaseID] = true
			}
			switch line.Msg {
			case msgJobStarted:
				started = line.Time
			case msgHandlerFinished:
				finished = true
			case msgHeartbeatRejected:
				rejected = line.Time
			case msgLeaseRanOut:
				ranOut = true
			case msgConfirmRefused:
				ackRefused = ackRefused || line.Operation == "ack"
			case msgLeaseNotConfirmed:
				notConfirmed = true
			case msgJobConfirmed:
				p = append(p, fmt.Sprintf("job %s: lease %s was confirmed (%s) after its job was cancelled", id, line.LeaseID, line.Operation))
			}
		}
	}
	if len(leases) > 1 {
		p = append(p, fmt.Sprintf("job %s: leased %d times; a cancelled job is never retried", id, len(leases)))
	}
	switch {
	case started.IsZero() && notConfirmed:
		return CancelNeverStarted, p
	case started.IsZero():
		return "", append(p, fmt.Sprintf("job %s: no worker logged its start or a refused ownership check", id))
	case !rejected.IsZero():
		if after := rejected.Sub(started); after < grant/3-time.Second || after > grant/3+2*time.Second {
			p = append(p, fmt.Sprintf("job %s: its renewal was refused %s after the handler started; G/3 is %s", id, after, grant/3))
		}
		if !finished || !ranOut {
			p = append(p, fmt.Sprintf("job %s: a renewal was refused, but the handler was not stopped and left unconfirmed", id))
		}
		return CancelStoppedAtRenewal, p
	case ackRefused:
		return CancelAckRefused, p
	}
	return "", append(p, fmt.Sprintf("job %s: started, but neither a renewal nor its Ack learned of the cancellation", id))
}

// ExpiredLocally judges job id's delivery that was running when an outage
// began at from, for a lease of grant G (docs/design/conformance.md, "Local
// expiry"). Its deadline is its last successful renewal's send, or its start's
// evidence, plus G less the margin (min(0.5 s, G/10)); that instant was at most
// G/3 before the outage began. So the SDK must log "lease expired locally;
// cancelling the handler" in (G − margin − G/3 − 1 s, G − margin + 1 s] after
// from, "handler finished" within 2 s of that, and leave the lease unconfirmed,
// never confirming it.
func ExpiredLocally(logs []Log, id string, from time.Time, grant time.Duration) []string {
	margin := min(500*time.Millisecond, grant/10)
	lo, hi := grant-margin-grant/3-time.Second, grant-margin+time.Second

	// The delivery that was running at the outage: the latest start before it.
	var lease string
	var startedAt time.Time
	for _, l := range logs {
		for _, line := range l.Lines {
			if line.JobID == id && line.Msg == msgJobStarted && line.Time.Before(from) &&
				(lease == "" || line.Time.After(startedAt)) {
				lease, startedAt = line.LeaseID, line.Time
			}
		}
	}
	if lease == "" {
		return []string{fmt.Sprintf("job %s: no handler started before the outage at %s", id, from.Format(time.RFC3339Nano))}
	}

	var expiredAt, finishedAt time.Time
	var ranOut, confirmed bool
	for _, l := range logs {
		for _, line := range l.Lines {
			if line.JobID != id || line.LeaseID != lease {
				continue
			}
			switch line.Msg {
			case msgExpiredLocally:
				if expiredAt.IsZero() {
					expiredAt = line.Time
				}
			case msgHandlerFinished:
				if finishedAt.IsZero() {
					finishedAt = line.Time
				}
			case msgLeaseRanOut:
				ranOut = true
			case msgJobConfirmed:
				confirmed = true
			}
		}
	}

	var p []string
	if confirmed {
		p = append(p, fmt.Sprintf("job %s: lease %s was confirmed after it expired locally", id, lease))
	}
	if expiredAt.IsZero() {
		return append(p, fmt.Sprintf("job %s: lease %s never expired locally; its handler was not stopped at the deadline", id, lease))
	}
	if after := expiredAt.Sub(from); after <= lo || after > hi {
		p = append(p, fmt.Sprintf("job %s: lease expired locally %s after the outage began; its deadline falls in (%s, %s]", id, after, lo, hi))
	}
	switch {
	case finishedAt.IsZero():
		p = append(p, fmt.Sprintf("job %s: lease %s expired locally, but its handler never finished", id, lease))
	case finishedAt.Before(expiredAt) || finishedAt.Sub(expiredAt) > 2*time.Second:
		p = append(p, fmt.Sprintf("job %s: handler finished %s after the lease expired locally; want within 2s", id, finishedAt.Sub(expiredAt)))
	}
	if !ranOut {
		p = append(p, fmt.Sprintf("job %s: lease %s expired locally but did not end on %q", id, lease, msgLeaseRanOut))
	}
	return p
}

// DeadlineCounts is what a "shutdown deadline reached" line says was abandoned.
type DeadlineCounts struct {
	Deliveries, Handlers, Acquiring, Confirming int
}

// DeadlineLine requires exactly one "shutdown deadline reached" in l, naming
// all four counts as integers, and returns them (docs/semantics.md §11 item 4).
func DeadlineLine(l Log) (DeadlineCounts, []string) {
	var found []Line
	for _, line := range l.Lines {
		if line.Msg == msgDeadlineReached {
			found = append(found, line)
		}
	}
	switch len(found) {
	case 0:
		return DeadlineCounts{}, []string{fmt.Sprintf("pod %s: no shutdown deadline reached line", l.Pod)}
	case 1:
	default:
		return DeadlineCounts{}, []string{fmt.Sprintf("pod %s: %d shutdown deadline reached lines, want one", l.Pod, len(found))}
	}
	var p []string
	d := found[0]
	var c DeadlineCounts
	for _, f := range []struct {
		key string
		in  *int
		out *int
	}{
		{"active_deliveries", d.ActiveDeliveries, &c.Deliveries},
		{"running_handlers", d.RunningHandlers, &c.Handlers},
		{"acquiring", d.Acquiring, &c.Acquiring},
		{"confirming", d.Confirming, &c.Confirming},
	} {
		if f.in == nil {
			p = append(p, fmt.Sprintf("pod %s: the shutdown deadline reached line names no %s", l.Pod, f.key))
			continue
		}
		*f.out = *f.in
	}
	return c, p
}

// Modes pick the reconciliation a scenario's run must pass.
const (
	ModeExecution = "execution" // Report.ExecutionProblems
	ModeAttempts  = "attempts"  // Report.AttemptProblems
	ModeGraceful  = "graceful"  // Report.Problems
)

// Expect is what one conformance scenario requires of a run beyond the log
// contract and the concurrency bound, which every run must keep. It is saved
// beside the evidence as expect.json, so a copy is judged by exactly the
// rules the live run was (docs/design/conformance.md).
type Expect struct {
	Scenario string `json:"scenario"`
	Mode     string `json:"mode"`
	// Problems are the exact problems Mode's check must report; usually none.
	Problems []string `json:"problems,omitempty"`
	// RepeatCauses are the causes a repeated start may have; empty allows none.
	RepeatCauses []string `json:"repeat_causes,omitempty"`
	// MinRepeatGap: each repeat's job started again at least this long after it.
	MinRepeatGap time.Duration `json:"min_repeat_gap,omitempty"`
	// Kills: exactly this many processes were cut, at least one with a handler
	// unfinished, and at least that many repeats.
	Kills           int                       `json:"kills,omitempty"`
	NoExtras        bool                      `json:"no_extras,omitempty"`        // no job leased more than once
	FirstLease      []string                  `json:"first_lease,omitempty"`      // completed, attempts 1, one start
	EveryWorkerRan  bool                      `json:"every_worker_ran,omitempty"` // each pod started one of the run's jobs
	Saturated       bool                      `json:"saturated,omitempty"`        // some process reached its concurrency
	DrainedHandlers bool                      `json:"drained_handlers,omitempty"` // deliveries open when a drain began ended on job confirmed
	Rows            map[string]RowWant        `json:"rows,omitempty"`             // job id -> how its row ends
	Cancels         map[string]string         `json:"cancels,omitempty"`          // job id -> a Cancel* outcome
	Expired         []string                  `json:"expired,omitempty"`          // jobs stopped at their local deadline
	OutageStart     time.Time                 `json:"outage_start,omitzero"`      // from the Docker VM's clock
	Grant           time.Duration             `json:"grant,omitempty"`
	Deadline        map[string]DeadlineCounts `json:"deadline,omitempty"` // pod -> its deadline line
	Exits           map[string]int            `json:"exits,omitempty"`    // pod -> exit code
	Stderr          map[string]string         `json:"stderr,omitempty"`   // pod -> whole stderr, trimmed; others must be empty
}

// RowWant is how one job's row and log must end. Starts nil skips the count.
type RowWant struct {
	State     string  `json:"state"`
	Attempts  int     `json:"attempts"`
	Starts    *int    `json:"starts,omitempty"` // "job started" lines for the job, across its leases
	LastError *string `json:"last_error,omitempty"`
}

// Evidence is one scenario's run as files hold it.
type Evidence struct {
	Rows   []Row
	Stdout map[string][]byte // container -> its stdout: the JSON log
	Stderr map[string][]byte // container -> its stderr
	Exits  map[string]int    // container -> exit code, for containers the scenario stopped
}

// Judge returns every way a run breaks e, the log contract or the concurrency
// bound, sorted; nothing for a run that keeps them. A live scenario saves its
// evidence and requires Judge to return nothing, and go test -tags evidence
// judges a saved copy by the same function (docs/design/conformance.md).
func Judge(e Expect, ev Evidence) []string {
	var p []string

	// 1. The logs, one per container.
	containers := slices.Sorted(maps.Keys(ev.Stdout))
	logs := make([]Log, 0, len(containers))
	for _, c := range containers {
		l, err := Parse(c, bytes.NewReader(ev.Stdout[c]))
		if err != nil {
			p = append(p, fmt.Sprintf("container %s: reading its stdout: %v", c, err))
		}
		logs = append(logs, l)
	}

	// 2. Reconcile, in the scenario's mode, less the endings it expects.
	rep := Reconcile(ev.Rows, logs)
	var mode []string
	switch e.Mode {
	case ModeExecution:
		mode = rep.ExecutionProblems()
	case ModeAttempts:
		mode = rep.AttemptProblems()
	case ModeGraceful:
		mode = rep.Problems()
	default:
		p = append(p, fmt.Sprintf("expect %q: unknown mode %q", e.Scenario, e.Mode))
	}
	excused := map[string]bool{}
	for id, w := range e.Rows {
		if u, err := uuid.Parse(id); err == nil {
			id = u.String()
		}
		// The line commonProblems writes for a job that did not complete
		// (accounting.go: "job %s: ended %s after %d attempts").
		excused[fmt.Sprintf("job %s: ended %s after %d attempts", id, w.State, w.Attempts)] = true
	}
	got := map[string]bool{}
	for _, m := range mode {
		if !excused[m] {
			got[m] = true
		}
	}
	for _, m := range e.Problems {
		if !got[m] {
			p = append(p, fmt.Sprintf("expected problem was not reported: %q", m))
		}
	}
	for m := range got {
		if !slices.Contains(e.Problems, m) {
			p = append(p, m)
		}
	}

	// 3. stderr and exit codes. A container's stderr must be empty unless the
	// scenario names it.
	seen := map[string]bool{}
	for _, m := range []map[string][]byte{ev.Stdout, ev.Stderr} {
		for c := range m {
			seen[c] = true
		}
	}
	for c := range e.Stderr {
		seen[c] = true
	}
	for _, c := range slices.Sorted(maps.Keys(seen)) {
		have, want := strings.TrimSpace(string(ev.Stderr[c])), strings.TrimSpace(e.Stderr[c])
		if have != want {
			p = append(p, fmt.Sprintf("container %s: stderr is %q, want %q", c, have, want))
		}
	}
	for _, c := range slices.Sorted(maps.Keys(e.Exits)) {
		switch code, ok := ev.Exits[c]; {
		case !ok:
			p = append(p, fmt.Sprintf("container %s: no exit code recorded, want %d", c, e.Exits[c]))
		case code != e.Exits[c]:
			p = append(p, fmt.Sprintf("container %s: exit code %d, want %d", c, code, e.Exits[c]))
		}
	}

	// 4. Repeats, gaps and kills.
	for _, rp := range rep.Repeats {
		if !slices.Contains(e.RepeatCauses, rp.Cause) {
			p = append(p, fmt.Sprintf("job %s lease %s: started again after %s process %d with cause %q; the scenario allows %q",
				rp.ID, rp.LeaseID, rp.Pod, rp.Process, rp.Cause, e.RepeatCauses))
		}
		if e.MinRepeatGap > 0 {
			switch gap := rp.NextStartedAt.Sub(rp.StartedAt); {
			case rp.StartedAt.IsZero() || rp.NextStartedAt.IsZero():
				p = append(p, fmt.Sprintf("job %s lease %s: the log has no times to measure its repeat gap", rp.ID, rp.LeaseID))
			case gap < e.MinRepeatGap:
				p = append(p, fmt.Sprintf("job %s lease %s: repeat gap %s, want at least %s", rp.ID, rp.LeaseID, gap, e.MinRepeatGap))
			}
		}
	}
	if e.Kills > 0 {
		cut, unfinished := 0, 0
		for _, pr := range rep.Processes {
			if pr.Cut {
				cut++
				unfinished += pr.Interrupted
			}
		}
		switch {
		case cut != e.Kills:
			p = append(p, fmt.Sprintf("%d processes were cut, want %d", cut, e.Kills))
		case unfinished < 1:
			p = append(p, "the cut processes left no handler unfinished; the kill missed every running job")
		case len(rep.Repeats) < unfinished:
			p = append(p, fmt.Sprintf("%d repeats for %d unfinished handlers in the cut processes", len(rep.Repeats), unfinished))
		}
	}

	// 5. Extras, first leases, every worker.
	starts := map[uuid.UUID]int{}
	inRun := map[uuid.UUID]Row{}
	for _, r := range ev.Rows {
		inRun[r.ID] = r
	}
	for _, l := range logs {
		ran := false
		for _, line := range l.Lines {
			if id, err := uuid.Parse(line.JobID); err == nil && line.Msg == msgJobStarted {
				if _, ok := inRun[id]; ok {
					starts[id]++
					ran = true
				}
			}
		}
		if e.EveryWorkerRan && !ran {
			p = append(p, fmt.Sprintf("container %s ran none of the run's jobs", l.Pod))
		}
	}
	if e.NoExtras {
		for _, x := range rep.Extras {
			p = append(p, fmt.Sprintf("job %s: leased more than once (%d attempts), but this scenario allows no redelivery", x.ID, x.Attempts))
		}
	}
	for _, id := range e.FirstLease {
		u, err := uuid.Parse(id)
		r, ok := inRun[u]
		switch {
		case err != nil || !ok:
			p = append(p, fmt.Sprintf("job %s: no row, so no first lease to judge", id))
		case r.State != "completed" || r.Attempts != 1 || starts[u] != 1:
			p = append(p, fmt.Sprintf("job %s: not completed under its first lease (state %s, %d attempts, %d job started lines)",
				id, r.State, r.Attempts, starts[u]))
		}
	}

	// 6, 12. The bound, and saturation.
	peaks, bound := ConcurrencyProblems(logs)
	p = append(p, bound...)
	if e.Saturated && !slices.ContainsFunc(peaks, func(pk Peak) bool { return pk.Concurrency > 0 && pk.Max == pk.Concurrency }) {
		p = append(p, fmt.Sprintf("no worker process reached its concurrency: %+v", peaks))
	}

	// 7. Handlers open when a drain began.
	if e.DrainedHandlers {
		p = append(p, drainProblems(logs)...)
	}

	// 8. Rows.
	for _, id := range slices.Sorted(maps.Keys(e.Rows)) {
		w := e.Rows[id]
		u, err := uuid.Parse(id)
		r, ok := inRun[u]
		if err != nil || !ok {
			p = append(p, fmt.Sprintf("job %s: no row for it in the evidence", id))
			continue
		}
		if r.State != w.State {
			p = append(p, fmt.Sprintf("job %s: state %s, want %s", id, r.State, w.State))
		}
		if r.Attempts != w.Attempts {
			p = append(p, fmt.Sprintf("job %s: attempts %d, want %d", id, r.Attempts, w.Attempts))
		}
		if w.LastError != nil && r.LastError != *w.LastError {
			p = append(p, fmt.Sprintf("job %s: last_error %q, want %q", id, r.LastError, *w.LastError))
		}
		if w.Starts != nil && starts[u] != *w.Starts {
			p = append(p, fmt.Sprintf("job %s: want %d starts, the logs show %d job started lines", id, *w.Starts, starts[u]))
		}
	}

	// 9. Cancels.
	if len(e.Cancels) > 0 && e.Grant <= 0 {
		p = append(p, "expect: Cancels needs a Grant")
	} else {
		for _, id := range slices.Sorted(maps.Keys(e.Cancels)) {
			want := e.Cancels[id]
			outcome, cp := CancelOutcome(logs, id, e.Grant)
			p = append(p, cp...)
			ok := outcome == want || (want == CancelRenewalOrNeverStarted &&
				(outcome == CancelStoppedAtRenewal || outcome == CancelNeverStarted))
			if outcome != "" && !ok {
				p = append(p, fmt.Sprintf("job %s: its cancel ended as: %s, want %s", id, outcome, want))
			}
		}
	}

	// 10. Local expiry.
	if len(e.Expired) > 0 && e.Grant <= 0 {
		p = append(p, "expect: Expired needs a Grant")
	} else {
		for _, id := range e.Expired {
			p = append(p, ExpiredLocally(logs, id, e.OutageStart, e.Grant)...)
		}
	}

	// 11. Deadline lines.
	for _, c := range slices.Sorted(maps.Keys(e.Deadline)) {
		i := slices.IndexFunc(logs, func(l Log) bool { return l.Pod == c })
		if i < 0 {
			p = append(p, fmt.Sprintf("container %s: no log to read its shutdown deadline reached line from", c))
			continue
		}
		counts, dp := DeadlineLine(logs[i])
		p = append(p, dp...)
		if len(dp) == 0 && counts != e.Deadline[c] {
			p = append(p, fmt.Sprintf("container %s: the shutdown deadline reached line names %+v, want %+v", c, counts, e.Deadline[c]))
		}
	}

	// 12. The log contract.
	p = append(p, ContractProblems(ev.Rows, logs)...)

	slices.Sort(p)
	return p
}

// drainProblems checks that every delivery open when a process logged
// "shutdown started" ended on a confirmation, in each process that did not
// reach its deadline (one that did abandoned them by design).
func drainProblems(logs []Log) []string {
	var p []string
	for _, l := range logs {
		for _, pr := range processesOf(l) {
			at := slices.IndexFunc(pr.lines, func(x Line) bool { return x.Msg == msgShutdownStarted })
			if at < 0 || slices.ContainsFunc(pr.lines, func(x Line) bool { return x.Msg == msgDeadlineReached }) {
				continue
			}
			open := map[deliveryKey]bool{}
			for _, line := range pr.lines[:at] {
				if line.JobID == "" {
					continue
				}
				k := deliveryKey{line.JobID, line.LeaseID}
				switch {
				case opens(line.Msg):
					open[k] = true
				case terminalMessages[line.Msg]:
					delete(open, k)
				}
			}
			for k := range open {
				end := slices.IndexFunc(pr.lines[at:], func(x Line) bool {
					return x.JobID == k.job && x.LeaseID == k.lease && terminalMessages[x.Msg]
				})
				switch {
				case end < 0:
					p = append(p, fmt.Sprintf("pod %s process %d: job %s lease %s was open when the drain began and never ended",
						l.Pod, pr.n, k.job, k.lease))
				case pr.lines[at+end].Msg != msgJobConfirmed:
					p = append(p, fmt.Sprintf("pod %s process %d: job %s lease %s was open when the drain began, but ended on %q, not a confirmation",
						l.Pod, pr.n, k.job, k.lease, pr.lines[at+end].Msg))
				}
			}
		}
	}
	return p
}
