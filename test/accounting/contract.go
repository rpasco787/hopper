package accounting

import (
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Messages the conformance judges read beyond accounting.go's.
const (
	msgLeaseNotConfirmed = "lease not confirmed; leaving the job to be redelivered"
	msgHeartbeatRejected = "heartbeat rejected; cancelling the handler"
	msgExpiredLocally    = "lease expired locally; cancelling the handler"
	msgConfirmRefused    = "job confirmation refused; someone else may have it now"
	msgLeaseRanOut       = "job left unconfirmed; its lease ran out and it will be redelivered"
)

// terminalMessages end a delivery the SDK took: exactly one of them is the
// last line a started or unstarted delivery logs, written before its capacity
// token goes back (worker/confirm.go logConfirmation, worker/execute.go:100,
// the Java SDK's Execution).
var terminalMessages = map[string]bool{
	msgJobConfirmed:   true,
	msgConfirmRefused: true,
	msgLeaseRanOut:    true,
	"job left unconfirmed; the worker was shutting down":           true,
	"handler returned after the shutdown deadline; not confirming": true,
}

// proc is one process's lines, split as Reconcile splits them: each "worker
// starting" begins a process, and lines before the first belong to process 0.
// cut is Reconcile's Process.Cut: followed by another process in the same pod
// without having logged "worker stopped".
type proc struct {
	n        int
	starting *Line // its "worker starting" line; nil for process 0
	lines    []Line
	stopped  bool
	cut      bool
}

func processesOf(l Log) []proc {
	var ps []proc
	for i := range l.Lines {
		line := l.Lines[i]
		if line.Msg == msgWorkerStarting {
			n := 1
			if len(ps) > 0 {
				n = ps[len(ps)-1].n + 1
			}
			ps = append(ps, proc{n: n, starting: &l.Lines[i]})
			continue
		}
		if len(ps) == 0 {
			ps = append(ps, proc{n: 0})
		}
		p := &ps[len(ps)-1]
		p.lines = append(p.lines, line)
		if line.Msg == msgWorkerStopped {
			p.stopped = true
		}
	}
	for i := 0; i < len(ps)-1; i++ {
		ps[i].cut = !ps[i].stopped
	}
	return ps
}

// deliveryKey names one delivery by its logged ids, unparsed.
type deliveryKey struct{ job, lease string }

// isStructural reports whether a line is one the contract orders: the lines a
// delivery's shape is made of. Other lines of a delivery (heartbeat losses,
// debug retries) may fall anywhere between its first line and its terminal one.
func isStructural(msg string) bool {
	switch msg {
	case msgJobStarted, msgHandlerFinished, msgUnstartedNack, msgLeaseNotConfirmed:
		return true
	}
	return terminalMessages[msg]
}

// opens reports whether a line opens a delivery for the concurrency bound.
func opens(msg string) bool { return msg == msgJobStarted || msg == msgUnstartedNack }

// delivery is one job under one lease in one process: the structural lines it
// logged, in file order.
type delivery struct {
	pod        string
	proc       int
	cut        bool // its process was followed by another without a clean stop
	job        uuid.UUID
	lease      string
	structural []Line
}

func (d *delivery) name() string {
	return fmt.Sprintf("pod %s process %d job %s lease %s", d.pod, d.proc, d.job, d.lease)
}

func (d *delivery) count(match func(string) bool) (n, first int) {
	first = -1
	for i, l := range d.structural {
		if match(l.Msg) {
			if n == 0 {
				first = i
			}
			n++
		}
	}
	return n, first
}

func is(msg string) func(string) bool { return func(m string) bool { return m == msg } }
func isTerminal(m string) bool        { return terminalMessages[m] }

// terminal is the delivery's first terminal line, if it logged one.
func (d *delivery) terminal() (Line, bool) {
	for _, l := range d.structural {
		if terminalMessages[l.Msg] {
			return l, true
		}
	}
	return Line{}, false
}

func (d *delivery) endedOnAck() bool {
	t, ok := d.terminal()
	return ok && t.Msg == msgJobConfirmed && t.Operation == "ack"
}

// shape checks one delivery against §4's three forms. final reports that it
// is the lease a completed job's row names: a missing terminal line is then
// reported once, by the row's rule, which says what it means for the Ack.
func (d *delivery) shape(final bool) []string {
	var p []string
	bad := func(format string, args ...any) { p = append(p, d.name()+": "+fmt.Sprintf(format, args...)) }

	started, startedAt := d.count(is(msgJobStarted))
	finished, finishedAt := d.count(is(msgHandlerFinished))
	unstarted, _ := d.count(is(msgUnstartedNack))
	notConfirmed, _ := d.count(is(msgLeaseNotConfirmed))
	terminals, terminalAt := d.count(isTerminal)

	switch {
	case notConfirmed > 0:
		if len(d.structural) != 1 || notConfirmed != 1 {
			var got []string
			for _, l := range d.structural {
				got = append(got, fmt.Sprintf("%q", l.Msg))
			}
			bad("%q must stand alone, but the delivery logged %s", msgLeaseNotConfirmed, strings.Join(got, ", "))
		}
	case started > 0 && unstarted > 0:
		bad("a delivery is both started and nacked unstarted")
	case started > 0:
		if started > 1 {
			bad("job started %d times", started)
		}
		if finished > 1 {
			bad("handler finished %d times", finished)
		}
		if terminals > 1 {
			bad("%d terminal lines", terminals)
		}
		if finished > 0 && finishedAt < startedAt {
			bad("handler finished before job started")
		}
		if terminals > 0 && (finished == 0 || terminalAt < finishedAt) {
			bad("a terminal line before handler finished")
		}
		if terminals == 0 && !d.cut && !final {
			bad("started, but no terminal line before the process ended")
		}
	case unstarted > 0:
		if unstarted > 1 {
			bad("%q %d times", msgUnstartedNack, unstarted)
		}
		if terminals > 1 {
			bad("%d terminal lines", terminals)
		}
		if t, ok := d.terminal(); ok {
			if t.Msg != msgJobConfirmed || t.Operation != "nack" {
				bad("an unstarted job ended on %q (operation %q), not a confirmed nack", t.Msg, t.Operation)
			}
		} else if !d.cut && !final {
			bad("nacked unstarted, but no terminal line before the process ended")
		}
	default:
		bad("handler finished or terminal lines with no job started, nacking an unstarted job or lease not confirmed")
	}
	return p
}

// ContractProblems judges what the workers' logs must say about every
// delivery of the run's jobs, apart from what the rows prove (the Ack is its
// row: Reconcile). It implements docs/design/conformance.md, "How a run is
// judged":
//   - per process, per delivery (one job under one lease), the structural lines
//     are started (job started, handler finished, one terminal line), unstarted
//     (nacking an unstarted job, then a confirmed nack) or unchecked (lease not
//     confirmed, alone); a process that was cut may stop a delivery part way;
//   - a completed job's final lease ends on an ack job confirmed, a dead job's
//     does not, and a cancelled job is never confirmed;
//   - every line about a run job carries lease_id and queue, and every process
//     that ran a delivery logged worker starting with a positive concurrency.
//
// Only jobs in rows are judged. Problems are sorted and name their pod,
// process, job and lease.
func ContractProblems(rows []Row, logs []Log) []string {
	byID := make(map[uuid.UUID]Row, len(rows))
	for _, r := range rows {
		byID[r.ID] = r
	}
	final := func(id uuid.UUID, lease string) bool {
		r, ok := byID[id]
		return ok && r.State == "completed" && r.LeaseID != uuid.Nil && r.LeaseID.String() == lease
	}

	var p []string
	byJob := map[uuid.UUID][]*delivery{}
	for _, l := range logs {
		for _, pr := range processesOf(l) {
			// missing counts, per message and field, the lines about run jobs that
			// lack lease_id or queue, so a thousand of them make one problem.
			type gap struct {
				n     int
				first uuid.UUID
			}
			missing := map[string]*gap{}
			var order []deliveryKey
			ds := map[deliveryKey]*delivery{}
			ran := false
			for _, line := range pr.lines {
				id, err := uuid.Parse(line.JobID)
				if err != nil {
					continue
				}
				if _, ok := byID[id]; !ok {
					continue
				}
				for field, absent := range map[string]bool{"lease_id": line.LeaseID == "", "queue": line.Queue == ""} {
					if !absent {
						continue
					}
					k := line.Msg + "\x00" + field
					if missing[k] == nil {
						missing[k] = &gap{first: id}
					}
					missing[k].n++
				}
				if !isStructural(line.Msg) {
					continue
				}
				ran = true
				k := deliveryKey{line.JobID, line.LeaseID}
				d := ds[k]
				if d == nil {
					d = &delivery{pod: l.Pod, proc: pr.n, cut: pr.cut, job: id, lease: line.LeaseID}
					ds[k] = d
					order = append(order, k)
				}
				d.structural = append(d.structural, line)
			}

			for k, g := range missing {
				msg, field, _ := strings.Cut(k, "\x00")
				p = append(p, fmt.Sprintf("pod %s process %d: %d %q lines about run jobs carry no %s (first: job %s)",
					l.Pod, pr.n, g.n, msg, field, g.first))
			}
			if ran {
				switch {
				case pr.starting == nil:
					p = append(p, fmt.Sprintf("pod %s process %d: ran deliveries before any worker starting line", l.Pod, pr.n))
				case pr.starting.Concurrency == nil || *pr.starting.Concurrency <= 0:
					p = append(p, fmt.Sprintf("pod %s process %d: ran deliveries, but its worker starting line logged no positive concurrency",
						l.Pod, pr.n))
				}
			}
			for _, k := range order {
				d := ds[k]
				p = append(p, d.shape(final(d.job, d.lease))...)
				byJob[d.job] = append(byJob[d.job], d)
			}
		}
	}

	for _, r := range rows {
		if r.LeaseID == uuid.Nil {
			continue
		}
		lease := r.LeaseID.String()
		switch r.State {
		case "completed":
			for _, d := range byJob[r.ID] {
				if d.lease != lease {
					continue
				}
				t, ok := d.terminal()
				switch {
				case ok && !d.endedOnAck():
					p = append(p, fmt.Sprintf("job %s: completed under lease %s, but %s ended on %q (operation %q); it never logged its ack",
						r.ID, lease, d.name(), t.Msg, t.Operation))
				case !ok && !d.cut:
					p = append(p, fmt.Sprintf("job %s: completed under lease %s, but %s has no terminal line, so it never logged its ack",
						r.ID, lease, d.name()))
				}
			}
		case "dead":
			for _, d := range byJob[r.ID] {
				if d.lease == lease && d.endedOnAck() {
					p = append(p, fmt.Sprintf("job %s: dead, yet its final lease %s ended on an ack (%s)", r.ID, lease, d.name()))
				}
			}
		case "cancelled":
			for _, d := range byJob[r.ID] {
				for _, l := range d.structural {
					if l.Msg == msgJobConfirmed {
						p = append(p, fmt.Sprintf("job %s: cancelled, yet it was confirmed (%s) by %s", r.ID, l.Operation, d.name()))
					}
				}
			}
		}
	}
	slices.Sort(p)
	return p
}

// OpenDeliveries is the number of deliveries open at the end of the last
// process in l: each opened by its first job started or nacking an unstarted
// job line and not yet ended by a terminal line, in file order. Task 11's
// busiest worker is the one with the most.
func OpenDeliveries(l Log) int {
	ps := processesOf(l)
	if len(ps) == 0 {
		return 0
	}
	open := map[deliveryKey]bool{}
	for _, line := range ps[len(ps)-1].lines {
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
	return len(open)
}
