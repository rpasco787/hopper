// Package accounting reconciles what workers logged about each job with what
// the database recorded, and says whether a run executed every job exactly
// once. It is the evidence behind "zero duplicate executions"
// (docs/semantics.md §11, §15). It reads only log text and rows, so the same
// check serves kind, Compose and in-process runs.
package accounting

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/google/uuid"
)

// Messages this package reads, from worker/execute.go, worker/lifecycle.go and
// cmd/worker. If one is renamed, completed jobs show up as never started and
// the run fails, so a drifted name cannot let a run pass.
const (
	msgWorkerStarting  = "worker starting"
	msgJobStarted      = "job started"
	msgHandlerFinished = "handler finished"
	msgJobConfirmed    = "job confirmed"
	msgUnstartedNack   = "nacking an unstarted job"
	msgShutdownStarted = "shutdown started"
	msgDeadlineReached = "shutdown deadline reached"
	msgWorkerStopped   = "worker stopped"
)

// lossMessages are the SDK's own records of giving up a delivery: its lease
// was refused, ran out, or outlived the worker's drain. Each line carries the
// delivery's lease_id (worker/heartbeat.go, worker/confirm.go,
// worker/execute.go). A renamed message leaves a repeat unexplained, so the run
// fails rather than passing on a drifted name.
var lossMessages = map[string]bool{
	"lease expired locally; cancelling the handler":                      true,
	"heartbeat rejected; cancelling the handler":                         true,
	"lease expired while applying a renewal; cancelling the handler":     true,
	"job confirmation refused; someone else may have it now":             true,
	"job left unconfirmed; its lease ran out and it will be redelivered": true,
	"job left unconfirmed; the worker was shutting down":                 true,
	"handler returned after the shutdown deadline; not confirming":       true,
}

// Row is one job as the database left it.
type Row struct {
	ID       uuid.UUID
	State    string // jobs.state
	Attempts int
	// LeaseID is jobs.lease_id: the last lease granted, which for a completed
	// job is the one that completed it. uuid.Nil when never leased, or when
	// the caller did not load it; AttemptProblems needs it.
	LeaseID uuid.UUID
}

// Line holds the fields of a worker's JSON log line that accounting reads.
type Line struct {
	// Time is when the line was logged. It orders events across pods, which
	// is sound when they share a clock, as Compose containers on one Docker
	// VM and pods on one kind node do.
	Time            time.Time `json:"time"`
	Msg             string    `json:"msg"`
	JobID           string    `json:"job_id"`
	LeaseID         string    `json:"lease_id"`
	Operation       string    `json:"operation"` // "ack" or "nack", on "job confirmed"
	RunningHandlers *int      `json:"running_handlers"`
}

// Log is one worker pod's or container's captured output.
type Log struct {
	Pod   string
	Lines []Line
	// Unparsed counts non-blank lines that were not JSON log records.
	// cmd/worker writes those to stderr only when it exits with an error.
	Unparsed int
}

// Parse reads one process's output.
func Parse(pod string, r io.Reader) (Log, error) {
	l := Log{Pod: pod}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		var line Line
		if err := json.Unmarshal(b, &line); err != nil || line.Msg == "" {
			l.Unparsed++
			continue
		}
		l.Lines = append(l.Lines, line)
	}
	return l, sc.Err()
}

// Shutdown is what one process logged about its own graceful shutdown.
type Shutdown struct {
	Pod             string
	RunningHandlers int  // handlers running when the drain began
	Stopped         bool // logged "worker stopped": the drain finished inside its budget
	DeadlineReached bool
}

// Extra is a completed job with lost leases: leased more than once, or logged
// as nacked unstarted. Its final lease completed it, so each of the
// Attempts-1 earlier leases was lost to a delivery nacked unstarted during
// shutdown (logged), a handler that started and did not complete it (logged,
// and a duplicate start), or a delivery the worker never received, whose
// lease the reaper took back (logged nowhere). The first and last are
// redeliveries, not duplicate executions (§11.5).
type Extra struct {
	ID             uuid.UUID
	Attempts       int
	UnstartedNacks int
	// Restarted counts the earlier leases whose handler started: every start
	// of the job's handler but one.
	Restarted int
	// NeverReceived is Attempts-1-UnstartedNacks-Restarted: the lost leases no
	// log line explains, which are deliveries the worker never received. It
	// is inferred, not measured. Below zero, the log and the row disagree.
	NeverReceived int
}

// Process is one worker process in a pod's log. A pod logs several when its
// container is started again in place, as the chaos suite does after killing
// it: each "worker starting" line begins a process. Lines before the first
// belong to process 0, one that started before the capture did.
type Process struct {
	Pod    string
	N      int
	Starts int // handler starts it logged for the run's jobs
	// Interrupted counts those whose handler it never logged finishing.
	Interrupted int
	Stopped     bool // logged "worker stopped"
	// Cut is a process that ended without a clean stop, followed by another
	// process in the same pod: one that was killed, or crashed.
	Cut bool
	// Last is when it logged its last line. A cut process was killed after
	// Last and before the pod's next process logged its first line.
	Last time.Time
}

// Repeat is a handler start under a lease that did not complete its job: the
// job completed under a later lease. Cause is what the run shows about why,
// or empty when nothing does, which AttemptProblems reports.
type Repeat struct {
	ID      uuid.UUID
	LeaseID string
	Pod     string
	Process int // Process.N
	Cause   string
	// StartedAt is when this lease's handler started; NextStartedAt is when
	// the job's handler next started, under any lease. Zero when the log
	// carries no times.
	StartedAt, NextStartedAt time.Time
}

// Causes of a Repeat.
const (
	CauseNacked = "nacked" // its handler failed and the worker nacked it: an ordinary retry
	CauseLost   = "lost"   // the worker logged giving the lease up (lossMessages)
	// CauseKilled: its process was cut before confirming it, and the job's
	// handler started again only after that process's last line, so the kill
	// came first. A lease the reaper took back while its process still ran,
	// and that a later kill merely ended, is not explained by the kill.
	CauseKilled = "killed"
)

// Report is the reconciliation of one run.
type Report struct {
	Jobs, Completed int
	Duplicated      map[uuid.UUID]int // run jobs whose handler started more than once
	NeverStarted    []uuid.UUID       // completed with no captured start
	Unfinished      []Row
	Extras          []Extra
	// Sums over Extras. len(Extras) counts redelivered jobs; ExtraLeases
	// counts lost leases, of which Restarted started a handler again and the
	// rest (UnstartedNacks + NeverReceived) are redeliveries.
	ExtraLeases, UnstartedNacks, Restarted, NeverReceived int
	Shutdowns                                             []Shutdown
	Unparsed                                              map[string]int // pod -> non-JSON lines
	Processes                                             []Process
	Repeats                                               []Repeat

	// leaseProblems are the per-lease violations AttemptProblems reports.
	leaseProblems []string
}

// leaseKey names one delivery: a job under one of its leases.
type leaseKey struct {
	job   uuid.UUID
	lease string
}

// leaseSeen is what the logs say about one delivery whose handler started.
type leaseSeen struct {
	starts                        int
	process                       int       // index into Report.Processes of its first start
	startedAt                     time.Time // of its first start
	finished, acked, nacked, lost bool
}

// Reconcile matches the run's rows against every pod's log. Lines about jobs
// outside rows (cron ticks, smoke checks) are ignored.
func Reconcile(rows []Row, logs []Log) Report {
	inRun := make(map[uuid.UUID]bool, len(rows))
	for _, r := range rows {
		inRun[r.ID] = true
	}
	starts, nacks := map[uuid.UUID]int{}, map[uuid.UUID]int{}
	leases := map[leaseKey]*leaseSeen{}
	jobLeases := map[uuid.UUID][]string{} // started leases per job, in log order
	rep := Report{Jobs: len(rows), Duplicated: map[uuid.UUID]int{}, Unparsed: map[string]int{}}
	for _, l := range logs {
		if l.Unparsed > 0 {
			rep.Unparsed[l.Pod] = l.Unparsed
		}
		var sd *Shutdown
		first, proc := len(rep.Processes), -1
		current := func() int {
			if proc < 0 {
				rep.Processes = append(rep.Processes, Process{Pod: l.Pod})
				proc = len(rep.Processes) - 1
			}
			return proc
		}
		for _, line := range l.Lines {
			if line.Msg != msgWorkerStarting && proc >= 0 && !line.Time.IsZero() {
				rep.Processes[proc].Last = line.Time
			}
			if line.Msg == msgWorkerStarting {
				n := 1
				if proc >= 0 {
					n = rep.Processes[proc].N + 1
				}
				rep.Processes = append(rep.Processes, Process{Pod: l.Pod, N: n, Last: line.Time})
				proc = len(rep.Processes) - 1
				continue
			}
			id, err := uuid.Parse(line.JobID)
			ours := err == nil && inRun[id]
			key := leaseKey{id, line.LeaseID}
			switch {
			case line.Msg == msgJobStarted && ours:
				process := current()
				if !line.Time.IsZero() {
					rep.Processes[process].Last = line.Time
				}
				starts[id]++
				ls := leases[key]
				if ls == nil {
					ls = &leaseSeen{process: process, startedAt: line.Time}
					leases[key] = ls
					jobLeases[id] = append(jobLeases[id], line.LeaseID)
				}
				ls.starts++
				rep.Processes[process].Starts++
			case line.Msg == msgUnstartedNack && ours:
				nacks[id]++
			case line.Msg == msgHandlerFinished && ours && leases[key] != nil:
				leases[key].finished = true
			case line.Msg == msgJobConfirmed && ours && leases[key] != nil:
				switch line.Operation {
				case "ack":
					leases[key].acked = true
				case "nack":
					leases[key].nacked = true
				}
			case lossMessages[line.Msg] && ours && leases[key] != nil:
				leases[key].lost = true
			case line.Msg == msgShutdownStarted:
				sd = &Shutdown{Pod: l.Pod}
				if line.RunningHandlers != nil {
					sd.RunningHandlers = *line.RunningHandlers
				}
			case line.Msg == msgDeadlineReached:
				if sd != nil {
					sd.DeadlineReached = true
				}
			case line.Msg == msgWorkerStopped:
				rep.Processes[current()].Stopped = true
				if sd != nil {
					sd.Stopped = true
				}
			}
		}
		if sd != nil {
			rep.Shutdowns = append(rep.Shutdowns, *sd)
		}
		// Every process but the pod's last was followed by another; one that
		// did not stop cleanly first was cut short.
		for i := first; i < len(rep.Processes)-1; i++ {
			rep.Processes[i].Cut = !rep.Processes[i].Stopped
		}
	}
	for _, ls := range leases {
		if !ls.finished {
			rep.Processes[ls.process].Interrupted++
		}
	}
	for _, r := range rows {
		n := starts[r.ID]
		if n > 1 {
			rep.Duplicated[r.ID] = n
		}
		if r.State != "completed" {
			rep.Unfinished = append(rep.Unfinished, r)
			continue
		}
		rep.Completed++
		if n == 0 {
			rep.NeverStarted = append(rep.NeverStarted, r.ID)
		}
		if lost := r.Attempts - 1; lost > 0 || nacks[r.ID] > 0 {
			e := Extra{ID: r.ID, Attempts: r.Attempts, UnstartedNacks: nacks[r.ID], Restarted: max(n-1, 0)}
			e.NeverReceived = lost - e.UnstartedNacks - e.Restarted
			rep.Extras = append(rep.Extras, e)
			rep.ExtraLeases += lost
			rep.UnstartedNacks += e.UnstartedNacks
			rep.Restarted += e.Restarted
			rep.NeverReceived += e.NeverReceived
		}
		rep.judgeLeases(r, jobLeases[r.ID], leases)
	}
	return rep
}

// judgeLeases records the per-lease violations and the repeats of one
// completed job.
func (rep *Report) judgeLeases(r Row, started []string, leases map[leaseKey]*leaseSeen) {
	if r.LeaseID == uuid.Nil {
		rep.leaseProblems = append(rep.leaseProblems,
			fmt.Sprintf("job %s: completed with no lease_id; load the rows with it to judge attempts", r.ID))
		return
	}
	final := r.LeaseID.String()
	if len(started) > r.Attempts {
		rep.leaseProblems = append(rep.leaseProblems, fmt.Sprintf(
			"job %s: %d leases started a handler but the row counts %d attempts", r.ID, len(started), r.Attempts))
	}
	if len(started) > 0 && leases[leaseKey{r.ID, final}] == nil {
		rep.leaseProblems = append(rep.leaseProblems, fmt.Sprintf(
			"job %s: completed under lease %s, which no captured process started", r.ID, final))
	}
	for _, lease := range started {
		ls := leases[leaseKey{r.ID, lease}]
		if ls.starts > 1 {
			rep.leaseProblems = append(rep.leaseProblems,
				fmt.Sprintf("job %s: lease %s started %d handlers", r.ID, lease, ls.starts))
		}
		if lease == final {
			continue
		}
		p := rep.Processes[ls.process]
		rp := Repeat{ID: r.ID, LeaseID: lease, Pod: p.Pod, Process: p.N,
			StartedAt: ls.startedAt, NextStartedAt: nextStart(r.ID, ls.startedAt, started, leases)}
		switch {
		case ls.acked:
			rep.leaseProblems = append(rep.leaseProblems, fmt.Sprintf(
				"job %s: lease %s was acked, yet the job ran again and completed under lease %s", r.ID, lease, final))
		case ls.nacked:
			rp.Cause = CauseNacked
		case ls.lost:
			rp.Cause = CauseLost
		case p.Cut && (rp.StartedAt.IsZero() || rp.NextStartedAt.IsZero()):
			rep.leaseProblems = append(rep.leaseProblems, fmt.Sprintf(
				"job %s: lease %s ran on %s process %d, which was cut, but the log has no times to show the job started again only after the cut",
				r.ID, lease, p.Pod, p.N))
		case p.Cut && !rp.NextStartedAt.After(p.Last):
			rep.leaseProblems = append(rep.leaseProblems, fmt.Sprintf(
				"job %s: lease %s ran on %s process %d, but the job started again at %s, before that process's last line at %s; the kill came later and cannot explain it",
				r.ID, lease, p.Pod, p.N, rp.NextStartedAt.Format(time.RFC3339Nano), p.Last.Format(time.RFC3339Nano)))
		case p.Cut:
			rp.Cause = CauseKilled
		default:
			rep.leaseProblems = append(rep.leaseProblems, fmt.Sprintf(
				"job %s: lease %s started on %s process %d, and no kill, nack or record of losing it explains the job running again",
				r.ID, lease, p.Pod, p.N))
		}
		rep.Repeats = append(rep.Repeats, rp)
	}
}

// nextStart is when the job's handler first started after `after`, under any
// of its leases: zero when that is unknown.
func nextStart(id uuid.UUID, after time.Time, started []string, leases map[leaseKey]*leaseSeen) time.Time {
	var next time.Time
	if after.IsZero() {
		return next
	}
	for _, lease := range started {
		at := leases[leaseKey{id, lease}].startedAt
		if at.After(after) && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}
	return next
}

// Problems lists every reason the run fails to show each job executing
// exactly once under graceful shutdown. Empty means the claim holds.
func (r Report) Problems() []string {
	p := r.ExecutionProblems()
	if !r.shutDownMidHandler() {
		p = append(p, "no worker was shut down while a handler ran; the run says nothing about graceful shutdown")
		slices.Sort(p)
	}
	return p
}

// ExecutionProblems is Problems without its demand that some worker was shut
// down while a handler ran. It judges runs whose shutdowns may all find their
// workers idle, such as an autoscaler removing workers after a drain: every
// job must still execute exactly once, and every worker that shut down must
// stop cleanly.
func (r Report) ExecutionProblems() []string {
	p := r.commonProblems()
	for id, n := range r.Duplicated {
		p = append(p, fmt.Sprintf("job %s: handler started %d times", id, n))
	}
	slices.Sort(p)
	return p
}

// AttemptProblems judges a run in which processes may die without draining,
// such as workers killed with SIGKILL. A job's handler may start more than
// once, but only as attempt accounting allows: each lease starts at most one
// handler, the row counts every lease that started one, the lease that
// completed the job started a handler, no acked lease is followed by another,
// and every other start is explained by a nack, the worker's own record of
// losing the lease, or a kill that came before the job started again
// (Repeats). Every ExecutionProblems check except the one against duplicate
// starts still applies. The rows must carry LeaseID, and the logs times.
func (r Report) AttemptProblems() []string {
	p := append(r.commonProblems(), r.leaseProblems...)
	slices.Sort(p)
	return p
}

// commonProblems are the checks ExecutionProblems and AttemptProblems share.
func (r Report) commonProblems() []string {
	var p []string
	for _, id := range r.NeverStarted {
		p = append(p, fmt.Sprintf("job %s: completed, but no worker logged its start; the log capture is incomplete", id))
	}
	for _, row := range r.Unfinished {
		p = append(p, fmt.Sprintf("job %s: ended %s after %d attempts", row.ID, row.State, row.Attempts))
	}
	for _, e := range r.Extras {
		if e.NeverReceived < 0 {
			p = append(p, fmt.Sprintf("job %s: %d unstarted nacks and %d restarted handlers logged but only %d leases before the one that completed it; the log and the row disagree",
				e.ID, e.UnstartedNacks, e.Restarted, e.Attempts-1))
		}
	}
	for pod, n := range r.Unparsed {
		p = append(p, fmt.Sprintf("pod %s: %d non-JSON lines; cmd/worker prints those only when it exits with an error", pod, n))
	}
	for _, s := range r.Shutdowns {
		if s.DeadlineReached {
			p = append(p, fmt.Sprintf("pod %s: shutdown deadline reached; work was abandoned", s.Pod))
		}
		if !s.Stopped {
			p = append(p, fmt.Sprintf("pod %s: shutdown started but the worker never logged a clean stop", s.Pod))
		}
	}
	return p
}

// shutDownMidHandler reports whether some worker began its drain with a
// handler running, which is what makes a run evidence of graceful shutdown.
func (r Report) shutDownMidHandler() bool {
	for _, s := range r.Shutdowns {
		if s.RunningHandlers > 0 {
			return true
		}
	}
	return false
}
