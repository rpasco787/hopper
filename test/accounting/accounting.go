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

	"github.com/google/uuid"
)

// Messages this package reads, from worker/execute.go, worker/lifecycle.go and
// cmd/worker. If one is renamed, completed jobs show up as never started and
// the run fails, so a drifted name cannot let a run pass.
const (
	msgJobStarted      = "job started"
	msgUnstartedNack   = "nacking an unstarted job"
	msgShutdownStarted = "shutdown started"
	msgDeadlineReached = "shutdown deadline reached"
	msgWorkerStopped   = "worker stopped"
)

// Row is one job as the database left it.
type Row struct {
	ID       uuid.UUID
	State    string // jobs.state
	Attempts int
}

// Line holds the fields of a worker's JSON log line that accounting reads.
type Line struct {
	Msg             string `json:"msg"`
	JobID           string `json:"job_id"`
	RunningHandlers *int   `json:"running_handlers"`
}

// Log is one worker process's captured output.
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
// shutdown (logged), one the worker never received, whose lease the reaper
// took back (logged nowhere), or a handler that started and was abandoned (a
// duplicate start). The first two are redeliveries, not duplicate executions
// (§11.5).
type Extra struct {
	ID             uuid.UUID
	Attempts       int
	UnstartedNacks int
	// NeverReceived is Attempts-1-UnstartedNacks: the lost leases no log line
	// explains. In a run without duplicate starts those are deliveries the
	// worker never received. It is inferred, not measured. Below zero, the log
	// and the row disagree.
	NeverReceived int
}

// Report is the reconciliation of one run.
type Report struct {
	Jobs, Completed int
	Duplicated      map[uuid.UUID]int // run jobs whose handler started more than once
	NeverStarted    []uuid.UUID       // completed with no captured start
	Unfinished      []Row
	Extras          []Extra
	// Sums over Extras. len(Extras) counts redelivered jobs; ExtraLeases
	// counts redeliveries, one per lost lease.
	ExtraLeases, UnstartedNacks, NeverReceived int
	Shutdowns                                  []Shutdown
	Unparsed                                   map[string]int // pod -> non-JSON lines
}

// Reconcile matches the run's rows against every pod's log. Lines about jobs
// outside rows (cron ticks, smoke checks) are ignored.
func Reconcile(rows []Row, logs []Log) Report {
	inRun := make(map[uuid.UUID]bool, len(rows))
	for _, r := range rows {
		inRun[r.ID] = true
	}
	starts, nacks := map[uuid.UUID]int{}, map[uuid.UUID]int{}
	rep := Report{Jobs: len(rows), Duplicated: map[uuid.UUID]int{}, Unparsed: map[string]int{}}
	for _, l := range logs {
		if l.Unparsed > 0 {
			rep.Unparsed[l.Pod] = l.Unparsed
		}
		var sd *Shutdown
		for _, line := range l.Lines {
			switch line.Msg {
			case msgJobStarted, msgUnstartedNack:
				id, err := uuid.Parse(line.JobID)
				if err != nil || !inRun[id] {
					continue
				}
				if line.Msg == msgJobStarted {
					starts[id]++
				} else {
					nacks[id]++
				}
			case msgShutdownStarted:
				sd = &Shutdown{Pod: l.Pod}
				if line.RunningHandlers != nil {
					sd.RunningHandlers = *line.RunningHandlers
				}
			case msgDeadlineReached:
				if sd != nil {
					sd.DeadlineReached = true
				}
			case msgWorkerStopped:
				if sd != nil {
					sd.Stopped = true
				}
			}
		}
		if sd != nil {
			rep.Shutdowns = append(rep.Shutdowns, *sd)
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
			e := Extra{ID: r.ID, Attempts: r.Attempts, UnstartedNacks: nacks[r.ID]}
			e.NeverReceived = lost - e.UnstartedNacks
			rep.Extras = append(rep.Extras, e)
			rep.ExtraLeases += lost
			rep.UnstartedNacks += e.UnstartedNacks
			rep.NeverReceived += e.NeverReceived
		}
	}
	return rep
}

// Problems lists every reason the run fails to show each job executing
// exactly once under graceful shutdown. Empty means the claim holds.
func (r Report) Problems() []string {
	var p []string
	for id, n := range r.Duplicated {
		p = append(p, fmt.Sprintf("job %s: handler started %d times", id, n))
	}
	for _, id := range r.NeverStarted {
		p = append(p, fmt.Sprintf("job %s: completed, but no worker logged its start; the log capture is incomplete", id))
	}
	for _, row := range r.Unfinished {
		p = append(p, fmt.Sprintf("job %s: ended %s after %d attempts", row.ID, row.State, row.Attempts))
	}
	for _, e := range r.Extras {
		if e.NeverReceived < 0 {
			p = append(p, fmt.Sprintf("job %s: %d unstarted nacks logged but only %d leases before the one that completed it; the log and the row disagree",
				e.ID, e.UnstartedNacks, e.Attempts-1))
		}
	}
	for pod, n := range r.Unparsed {
		p = append(p, fmt.Sprintf("pod %s: %d non-JSON lines; cmd/worker prints those only when it exits with an error", pod, n))
	}
	midJob := 0
	for _, s := range r.Shutdowns {
		if s.DeadlineReached {
			p = append(p, fmt.Sprintf("pod %s: shutdown deadline reached; work was abandoned", s.Pod))
		}
		if !s.Stopped {
			p = append(p, fmt.Sprintf("pod %s: shutdown started but the worker never logged a clean stop", s.Pod))
		}
		if s.RunningHandlers > 0 {
			midJob++
		}
	}
	if midJob == 0 {
		p = append(p, "no worker was shut down while a handler ran; the run says nothing about graceful shutdown")
	}
	slices.Sort(p)
	return p
}
