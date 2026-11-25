package accounting

import (
	"fmt"
	"slices"
	"time"
)

// Peak is the most deliveries one worker process had open at once, judged
// from its log, against the concurrency it logged at its start.
type Peak struct {
	Pod         string
	Process     int // Process.N
	Concurrency int // from "worker starting"; 0 when it logged none
	Max         int
}

// ConcurrencyProblems checks the concurrency bound (docs/semantics.md §4)
// from the logs. A delivery is open from its first line among "job started"
// and "nacking an unstarted job" until its terminal line, in the order its
// process wrote them; a killed process leaves its deliveries open until its
// log ends. That interval lies inside the time the delivery's capacity token
// is held: the token left the pool when its credit was sent, and returns only
// after the terminal line is written. So more open deliveries than the
// process's concurrency proves a violation. No more proves the bound only for
// the part of each token's life the log shows: not for credit outstanding,
// and not for an ownership check in progress (docs/design/conformance.md).
// Lines are counted in file order, which is the order each SDK wrote them,
// not by their times.
func ConcurrencyProblems(logs []Log) ([]Peak, []string) {
	var peaks []Peak
	var p []string
	for _, l := range logs {
		for _, pr := range processesOf(l) {
			pk := Peak{Pod: l.Pod, Process: pr.n}
			if pr.starting != nil && pr.starting.Concurrency != nil {
				pk.Concurrency = *pr.starting.Concurrency
			}
			open := map[leaseKeyText]bool{}
			var firstOver time.Time
			for _, line := range pr.lines {
				if line.JobID == "" {
					continue
				}
				key := leaseKeyText{line.JobID, line.LeaseID}
				switch {
				case line.Msg == msgJobStarted || line.Msg == msgUnstartedNack:
					if open[key] {
						continue
					}
					open[key] = true
					pk.Max = max(pk.Max, len(open))
					if pk.Concurrency > 0 && len(open) > pk.Concurrency && firstOver.IsZero() {
						firstOver = line.Time
					}
				case terminalMessages[line.Msg]:
					delete(open, key)
				}
			}
			switch {
			case pk.Max > 0 && pk.Concurrency == 0:
				p = append(p, fmt.Sprintf("pod %s process %d: ran deliveries but logged no concurrency at its start", l.Pod, pr.n))
			case pk.Concurrency > 0 && pk.Max > pk.Concurrency:
				p = append(p, fmt.Sprintf("pod %s process %d: %d deliveries open at once (first over the bound at %s), concurrency %d",
					l.Pod, pr.n, pk.Max, firstOver.Format(time.RFC3339Nano), pk.Concurrency))
			}
			peaks = append(peaks, pk)
		}
	}
	slices.Sort(p)
	return peaks, p
}

// leaseKeyText names one delivery by its logged ids, unparsed.
type leaseKeyText struct{ job, lease string }
