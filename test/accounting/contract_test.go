package accounting

// The log contract (docs/design/conformance.md, "How a run is judged"), tested
// on synthetic logs. This file is package accounting, not accounting_test:
// TestProcessesAgreeWithReconcile reads the unexported process split.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

const (
	jobA = "0193a000-0000-7000-8000-00000000000a"
	jobB = "0193a000-0000-7000-8000-00000000000b"
	l1   = "0193a000-0000-7000-8000-000000000101"
	l2   = "0193a000-0000-7000-8000-000000000102"
	l3   = "0193a000-0000-7000-8000-000000000103"
)

// sp is one log line as lines reads it: "msg|job|lease|op|offset_ms|flag".
// On "worker starting" the op column is the concurrency ("" omits it). The
// flag "noqueue" leaves the queue off a line about a job.
func sp(msg, job, lease, op string, ms int, flag ...string) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d|%s", msg, job, lease, op, ms, strings.Join(flag, ","))
}

func starting(concurrency string, ms int) string {
	return sp(msgWorkerStarting, "", "", concurrency, ms)
}
func start(job, lease string, ms int) string     { return sp(msgJobStarted, job, lease, "", ms) }
func finish(job, lease string, ms int) string    { return sp(msgHandlerFinished, job, lease, "", ms) }
func ack(job, lease string, ms int) string       { return sp(msgJobConfirmed, job, lease, "ack", ms) }
func nack(job, lease string, ms int) string      { return sp(msgJobConfirmed, job, lease, "nack", ms) }
func unstarted(job, lease string, ms int) string { return sp(msgUnstartedNack, job, lease, "", ms) }
func stopped(ms int) string                      { return sp(msgWorkerStopped, "", "", "", ms) }

// jsonl renders specs as one container's stdout. A spec that starts with "{"
// is already a JSON line and is written as it is.
func jsonl(t *testing.T, specs ...string) []byte {
	t.Helper()
	var b strings.Builder
	for _, s := range specs {
		if strings.HasPrefix(s, "{") {
			b.WriteString(s + "\n")
			continue
		}
		f := strings.Split(s, "|")
		require.Len(t, f, 6, "spec %q", s)
		ms, err := strconv.Atoi(f[4])
		require.NoError(t, err)
		rec := map[string]any{"time": t0.Add(time.Duration(ms) * time.Millisecond).Format(time.RFC3339Nano),
			"level": "INFO", "msg": f[0]}
		switch {
		case f[0] == msgWorkerStarting && f[3] != "":
			n, err := strconv.Atoi(f[3])
			require.NoError(t, err)
			rec["concurrency"] = n
		case f[3] != "":
			rec["operation"] = f[3]
		}
		if f[1] != "" {
			rec["job_id"] = f[1]
			if f[2] != "" {
				rec["lease_id"] = f[2]
			}
			if f[5] != "noqueue" {
				rec["queue"] = "q"
			}
		}
		raw, err := json.Marshal(rec)
		require.NoError(t, err)
		b.Write(raw)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// lines parses specs as one pod's JSON log, through Parse.
func lines(t *testing.T, pod string, specs ...string) Log {
	t.Helper()
	l, err := Parse(pod, bytes.NewReader(jsonl(t, specs...)))
	require.NoError(t, err)
	require.Zero(t, l.Unparsed)
	return l
}

func row(job, state string, attempts int, lease string) Row {
	r := Row{ID: uuid.MustParse(job), State: state, Attempts: attempts}
	if lease != "" {
		r.LeaseID = uuid.MustParse(lease)
	}
	return r
}

func requireHas(t *testing.T, problems []string, substrs ...string) {
	t.Helper()
	require.True(t, slices.ContainsFunc(problems, func(p string) bool {
		for _, s := range substrs {
			if !strings.Contains(p, s) {
				return false
			}
		}
		return true
	}), "no one problem contains all of %q in %q", substrs, problems)
}

// cleanDelivery is a clean started delivery: start, finish, ack.
func cleanDelivery(job, lease string, ms int) []string {
	return []string{start(job, lease, ms), finish(job, lease, ms+100), ack(job, lease, ms+110)}
}

func TestAContractCleanRunHasNoProblems(t *testing.T) {
	l := lines(t, "a", append([]string{starting("4", 0)}, cleanDelivery(jobA, l1, 10)...)...)
	rows := []Row{row(jobA, "completed", 1, l1)}
	require.Empty(t, ContractProblems(rows, []Log{l}))
	require.Empty(t, Reconcile(rows, []Log{l}).AttemptProblems())
}

// 3b's negative control (docs/bench/java-smoke/20261123/negative-controls.txt):
// a log without a completed job's ack line passed Reconcile. The contract
// closes that gap.
func TestACompletedJobWithoutItsAckLineIsAProblem(t *testing.T) {
	l := lines(t, "a", starting("4", 0), start(jobA, l1, 10), finish(jobA, l1, 110))
	rows := []Row{row(jobA, "completed", 1, l1)}

	p := ContractProblems(rows, []Log{l})
	require.Len(t, p, 1, "%q", p)
	requireHas(t, p, "completed under lease "+l1, "never logged its ack")
	require.Empty(t, Reconcile(rows, []Log{l}).AttemptProblems(), "accounting alone does not see the missing line")
}

func TestACutProcessMayStopADeliveryPartWay(t *testing.T) {
	first := lines(t, "a",
		starting("4", 0), start(jobA, l1, 10), // killed here: no finish, no stop
		starting("4", 40000), start(jobA, l2, 40010), finish(jobA, l2, 40100), ack(jobA, l2, 40110))
	rows := []Row{row(jobA, "completed", 2, l2)}
	require.Empty(t, ContractProblems(rows, []Log{first}))
	require.Empty(t, Reconcile(rows, []Log{first}).AttemptProblems())
}

func TestALastProcessMayNotLeaveADeliveryOpen(t *testing.T) {
	t.Run("the delivery completed the job", func(t *testing.T) {
		l := lines(t, "a", starting("4", 0), start(jobA, l1, 10))
		p := ContractProblems([]Row{row(jobA, "completed", 1, l1)}, []Log{l})
		requireHas(t, p, "no terminal line")
	})
	t.Run("another pod completed it", func(t *testing.T) {
		a := lines(t, "a", starting("4", 0), start(jobA, l1, 10))
		b := lines(t, "b", append([]string{starting("4", 0)}, cleanDelivery(jobA, l2, 40000)...)...)
		p := ContractProblems([]Row{row(jobA, "completed", 2, l2)}, []Log{a, b})
		requireHas(t, p, "pod a", "no terminal line")
		require.Len(t, p, 1, "%q", p)
	})
}

func TestAStartedDeliveryFinishesOnceAndEndsOnce(t *testing.T) {
	rows := []Row{row(jobA, "completed", 1, l1)}
	tests := map[string]struct {
		specs []string
		want  string
	}{
		"handler finished twice": {
			[]string{start(jobA, l1, 10), finish(jobA, l1, 20), finish(jobA, l1, 30), ack(jobA, l1, 40)},
			"handler finished 2 times"},
		"two terminal lines": {
			[]string{start(jobA, l1, 10), finish(jobA, l1, 20), ack(jobA, l1, 30), ack(jobA, l1, 40)},
			"2 terminal lines"},
		"a terminal line before handler finished": {
			[]string{start(jobA, l1, 10), ack(jobA, l1, 20), finish(jobA, l1, 30)},
			"terminal line before handler finished"},
		"started twice": {
			[]string{start(jobA, l1, 10), start(jobA, l1, 15), finish(jobA, l1, 20), ack(jobA, l1, 30)},
			"job started 2 times"},
		"a terminal line without handler finished": {
			[]string{start(jobA, l1, 10), ack(jobA, l1, 30)},
			"terminal line before handler finished"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			l := lines(t, "a", append([]string{starting("4", 0)}, tc.specs...)...)
			requireHas(t, ContractProblems(rows, []Log{l}), tc.want)
		})
	}
}

func TestEveryTerminalMessageEndsAStartedDelivery(t *testing.T) {
	for msg := range terminalMessages {
		t.Run(msg, func(t *testing.T) {
			op := ""
			if msg == msgJobConfirmed {
				op = "ack"
			}
			l := lines(t, "a", starting("4", 0), start(jobA, l1, 10), finish(jobA, l1, 20), sp(msg, jobA, l1, op, 30))
			// A job that is not completed under this lease has no row rule here.
			require.Empty(t, ContractProblems([]Row{row(jobA, "available", 1, l1)}, []Log{l}))
		})
	}
	require.Len(t, terminalMessages, 5)
}

func TestAnUnstartedNackEndsOnANack(t *testing.T) {
	other := lines(t, "b", append([]string{starting("4", 0)}, cleanDelivery(jobA, l2, 40000)...)...)
	rows := []Row{row(jobA, "completed", 2, l2)}
	run := func(specs ...string) []string {
		a := lines(t, "a", append([]string{starting("4", 0)}, specs...)...)
		return ContractProblems(rows, []Log{a, other})
	}

	require.Empty(t, run(unstarted(jobA, l1, 10), nack(jobA, l1, 20)))
	requireHas(t, run(unstarted(jobA, l1, 10), ack(jobA, l1, 20)), "nack")
	requireHas(t, run(start(jobA, l1, 5), unstarted(jobA, l1, 10), nack(jobA, l1, 20)), "both started and nacked unstarted")
	requireHas(t, run(unstarted(jobA, l1, 10)), "no terminal line")
}

func TestALeaseNotConfirmedStandsAlone(t *testing.T) {
	other := lines(t, "b", append([]string{starting("4", 0)}, cleanDelivery(jobA, l2, 40000)...)...)
	rows := []Row{row(jobA, "completed", 2, l2)}
	notConfirmed := sp(msgLeaseNotConfirmed, jobA, l1, "", 10)

	alone := lines(t, "a", starting("4", 0), notConfirmed)
	require.Empty(t, ContractProblems(rows, []Log{alone, other}))

	followed := lines(t, "a", starting("4", 0), notConfirmed, start(jobA, l1, 20))
	requireHas(t, ContractProblems(rows, []Log{followed, other}), "must stand alone")
}

func TestADeadJobDoesNotEndOnAnAck(t *testing.T) {
	l := lines(t, "a", append([]string{starting("4", 0)}, cleanDelivery(jobA, l1, 10)...)...)
	requireHas(t, ContractProblems([]Row{row(jobA, "dead", 1, l1)}, []Log{l}), "dead", "ended on an ack")

	nacked := lines(t, "a", starting("4", 0), start(jobA, l1, 10), finish(jobA, l1, 20), nack(jobA, l1, 30))
	require.Empty(t, ContractProblems([]Row{row(jobA, "dead", 1, l1)}, []Log{nacked}))
}

func TestACancelledJobIsNeverConfirmed(t *testing.T) {
	l := lines(t, "a", append([]string{starting("4", 0)}, cleanDelivery(jobA, l1, 10)...)...)
	requireHas(t, ContractProblems([]Row{row(jobA, "cancelled", 1, l1)}, []Log{l}), "cancelled", "was confirmed")

	unconfirmed := lines(t, "a", starting("4", 0), start(jobA, l1, 10), finish(jobA, l1, 20),
		sp("job left unconfirmed; its lease ran out and it will be redelivered", jobA, l1, "ack", 30))
	require.Empty(t, ContractProblems([]Row{row(jobA, "cancelled", 1, l1)}, []Log{unconfirmed}))
}

func TestDeliveryLinesCarryLeaseAndQueue(t *testing.T) {
	rows := []Row{row(jobA, "completed", 1, l1)}

	noQueue := lines(t, "a", starting("4", 0), sp(msgJobStarted, jobA, l1, "", 10, "noqueue"),
		finish(jobA, l1, 20), ack(jobA, l1, 30))
	requireHas(t, ContractProblems(rows, []Log{noQueue}), "carry no queue")

	noLease := lines(t, "a", starting("4", 0), start(jobA, "", 10), finish(jobA, l1, 20), ack(jobA, l1, 30))
	requireHas(t, ContractProblems(rows, []Log{noLease}), "carry no lease_id")
}

func TestAProcessThatRanDeliveriesLoggedItsConcurrency(t *testing.T) {
	rows := []Row{row(jobA, "completed", 1, l1)}

	none := lines(t, "a", append([]string{starting("", 0)}, cleanDelivery(jobA, l1, 10)...)...)
	requireHas(t, ContractProblems(rows, []Log{none}), "concurrency")

	zero := lines(t, "a", append([]string{starting("0", 0)}, cleanDelivery(jobA, l1, 10)...)...)
	requireHas(t, ContractProblems(rows, []Log{zero}), "concurrency")

	before := lines(t, "a", cleanDelivery(jobA, l1, 10)...)
	requireHas(t, ContractProblems(rows, []Log{before}), "before any worker starting")
}

func TestJobsOutsideTheRunAreIgnored(t *testing.T) {
	// Cron ticks and smoke checks share the worker's log; their lines may lack
	// a lease, and a worker that ran nothing for the run needs no concurrency.
	l := lines(t, "a", sp("scheduler tick fired", jobB, "", "", 5), start(jobB, "", 6))
	require.Empty(t, ContractProblems([]Row{row(jobA, "completed", 1, l1)}, []Log{l}))
}

func TestProcessesAgreeWithReconcile(t *testing.T) {
	logs := map[string]Log{
		"one process": lines(t, "a", append([]string{starting("4", 0)}, cleanDelivery(jobA, l1, 10)...)...),
		"a cut and a restart": lines(t, "a",
			starting("4", 0), start(jobA, l1, 10),
			starting("4", 40000), start(jobA, l2, 40010), finish(jobA, l2, 40100)),
		"a stop and a restart": lines(t, "a",
			starting("4", 0), start(jobA, l1, 10), finish(jobA, l1, 20), stopped(30),
			starting("4", 5000), start(jobB, l3, 5010)),
	}
	rows := []Row{row(jobA, "completed", 2, l2), row(jobB, "completed", 1, l3)}
	for name, l := range logs {
		t.Run(name, func(t *testing.T) {
			want := Reconcile(rows, []Log{l}).Processes
			require.NotEmpty(t, want)
			byN := map[int]proc{}
			for _, p := range processesOf(l) {
				byN[p.n] = p
			}
			for _, w := range want {
				got, ok := byN[w.N]
				require.True(t, ok, "process %d", w.N)
				require.Equal(t, w.Stopped, got.stopped, "process %d stopped", w.N)
				require.Equal(t, w.Cut, got.cut, "process %d cut", w.N)
			}
		})
	}
}

func TestOpenDeliveriesCountsTheLastProcess(t *testing.T) {
	l := lines(t, "a",
		starting("4", 0), start(jobA, l1, 10), start(jobB, l1, 11), start(jobB, l3, 12), // cut with three open
		starting("4", 5000),
		start(jobA, l2, 5010), start(jobB, l2, 5011), unstarted(jobA, l3, 5012),
		finish(jobA, l2, 5020), ack(jobA, l2, 5030))
	require.Equal(t, 2, OpenDeliveries(l), "jobB under l2 and the unstarted nack under l3")
	require.Zero(t, OpenDeliveries(Log{Pod: "empty"}))
}
