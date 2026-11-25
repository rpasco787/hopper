package accounting

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const grant = 30 * time.Second

// raw is one JSON log line with extra members, written as the SDKs write them.
func raw(msg string, ms int, members string) string {
	if members != "" {
		members = "," + members
	}
	return fmt.Sprintf(`{"time":%q,"level":"INFO","msg":%q%s}`,
		t0.Add(time.Duration(ms)*time.Millisecond).Format(time.RFC3339Nano), msg, members)
}

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// run is a worker log of one pod's single process.
func run(t *testing.T, specs ...string) []Log {
	t.Helper()
	return []Log{lines(t, "a", append([]string{starting("4", 0)}, specs...)...)}
}

// ---- cancellation ----

func TestCancelStoppedAtTheFirstRenewal(t *testing.T) {
	logs := run(t,
		start(jobA, l1, 100),
		sp(msgHeartbeatRejected, jobA, l1, "", 10150), // 10.05 s after the start
		finish(jobA, l1, 10160),
		sp(msgLeaseRanOut, jobA, l1, "ack", 10170))
	got, p := CancelOutcome(logs, jobA, grant)
	require.Empty(t, p)
	require.Equal(t, CancelStoppedAtRenewal, got)
}

func TestARenewalRefusedTooEarlyOrTooLateIsAProblem(t *testing.T) {
	for name, rejectedAt := range map[string]int{"5 s": 5100, "8.5 s": 8600, "12.5 s": 12600, "13 s": 13100} {
		t.Run(name, func(t *testing.T) {
			logs := run(t,
				start(jobA, l1, 100),
				sp(msgHeartbeatRejected, jobA, l1, "", rejectedAt),
				finish(jobA, l1, rejectedAt+10),
				sp(msgLeaseRanOut, jobA, l1, "ack", rejectedAt+20))
			got, p := CancelOutcome(logs, jobA, grant)
			require.Equal(t, CancelStoppedAtRenewal, got)
			requireHas(t, p, "its renewal was refused")
		})
	}
	// The edges are inclusive: G/3 - 1 s and G/3 + 2 s after the start.
	for _, rejectedAt := range []int{100 + 9000, 100 + 12000} {
		logs := run(t, start(jobA, l1, 100), sp(msgHeartbeatRejected, jobA, l1, "", rejectedAt),
			finish(jobA, l1, rejectedAt+10), sp(msgLeaseRanOut, jobA, l1, "ack", rejectedAt+20))
		_, p := CancelOutcome(logs, jobA, grant)
		require.Empty(t, p, "refused at %d ms", rejectedAt)
	}
}

func TestARefusedRenewalThatDidNotStopTheHandlerIsAProblem(t *testing.T) {
	logs := run(t, start(jobA, l1, 100), sp(msgHeartbeatRejected, jobA, l1, "", 10150))
	_, p := CancelOutcome(logs, jobA, grant)
	requireHas(t, p, "was not stopped")
}

func TestCancelAckRefused(t *testing.T) {
	logs := run(t, start(jobA, l1, 100), finish(jobA, l1, 3100), sp(msgConfirmRefused, jobA, l1, "ack", 3110))
	got, p := CancelOutcome(logs, jobA, grant)
	require.Empty(t, p)
	require.Equal(t, CancelAckRefused, got)
}

func TestCancelNeverStarted(t *testing.T) {
	logs := run(t, sp(msgLeaseNotConfirmed, jobA, l1, "", 16))
	got, p := CancelOutcome(logs, jobA, grant)
	require.Empty(t, p)
	require.Equal(t, CancelNeverStarted, got)
}

func TestACancelledJobThatWasConfirmedIsAProblem(t *testing.T) {
	logs := run(t, start(jobA, l1, 100), finish(jobA, l1, 3100), ack(jobA, l1, 3110))
	_, p := CancelOutcome(logs, jobA, grant)
	requireHas(t, p, "was confirmed (ack)")
}

func TestACancelledJobLeasedTwiceIsAProblem(t *testing.T) {
	logs := run(t, sp(msgLeaseNotConfirmed, jobA, l1, "", 16), start(jobA, l2, 40000),
		finish(jobA, l2, 40100), sp(msgConfirmRefused, jobA, l2, "ack", 40110))
	_, p := CancelOutcome(logs, jobA, grant)
	requireHas(t, p, "leased 2 times")
}

func TestACancelWithNoOutcomeIsAProblem(t *testing.T) {
	t.Run("started, and nothing learned of it", func(t *testing.T) {
		logs := run(t, start(jobA, l1, 100), finish(jobA, l1, 3100))
		got, p := CancelOutcome(logs, jobA, grant)
		require.Empty(t, got)
		requireHas(t, p, "neither a renewal nor its Ack")
	})
	t.Run("never seen", func(t *testing.T) {
		got, p := CancelOutcome(run(t), jobA, grant)
		require.Empty(t, got)
		requireHas(t, p, "no worker logged its start")
	})
}

// ---- local expiry ----

func TestExpiredLocallyAtTheDeadline(t *testing.T) {
	from := at(5000)
	logs := run(t, start(jobA, l1, 10),
		sp(msgExpiredLocally, jobA, l1, "", 30000), // 25 s after the outage began
		finish(jobA, l1, 30002),
		sp(msgLeaseRanOut, jobA, l1, "ack", 30003))
	require.Empty(t, ExpiredLocally(logs, jobA, from, grant))
}

func TestExpiredTooSoonOrTooLateIsAProblem(t *testing.T) {
	from := at(5000)
	for name, expiredAt := range map[string]int{"15 s": 20000, "32 s": 37000} {
		t.Run(name, func(t *testing.T) {
			logs := run(t, start(jobA, l1, 10), sp(msgExpiredLocally, jobA, l1, "", expiredAt),
				finish(jobA, l1, expiredAt+2), sp(msgLeaseRanOut, jobA, l1, "ack", expiredAt+3))
			requireHas(t, ExpiredLocally(logs, jobA, from, grant), "expired locally", "after the outage began")
		})
	}
	// (G - margin - G/3 - 1 s, G - margin + 1 s] is (18.5 s, 30.5 s]: open below.
	for name, c := range map[string]struct {
		expiredAt int
		ok        bool
	}{
		"exactly 18.5 s": {5000 + 18500, false},
		"18.6 s":         {5000 + 18600, true},
		"exactly 30.5 s": {5000 + 30500, true},
		"30.6 s":         {5000 + 30600, false},
	} {
		logs := run(t, start(jobA, l1, 10), sp(msgExpiredLocally, jobA, l1, "", c.expiredAt),
			finish(jobA, l1, c.expiredAt+2), sp(msgLeaseRanOut, jobA, l1, "ack", c.expiredAt+3))
		require.Equal(t, c.ok, len(ExpiredLocally(logs, jobA, from, grant)) == 0, name)
	}
}

func TestAHandlerThatOutlivedItsExpiryIsAProblem(t *testing.T) {
	logs := run(t, start(jobA, l1, 10), sp(msgExpiredLocally, jobA, l1, "", 30000),
		finish(jobA, l1, 33000), sp(msgLeaseRanOut, jobA, l1, "ack", 33001))
	requireHas(t, ExpiredLocally(logs, jobA, at(5000), grant), "handler finished", "after the lease expired")
}

func TestAnExpiredDeliveryThatWasConfirmedOrNotLeftUnconfirmedIsAProblem(t *testing.T) {
	confirmed := run(t, start(jobA, l1, 10), sp(msgExpiredLocally, jobA, l1, "", 30000),
		finish(jobA, l1, 30002), ack(jobA, l1, 30003))
	requireHas(t, ExpiredLocally(confirmed, jobA, at(5000), grant), "was confirmed")

	silent := run(t, start(jobA, l1, 10), sp(msgExpiredLocally, jobA, l1, "", 30000), finish(jobA, l1, 30002))
	requireHas(t, ExpiredLocally(silent, jobA, at(5000), grant), "left unconfirmed")
}

func TestADeliveryThatNeverExpiredIsAProblem(t *testing.T) {
	logs := run(t, start(jobA, l1, 10), finish(jobA, l1, 3000), ack(jobA, l1, 3010))
	requireHas(t, ExpiredLocally(logs, jobA, at(5000), grant), "never expired")

	none := run(t)
	requireHas(t, ExpiredLocally(none, jobA, at(5000), grant), "no handler started before the outage")
}

// ---- the deadline ----

func TestDeadlineLineReadsTheFourCounts(t *testing.T) {
	l := lines(t, "w", raw(msgDeadlineReached, 5,
		`"active_deliveries":2,"running_handlers":2,"acquiring":0,"confirming":0`))
	got, p := DeadlineLine(l)
	require.Empty(t, p)
	require.Equal(t, DeadlineCounts{Deliveries: 2, Handlers: 2}, got)

	l = lines(t, "w", raw(msgDeadlineReached, 5,
		`"active_deliveries":3,"running_handlers":1,"acquiring":1,"confirming":1`))
	got, _ = DeadlineLine(l)
	require.Equal(t, DeadlineCounts{3, 1, 1, 1}, got)
}

func TestADeadlineLineWithoutItsCountsIsAProblem(t *testing.T) {
	// The line the Go worker logged before Phase 3c: its counts struct had only
	// unexported fields, which slog renders {}.
	l := lines(t, "w", raw(msgDeadlineReached, 5, `"counts":{}`))
	_, p := DeadlineLine(l)
	requireHas(t, p, "deadline", "active_deliveries")
	requireHas(t, p, "deadline", "confirming")
}

func TestZeroOrTwoDeadlineLinesAreAProblem(t *testing.T) {
	counts := `"active_deliveries":1,"running_handlers":1,"acquiring":0,"confirming":0`
	_, p := DeadlineLine(lines(t, "w", raw(msgDeadlineReached, 5, counts), raw(msgDeadlineReached, 6, counts)))
	requireHas(t, p, "2 shutdown deadline reached lines")
	_, p = DeadlineLine(lines(t, "w", raw(msgShutdownStarted, 5, "")))
	requireHas(t, p, "no shutdown deadline reached line")
}

// ---- Judge ----

// cleanRun is one worker, two jobs, each completed and acked under its own lease.
func cleanRun(t *testing.T) (Expect, Evidence) {
	t.Helper()
	return Expect{Scenario: "clean", Mode: ModeAttempts}, Evidence{
		Rows: []Row{row(jobA, "completed", 1, l1), row(jobB, "completed", 1, l2)},
		Stdout: map[string][]byte{"w1": jsonl(t, starting("4", 0),
			start(jobA, l1, 10), finish(jobA, l1, 110), ack(jobA, l1, 120),
			start(jobB, l2, 20), finish(jobB, l2, 120), ack(jobB, l2, 130))},
		Stderr: map[string][]byte{"w1": {}},
	}
}

func TestJudgeAcceptsACleanRun(t *testing.T) {
	e, ev := cleanRun(t)
	require.Empty(t, Judge(e, ev))
}

func TestJudgeUsesTheModesReconciliation(t *testing.T) {
	// ModeExecution counts a second start as a duplicate; ModeAttempts allows it
	// when a nack explains it. ModeGraceful also wants a worker shut down mid-handler.
	retried := Evidence{
		Rows: []Row{row(jobA, "completed", 2, l2)},
		Stdout: map[string][]byte{"w1": jsonl(t, starting("4", 0),
			start(jobA, l1, 10), finish(jobA, l1, 20), nack(jobA, l1, 30),
			start(jobA, l2, 40), finish(jobA, l2, 50), ack(jobA, l2, 60))},
	}
	require.Empty(t, Judge(Expect{Mode: ModeAttempts, RepeatCauses: []string{"nacked"}}, retried))
	requireHas(t, Judge(Expect{Mode: ModeExecution, RepeatCauses: []string{"nacked"}}, retried), "handler started 2 times")
	requireHas(t, Judge(Expect{Mode: "other"}, retried), "unknown mode")

	e, ev := cleanRun(t)
	e.Mode = ModeGraceful
	requireHas(t, Judge(e, ev), "no worker was shut down while a handler ran")
}

func TestJudgeChecksStderrAndExits(t *testing.T) {
	e, ev := cleanRun(t)
	ev.Stderr["w1"] = []byte("boom\n")
	requireHas(t, Judge(e, ev), "w1", "stderr")

	e.Stderr = map[string]string{"w1": "boom"}
	require.Empty(t, Judge(e, ev), "a listed container's stderr must match, trimmed")
	e.Stderr["w1"] = "something else"
	requireHas(t, Judge(e, ev), "w1", "stderr")

	e, ev = cleanRun(t)
	e.Exits = map[string]int{"w1": 1}
	ev.Exits = map[string]int{"w1": 0}
	requireHas(t, Judge(e, ev), "w1", "exit code 0, want 1")
	ev.Exits = nil
	requireHas(t, Judge(e, ev), "w1", "no exit code")
	ev.Exits = map[string]int{"w1": 1}
	require.Empty(t, Judge(e, ev))

	// A container whose output was captured but that the scenario never named.
	e, ev = cleanRun(t)
	ev.Stderr["w2"] = []byte("worker: x")
	requireHas(t, Judge(e, ev), "w2", "stderr")
}

func TestJudgeRejectsNonJSONOnStdout(t *testing.T) {
	e, ev := cleanRun(t)
	ev.Stdout["w1"] = append(ev.Stdout["w1"], "plain text\n"...)
	requireHas(t, Judge(e, ev), "non-JSON")
}

func TestJudgeAllowsExactlyTheExpectedProblems(t *testing.T) {
	// Two starts of one lease are a problem AttemptProblems reports.
	e, ev := cleanRun(t)
	ev.Stdout["w1"] = append(ev.Stdout["w1"], jsonl(t, start(jobA, l1, 200))...)
	got := Judge(e, ev)
	requireHas(t, got, "lease "+l1+" started 2 handlers")

	e.Problems = []string{fmt.Sprintf("job %s: lease %s started 2 handlers", jobA, l1)}
	for _, p := range Judge(e, ev) {
		require.NotContains(t, p, "started 2 handlers", "an expected problem is not reported")
	}

	// An expected problem that did not happen is reported.
	e, ev = cleanRun(t)
	e.Problems = []string{"job x: a problem that never happens"}
	requireHas(t, Judge(e, ev), "expected problem was not reported", "never happens")
}

func TestJudgeExcusesExactlyTheExpectedEndings(t *testing.T) {
	deadRun := func() Evidence {
		return Evidence{
			Rows: []Row{row(jobA, "dead", 3, l3)},
			Stdout: map[string][]byte{"w1": jsonl(t, starting("4", 0),
				start(jobA, l1, 10), finish(jobA, l1, 20), nack(jobA, l1, 30),
				start(jobA, l2, 40), finish(jobA, l2, 50), nack(jobA, l2, 60),
				start(jobA, l3, 70), finish(jobA, l3, 80), nack(jobA, l3, 90))},
		}
	}
	e := Expect{Mode: ModeAttempts, RepeatCauses: []string{"nacked"},
		Rows: map[string]RowWant{jobA: {State: "dead", Attempts: 3}}}
	require.Empty(t, Judge(e, deadRun()), "the ending the scenario expects is not a problem")

	// Without the RowWant, the same ending is the problem accounting.go:459 reports.
	requireHas(t, Judge(Expect{Mode: ModeAttempts, RepeatCauses: []string{"nacked"}}, deadRun()),
		fmt.Sprintf("job %s: ended dead after 3 attempts", jobA))

	// One attempt off: the real ending is not the expected one, and says so twice.
	e.Rows = map[string]RowWant{jobA: {State: "dead", Attempts: 2}}
	got := Judge(e, deadRun())
	requireHas(t, got, "ended dead after 3 attempts")
	requireHas(t, got, "attempts 3, want 2")
}

func TestJudgeChecksRepeatCauses(t *testing.T) {
	retried := Evidence{
		Rows: []Row{row(jobA, "completed", 2, l2)},
		Stdout: map[string][]byte{"w1": jsonl(t, starting("4", 0),
			start(jobA, l1, 10), finish(jobA, l1, 20), nack(jobA, l1, 30),
			start(jobA, l2, 40), finish(jobA, l2, 50), ack(jobA, l2, 60))},
	}
	requireHas(t, Judge(Expect{Mode: ModeAttempts}, retried), "cause \"nacked\"")
	requireHas(t, Judge(Expect{Mode: ModeAttempts, RepeatCauses: []string{"killed"}}, retried), "cause \"nacked\"")
	require.Empty(t, Judge(Expect{Mode: ModeAttempts, RepeatCauses: []string{"killed", "nacked"}}, retried))
}

// killedRun: process 1 starts job A and is killed; process 2 completes it 40 s later.
func killedRun(t *testing.T) Evidence {
	t.Helper()
	return Evidence{
		Rows: []Row{row(jobA, "completed", 2, l2)},
		Stdout: map[string][]byte{"w1": jsonl(t,
			starting("4", 0), start(jobA, l1, 10),
			starting("4", 40000), start(jobA, l2, 40010), finish(jobA, l2, 40100), ack(jobA, l2, 40110))},
	}
}

func TestJudgeChecksKillsAndGaps(t *testing.T) {
	e := Expect{Mode: ModeAttempts, RepeatCauses: []string{"killed"}, Kills: 1, MinRepeatGap: 25 * time.Second}
	require.Empty(t, Judge(e, killedRun(t)))

	e.Kills = 2
	requireHas(t, Judge(e, killedRun(t)), "1 processes were cut, want 2")
	e.Kills = 1
	e.MinRepeatGap = 50 * time.Second
	requireHas(t, Judge(e, killedRun(t)), "repeat gap")

	// Kills: the cut process must have had a handler unfinished.
	finished := killedRun(t)
	finished.Stdout["w1"] = jsonl(t,
		starting("4", 0), start(jobA, l1, 10), finish(jobA, l1, 20), nack(jobA, l1, 30),
		starting("4", 40000), start(jobA, l2, 40010), finish(jobA, l2, 40100), ack(jobA, l2, 40110))
	e = Expect{Mode: ModeAttempts, RepeatCauses: []string{"nacked"}, Kills: 1}
	requireHas(t, Judge(e, finished), "unfinished")
}

func TestKillsNeedAtLeastAsManyRepeatsAsUnfinishedHandlers(t *testing.T) {
	// Process 1 was killed with job A's handler running, after the Ack committed
	// but before it logged it: A completed under that lease, and nothing repeats.
	ev := Evidence{
		Rows: []Row{row(jobA, "completed", 1, l1)},
		Stdout: map[string][]byte{"w1": jsonl(t,
			starting("4", 0), start(jobA, l1, 10),
			starting("4", 40000))},
	}
	requireHas(t, Judge(Expect{Mode: ModeAttempts, Kills: 1}, ev), "0 repeats for 1 unfinished handlers")
	require.Empty(t, Judge(Expect{Mode: ModeAttempts}, ev), "without Kills the same run is clean")
}

func TestJudgeChecksNoExtrasAndFirstLease(t *testing.T) {
	e := Expect{Mode: ModeAttempts, RepeatCauses: []string{"killed"}, NoExtras: true, FirstLease: []string{jobA}}
	got := Judge(e, killedRun(t))
	requireHas(t, got, "leased more than once")
	requireHas(t, got, "first lease")

	e, ev := cleanRun(t)
	e.NoExtras, e.FirstLease = true, []string{jobA, jobB}
	require.Empty(t, Judge(e, ev))
}

func TestJudgeChecksSaturation(t *testing.T) {
	four := Evidence{
		Rows: []Row{row(jobA, "completed", 1, l1), row(jobB, "completed", 1, l2)},
		Stdout: map[string][]byte{"w1": jsonl(t, starting("4", 0),
			start(jobA, l1, 10), start(jobB, l2, 11), start(job(2), lease(2), 12), start(job(3), lease(3), 13),
			finish(jobA, l1, 20), ack(jobA, l1, 21), finish(jobB, l2, 22), ack(jobB, l2, 23),
			finish(job(2), lease(2), 24), ack(job(2), lease(2), 25), finish(job(3), lease(3), 26), ack(job(3), lease(3), 27))},
	}
	four.Rows = append(four.Rows, row(job(2), "completed", 1, lease(2)), row(job(3), "completed", 1, lease(3)))
	e := Expect{Mode: ModeAttempts, Saturated: true}
	require.Empty(t, Judge(e, four))

	e, ev := cleanRun(t) // two deliveries at most at once: peak 2 of 4
	e.Saturated = true
	requireHas(t, Judge(e, ev), "concurrency")
}

func TestJudgeChecksDrainedHandlers(t *testing.T) {
	drain := `"running_handlers":1,"active_deliveries":1`
	drained := func(terminal string) Evidence {
		return Evidence{
			Rows: []Row{row(jobA, "completed", 1, l1)},
			Stdout: map[string][]byte{"w1": jsonl(t, starting("4", 0), start(jobA, l1, 10),
				raw(msgShutdownStarted, 20, drain), finish(jobA, l1, 30), terminal, stopped(50))},
		}
	}
	e := Expect{Mode: ModeGraceful, DrainedHandlers: true}
	require.Empty(t, Judge(e, drained(ack(jobA, l1, 40))))

	// The delivery open at the signal was abandoned instead of confirmed.
	bad := drained(sp("job left unconfirmed; the worker was shutting down", jobA, l1, "ack", 40))
	got := Judge(Expect{Mode: ModeGraceful, DrainedHandlers: true}, bad)
	requireHas(t, got, "open when the drain began")

	// A process that hit its deadline is judged by the deadline checks instead.
	abandoned := drained(sp("job left unconfirmed; the worker was shutting down", jobA, l1, "ack", 40))
	abandoned.Stdout["w1"] = append(abandoned.Stdout["w1"], jsonl(t,
		raw(msgDeadlineReached, 45, `"active_deliveries":1,"running_handlers":1,"acquiring":0,"confirming":0`))...)
	for _, p := range Judge(Expect{Mode: ModeGraceful, DrainedHandlers: true}, abandoned) {
		require.NotContains(t, p, "open when the drain began")
	}
}

func TestJudgeChecksEveryWorkerRan(t *testing.T) {
	e, ev := cleanRun(t)
	e.EveryWorkerRan = true
	require.Empty(t, Judge(e, ev))
	ev.Stdout["w2"] = jsonl(t, starting("4", 0))
	requireHas(t, Judge(e, ev), "w2", "ran none of the run's jobs")
}

func TestJudgeChecksRows(t *testing.T) {
	e, ev := cleanRun(t)
	one, two := 1, 2
	wrong := "intentional failure"
	e.Rows = map[string]RowWant{jobA: {State: "completed", Attempts: 1, Starts: &one, LastError: new(string)}}
	require.Empty(t, Judge(e, ev))

	e.Rows = map[string]RowWant{jobA: {State: "dead", Attempts: 1}}
	requireHas(t, Judge(e, ev), "state completed, want dead")
	e.Rows = map[string]RowWant{jobA: {State: "completed", Attempts: 2}}
	requireHas(t, Judge(e, ev), "attempts 1, want 2")
	e.Rows = map[string]RowWant{jobA: {State: "completed", Attempts: 1, Starts: &two}}
	requireHas(t, Judge(e, ev), "2 starts", "1 job started")
	e.Rows = map[string]RowWant{jobA: {State: "completed", Attempts: 1, LastError: &wrong}}
	requireHas(t, Judge(e, ev), "last_error")
	e.Rows = map[string]RowWant{job(9): {State: "completed", Attempts: 1}}
	requireHas(t, Judge(e, ev), "no row")
}

func TestJudgeChecksCancels(t *testing.T) {
	logs := jsonl(t, starting("4", 0), start(jobA, l1, 100),
		sp(msgHeartbeatRejected, jobA, l1, "", 10150), finish(jobA, l1, 10160),
		sp(msgLeaseRanOut, jobA, l1, "ack", 10170))
	ev := Evidence{Rows: []Row{row(jobA, "cancelled", 1, l1)}, Stdout: map[string][]byte{"w1": logs}}
	e := Expect{Mode: ModeAttempts, Grant: grant,
		Rows:    map[string]RowWant{jobA: {State: "cancelled", Attempts: 1}},
		Cancels: map[string]string{jobA: CancelStoppedAtRenewal}}
	require.Empty(t, Judge(e, ev))

	e.Cancels = map[string]string{jobA: CancelAckRefused}
	requireHas(t, Judge(e, ev), "stopped at its first renewal, want ack refused")
	e.Cancels = map[string]string{jobA: CancelRenewalOrNeverStarted}
	require.Empty(t, Judge(e, ev), "a cancel at lease time may land either way")
	e.Cancels = map[string]string{jobA: CancelNeverStarted}
	requireHas(t, Judge(e, ev), "want never started")
	e.Grant = 0
	requireHas(t, Judge(e, ev), "Grant")
}

func TestJudgeChecksExpiredAndDeadline(t *testing.T) {
	logs := jsonl(t, starting("4", 0), start(jobA, l1, 10),
		sp(msgExpiredLocally, jobA, l1, "", 30000), finish(jobA, l1, 30002),
		sp(msgLeaseRanOut, jobA, l1, "ack", 30003),
		raw(msgShutdownStarted, 30500, `"running_handlers":1,"active_deliveries":1`),
		raw(msgDeadlineReached, 31000, `"active_deliveries":2,"running_handlers":2,"acquiring":0,"confirming":0`))
	ev := Evidence{Rows: []Row{row(jobA, "available", 1, l1)}, Stdout: map[string][]byte{"w1": logs}}
	e := Expect{Mode: ModeAttempts, Grant: grant, OutageStart: at(5000), Expired: []string{jobA},
		Deadline: map[string]DeadlineCounts{"w1": {Deliveries: 2, Handlers: 2}},
		Rows:     map[string]RowWant{jobA: {State: "available", Attempts: 1}},
		Problems: []string{"pod w1: shutdown deadline reached; work was abandoned",
			"pod w1: shutdown started but the worker never logged a clean stop"}}
	// accounting reports the abandonment itself; Problems lists it as expected.
	require.Empty(t, Judge(e, ev))

	e.Deadline = map[string]DeadlineCounts{"w1": {Deliveries: 3, Handlers: 2}}
	requireHas(t, Judge(e, ev), "w1", "deadline", "3")
	e.Expired = []string{jobB}
	requireHas(t, Judge(e, ev), "no handler started before the outage")
}

func TestJudgeRunsTheContractAndTheBoundAlways(t *testing.T) {
	// Only the contract is broken: a completed job's ack line is missing.
	e, ev := cleanRun(t)
	ev.Stdout["w1"] = jsonl(t, starting("4", 0),
		start(jobA, l1, 10), finish(jobA, l1, 110), start(jobB, l2, 20), finish(jobB, l2, 120), ack(jobB, l2, 130))
	requireHas(t, Judge(e, ev), "never logged its ack")

	// Only the bound is broken: five deliveries open at concurrency 4.
	var rows []Row
	var specs []string
	specs = append(specs, starting("4", 0))
	for i := 0; i < 5; i++ {
		rows = append(rows, row(job(i), "completed", 1, lease(i)))
		specs = append(specs, start(job(i), lease(i), 10+i))
	}
	for i := 0; i < 5; i++ {
		specs = append(specs, finish(job(i), lease(i), 100+i), ack(job(i), lease(i), 110+i))
	}
	requireHas(t, Judge(Expect{Mode: ModeAttempts}, Evidence{Rows: rows, Stdout: map[string][]byte{"w1": jsonl(t, specs...)}}),
		"5 deliveries open at once")
}

// ---- evidence files ----

func TestEvidenceRoundTrips(t *testing.T) {
	e, ev := cleanRun(t)
	one := 1
	e.Rows = map[string]RowWant{jobA: {State: "completed", Attempts: 1, Starts: &one}}
	e.Exits = map[string]int{"w1": 1}
	e.Stderr = map[string]string{"w1": "worker: x"}
	e.Deadline = map[string]DeadlineCounts{"w1": {Deliveries: 2, Handlers: 2}}
	e.Grant = grant
	e.OutageStart = at(5000)
	e.Cancels = map[string]string{jobA: CancelRenewalOrNeverStarted}
	ev.Rows[0].LastError = "boom, with \"quotes\" and a comma"
	ev.Stderr["w1"] = []byte("worker: x\n")
	ev.Exits = map[string]int{"w1": 0} // a difference Judge must report the same way after the round trip

	dir := t.TempDir()
	require.NoError(t, WriteEvidence(dir, e, ev))
	e2, ev2, err := ReadEvidence(dir)
	require.NoError(t, err)

	require.Equal(t, ev.Rows, ev2.Rows)
	require.Equal(t, ev.Stdout, ev2.Stdout)
	require.Equal(t, ev.Stderr, ev2.Stderr)
	require.Equal(t, ev.Exits, ev2.Exits)
	require.True(t, e.OutageStart.Equal(e2.OutageStart))
	e2.OutageStart = e.OutageStart
	require.Equal(t, e, e2)

	want := Judge(e, ev)
	require.NotEmpty(t, want, "the round trip is checked on an answer that has something to say")
	require.Equal(t, want, Judge(e2, ev2))
}
