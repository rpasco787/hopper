package accounting_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/test/accounting"
)

func logOf(t *testing.T, pod string, lines ...string) accounting.Log {
	t.Helper()
	l, err := accounting.Parse(pod, strings.NewReader(strings.Join(lines, "\n")))
	require.NoError(t, err)
	return l
}

func started(id uuid.UUID) string {
	return fmt.Sprintf(`{"level":"INFO","msg":"job started","job_id":%q,"attempt":1}`, id.String())
}

func nackedUnstarted(id uuid.UUID) string {
	return fmt.Sprintf(`{"level":"INFO","msg":"nacking an unstarted job","job_id":%q}`, id.String())
}

func shutdown(running int) string {
	return fmt.Sprintf(`{"level":"INFO","msg":"shutdown started","running_handlers":%d}`, running)
}

const (
	stopped  = `{"level":"INFO","msg":"worker stopped"}`
	deadline = `{"level":"WARN","msg":"shutdown deadline reached"}`
)

func completed(id uuid.UUID, attempts int) accounting.Row {
	return accounting.Row{ID: id, State: "completed", Attempts: attempts}
}

// cleanStop is a pod that drained mid-job and stopped, so a report that should
// be clean does not trip the idle-shutdown problem.
func cleanStop(t *testing.T) accounting.Log {
	return logOf(t, "drained", shutdown(1), stopped)
}

func requireProblem(t *testing.T, problems []string, substr string) {
	t.Helper()
	require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, substr) }),
		"no problem contains %q in %q", substr, problems)
}

func TestACleanRestartHasNoProblems(t *testing.T) {
	j1, j2, j3 := uuid.New(), uuid.New(), uuid.New()
	rep := accounting.Reconcile(
		[]accounting.Row{completed(j1, 1), completed(j2, 1), completed(j3, 1)},
		[]accounting.Log{
			logOf(t, "a", started(j1), started(j2), shutdown(2), stopped),
			logOf(t, "b", started(j3)),
		})
	require.Empty(t, rep.Problems())
	require.Equal(t, 3, rep.Completed)
	require.Equal(t, []accounting.Shutdown{{Pod: "a", RunningHandlers: 2, Stopped: true}}, rep.Shutdowns)
}

func TestASecondStartIsADuplicate(t *testing.T) {
	id := uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completed(id, 2)},
		[]accounting.Log{logOf(t, "a", started(id)), logOf(t, "b", started(id)), cleanStop(t)})
	require.Equal(t, 2, rep.Duplicated[id])
	requireProblem(t, rep.Problems(), "handler started 2 times")
}

func TestACompletedJobWithNoStartMeansTheCaptureMissedIt(t *testing.T) {
	id := uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completed(id, 1)}, []accounting.Log{cleanStop(t)})
	require.Equal(t, []uuid.UUID{id}, rep.NeverStarted)
	requireProblem(t, rep.Problems(), "the log capture is incomplete")
}

func TestAnUnfinishedJobIsAProblem(t *testing.T) {
	id := uuid.New()
	row := accounting.Row{ID: id, State: "dead", Attempts: 5}
	rep := accounting.Reconcile([]accounting.Row{row}, []accounting.Log{logOf(t, "a", started(id)), cleanStop(t)})
	require.Equal(t, []accounting.Row{row}, rep.Unfinished)
	requireProblem(t, rep.Problems(), "ended dead after 5 attempts")
}

func TestAnExtraAttemptWithOneStartIsNotADuplicate(t *testing.T) {
	nacked, unreceived := uuid.New(), uuid.New()
	rep := accounting.Reconcile(
		[]accounting.Row{completed(nacked, 2), completed(unreceived, 2)},
		[]accounting.Log{
			logOf(t, "a", nackedUnstarted(nacked), shutdown(1), stopped),
			logOf(t, "b", started(nacked), started(unreceived)),
		})
	require.Empty(t, rep.Problems())
	require.ElementsMatch(t, []accounting.Extra{
		{ID: nacked, Attempts: 2, UnstartedNacks: 1, NeverReceived: 0},
		{ID: unreceived, Attempts: 2, UnstartedNacks: 0, NeverReceived: 1},
	}, rep.Extras)
	require.Equal(t, 2, rep.ExtraLeases)
	require.Equal(t, 1, rep.UnstartedNacks)
	require.Equal(t, 1, rep.NeverReceived)
}

func TestRedeliveriesAreCountedPerLease(t *testing.T) {
	id := uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completed(id, 3)},
		[]accounting.Log{
			logOf(t, "a", nackedUnstarted(id), shutdown(1), stopped),
			logOf(t, "b", nackedUnstarted(id)),
			logOf(t, "c", started(id)),
		})
	require.Empty(t, rep.Problems())
	require.Len(t, rep.Extras, 1)
	require.Equal(t, 2, rep.ExtraLeases)
	require.Equal(t, 2, rep.UnstartedNacks)
	require.Equal(t, 0, rep.NeverReceived)
}

func TestMoreNacksThanLostLeasesIsAProblem(t *testing.T) {
	twice, once := uuid.New(), uuid.New()
	rep := accounting.Reconcile(
		[]accounting.Row{completed(twice, 2), completed(once, 1)},
		[]accounting.Log{
			logOf(t, "a", nackedUnstarted(twice), nackedUnstarted(once), shutdown(1), stopped),
			logOf(t, "b", nackedUnstarted(twice), started(twice), started(once)),
		})
	require.ElementsMatch(t, []accounting.Extra{
		{ID: twice, Attempts: 2, UnstartedNacks: 2, NeverReceived: -1},
		{ID: once, Attempts: 1, UnstartedNacks: 1, NeverReceived: -1},
	}, rep.Extras)
	problems := rep.Problems()
	for _, id := range []uuid.UUID{twice, once} {
		requireProblem(t, problems, "job "+id.String()+": ")
	}
	disagree := 0
	for _, p := range problems {
		if strings.Contains(p, "the log and the row disagree") {
			disagree++
		}
	}
	require.Equal(t, 2, disagree, problems)
}

func TestAnUnfinishedJobIsNotAnExtra(t *testing.T) {
	id := uuid.New()
	rep := accounting.Reconcile([]accounting.Row{{ID: id, State: "dead", Attempts: 5}},
		[]accounting.Log{logOf(t, "a", started(id)), cleanStop(t)})
	require.Len(t, rep.Unfinished, 1)
	require.Empty(t, rep.Extras)
	require.Zero(t, rep.ExtraLeases)
}

func TestAShutdownMustStopCleanly(t *testing.T) {
	rep := accounting.Reconcile(nil, []accounting.Log{logOf(t, "a", shutdown(1))})
	requireProblem(t, rep.Problems(), "pod a: shutdown started but the worker never logged a clean stop")

	rep = accounting.Reconcile(nil, []accounting.Log{logOf(t, "a", shutdown(1), deadline, stopped)})
	requireProblem(t, rep.Problems(), "pod a: shutdown deadline reached")
}

func TestAnIdleShutdownProvesNothing(t *testing.T) {
	const want = "no worker was shut down while a handler ran"
	rep := accounting.Reconcile(nil, []accounting.Log{logOf(t, "a", shutdown(0), stopped)})
	require.Equal(t, []string{want + "; the run says nothing about graceful shutdown"}, rep.Problems())

	rep = accounting.Reconcile(nil, []accounting.Log{logOf(t, "a")})
	requireProblem(t, rep.Problems(), want)
}

func TestNonJSONOutputIsAProblem(t *testing.T) {
	l := logOf(t, "a", shutdown(1), "", "   ", "worker: abandoned 2 deliveries", stopped)
	require.Len(t, l.Lines, 2)
	require.Equal(t, 1, l.Unparsed)
	rep := accounting.Reconcile(nil, []accounting.Log{l})
	requireProblem(t, rep.Problems(), "pod a: 1 non-JSON lines")
}

func TestLinesForOtherJobsAreIgnored(t *testing.T) {
	cron := uuid.New()
	rep := accounting.Reconcile(nil,
		[]accounting.Log{logOf(t, "a", started(cron), nackedUnstarted(cron), started(cron)), cleanStop(t)})
	require.Empty(t, rep.Problems())
	require.Empty(t, rep.Duplicated)
}

func TestProblemsAreSorted(t *testing.T) {
	var rows []accounting.Row
	var lines []string
	for range 20 {
		id := uuid.New()
		rows = append(rows, completed(id, 1))
		lines = append(lines, started(id), started(id))
	}
	rep := accounting.Reconcile(rows, []accounting.Log{
		logOf(t, "z", "junk"), logOf(t, "y", "junk"), logOf(t, "x", lines...),
	})
	first := rep.Problems()
	require.True(t, slices.IsSorted(first), first)
	for range 10 {
		require.Equal(t, first, rep.Problems())
	}
}

func TestExecutionProblemsAllowIdleShutdowns(t *testing.T) {
	j1, j2 := uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completed(j1, 1), completed(j2, 1)},
		[]accounting.Log{
			logOf(t, "a", started(j1), started(j2)),
			logOf(t, "b", shutdown(0), stopped),
			logOf(t, "c", shutdown(0), stopped),
		})
	require.Empty(t, rep.ExecutionProblems())
	require.Equal(t, []string{"no worker was shut down while a handler ran; the run says nothing about graceful shutdown"},
		rep.Problems(), "Problems keeps demanding a mid-handler shutdown")
}

func TestExecutionProblemsKeepEveryOtherCheck(t *testing.T) {
	id := uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completed(id, 1)},
		[]accounting.Log{
			logOf(t, "a", started(id), shutdown(0)),
			logOf(t, "b", started(id), shutdown(0), stopped),
		})
	require.Equal(t, []string{
		fmt.Sprintf("job %s: handler started 2 times", id),
		"pod a: shutdown started but the worker never logged a clean stop",
	}, rep.ExecutionProblems())
}

const workerStarting = `{"level":"INFO","msg":"worker starting"}`

func startedUnder(id, lease uuid.UUID) string {
	return fmt.Sprintf(`{"level":"INFO","msg":"job started","job_id":%q,"lease_id":%q}`, id, lease)
}

func finishedUnder(id, lease uuid.UUID) string {
	return fmt.Sprintf(`{"level":"INFO","msg":"handler finished","job_id":%q,"lease_id":%q}`, id, lease)
}

func confirmedUnder(id, lease uuid.UUID, op string) string {
	return fmt.Sprintf(`{"level":"INFO","msg":"job confirmed","job_id":%q,"lease_id":%q,"operation":%q}`, id, lease, op)
}

// ran is a delivery that started, finished and was acked.
func ran(id, lease uuid.UUID) []string {
	return []string{startedUnder(id, lease), finishedUnder(id, lease), confirmedUnder(id, lease, "ack")}
}

func completedUnder(id uuid.UUID, attempts int, lease uuid.UUID) accounting.Row {
	return accounting.Row{ID: id, State: "completed", Attempts: attempts, LeaseID: lease}
}

// at stamps a log line with the time slog would have written.
func at(when time.Time, line string) string {
	return fmt.Sprintf(`{"time":%q,`, when.Format(time.RFC3339Nano)) + strings.TrimPrefix(line, "{")
}

func TestAKillExplainsARepeat(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	t0 := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, second)},
		[]accounting.Log{
			// Killed mid-handler, then started again in the same container.
			logOf(t, "w1", at(t0, workerStarting), at(t0.Add(time.Second), startedUnder(id, first)),
				at(t0.Add(3*time.Second), workerStarting)),
			// The reaper hands the job on only after the lease expires.
			logOf(t, "w2", at(t0, workerStarting), at(t0.Add(32*time.Second), startedUnder(id, second)),
				at(t0.Add(33*time.Second), finishedUnder(id, second)), at(t0.Add(33*time.Second), confirmedUnder(id, second, "ack"))),
		})
	require.Empty(t, rep.AttemptProblems())
	require.Equal(t, []accounting.Repeat{{ID: id, LeaseID: first.String(), Pod: "w1", Process: 1, Cause: accounting.CauseKilled,
		StartedAt: t0.Add(time.Second), NextStartedAt: t0.Add(32 * time.Second)}}, rep.Repeats)
	require.Equal(t, []accounting.Process{
		{Pod: "w1", N: 1, Starts: 1, Interrupted: 1, Cut: true, Last: t0.Add(time.Second)},
		{Pod: "w1", N: 2, Last: t0.Add(3 * time.Second)},
		{Pod: "w2", N: 1, Starts: 1, Last: t0.Add(33 * time.Second)},
	}, rep.Processes)
	require.Equal(t, []accounting.Extra{{ID: id, Attempts: 2, Restarted: 1}}, rep.Extras)
	requireProblem(t, rep.ExecutionProblems(), "handler started 2 times")
}

// A lease lost while its process still ran, and a kill that came only
// afterwards: the kill cannot explain the job running again.
func TestAKillAfterTheJobRanAgainExplainsNothing(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	t0 := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, second)},
		[]accounting.Log{
			logOf(t, "w1", at(t0, workerStarting), at(t0.Add(time.Second), startedUnder(id, first)),
				at(t0.Add(40*time.Second), `{"level":"INFO","msg":"lease stream opened"}`),
				at(t0.Add(45*time.Second), workerStarting)),
			logOf(t, "w2", at(t0, workerStarting), at(t0.Add(32*time.Second), startedUnder(id, second)),
				at(t0.Add(33*time.Second), finishedUnder(id, second)), at(t0.Add(33*time.Second), confirmedUnder(id, second, "ack"))),
		})
	requireProblem(t, rep.AttemptProblems(), "the kill came later and cannot explain it")
	require.Empty(t, rep.Repeats[0].Cause)
}

func TestAKillNeedsTimesToExplainARepeat(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, second)},
		[]accounting.Log{
			logOf(t, "w1", workerStarting, startedUnder(id, first), workerStarting),
			logOf(t, "w2", append([]string{workerStarting}, ran(id, second)...)...),
		})
	requireProblem(t, rep.AttemptProblems(), "the log has no times")
}

func TestAnAckedLeaseThatRanAgainIsALostAck(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, second)},
		[]accounting.Log{
			logOf(t, "w1", append([]string{workerStarting}, ran(id, first)...)...),
			logOf(t, "w2", append([]string{workerStarting}, ran(id, second)...)...),
		})
	requireProblem(t, rep.AttemptProblems(), "was acked, yet the job ran again")
}

func TestARepeatNothingExplainsIsAProblem(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, second)},
		[]accounting.Log{
			// w1 is never cut and logs nothing about losing the lease.
			logOf(t, "w1", workerStarting, startedUnder(id, first)),
			logOf(t, "w2", append([]string{workerStarting}, ran(id, second)...)...),
		})
	requireProblem(t, rep.AttemptProblems(), "no kill, nack or record of losing it explains")
}

func TestTheWorkersOwnLossRecordExplainsARepeat(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	refused := fmt.Sprintf(`{"level":"WARN","msg":"job confirmation refused; someone else may have it now","job_id":%q,"lease_id":%q,"operation":"ack"}`, id, first)
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, second)},
		[]accounting.Log{
			logOf(t, "w1", workerStarting, startedUnder(id, first), finishedUnder(id, first), refused),
			logOf(t, "w2", append([]string{workerStarting}, ran(id, second)...)...),
		})
	require.Empty(t, rep.AttemptProblems())
	require.Equal(t, accounting.CauseLost, rep.Repeats[0].Cause)
}

func TestANackedAttemptIsAnOrdinaryRetry(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, second)},
		[]accounting.Log{logOf(t, "w1", append([]string{workerStarting,
			startedUnder(id, first), finishedUnder(id, first), confirmedUnder(id, first, "nack")},
			ran(id, second)...)...)})
	require.Empty(t, rep.AttemptProblems())
	require.Equal(t, accounting.CauseNacked, rep.Repeats[0].Cause)
}

func TestALeaseStartsAtMostOneHandler(t *testing.T) {
	id, lease := uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 1, lease)},
		[]accounting.Log{
			logOf(t, "w1", append([]string{workerStarting}, ran(id, lease)...)...),
			logOf(t, "w2", workerStarting, startedUnder(id, lease)),
		})
	requireProblem(t, rep.AttemptProblems(), "started 2 handlers")
}

func TestTheCompletingLeaseMustHaveStarted(t *testing.T) {
	id, first, final := uuid.New(), uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 2, final)},
		[]accounting.Log{logOf(t, "w1", workerStarting, startedUnder(id, first), workerStarting)})
	requireProblem(t, rep.AttemptProblems(), "which no captured process started")
}

func TestMoreStartedLeasesThanAttemptsIsAProblem(t *testing.T) {
	id, first, second := uuid.New(), uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 1, second)},
		[]accounting.Log{
			logOf(t, "w1", workerStarting, startedUnder(id, first), workerStarting),
			logOf(t, "w2", append([]string{workerStarting}, ran(id, second)...)...),
		})
	requireProblem(t, rep.AttemptProblems(), "2 leases started a handler but the row counts 1 attempts")
}

func TestAttemptsCannotBeJudgedWithoutLeaseIDs(t *testing.T) {
	id := uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completed(id, 1)},
		[]accounting.Log{logOf(t, "w1", started(id))})
	requireProblem(t, rep.AttemptProblems(), "completed with no lease_id")
	require.Empty(t, rep.ExecutionProblems(), "ExecutionProblems does not need lease ids")
}

func TestLinesBeforeTheFirstStartBelongToProcessZero(t *testing.T) {
	id, lease := uuid.New(), uuid.New()
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 1, lease)},
		[]accounting.Log{logOf(t, "w1", append(ran(id, lease), workerStarting)...)})
	require.Empty(t, rep.AttemptProblems())
	require.Equal(t, []accounting.Process{{Pod: "w1", N: 0, Starts: 1, Cut: true}, {Pod: "w1", N: 1}}, rep.Processes)
}

func TestDiagnosticsBeforeWorkerStartingDoNotCreateAProcess(t *testing.T) {
	id, lease := uuid.New(), uuid.New()
	t0 := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	metrics := `{"level":"INFO","msg":"metrics listening"}`
	lines := []string{at(t0, metrics), at(t0.Add(time.Second), workerStarting)}
	for _, line := range ran(id, lease) {
		lines = append(lines, at(t0.Add(2*time.Second), line))
	}
	rep := accounting.Reconcile([]accounting.Row{completedUnder(id, 1, lease)},
		[]accounting.Log{logOf(t, "w1", lines...)})
	require.Empty(t, rep.AttemptProblems())
	require.Equal(t, []accounting.Process{{Pod: "w1", N: 1, Starts: 1, Last: t0.Add(2 * time.Second)}}, rep.Processes)
}
