package accounting_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

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
