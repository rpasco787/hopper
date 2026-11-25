package accounting

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// job returns the nth job id of a test's run.
func job(n int) string { return fmt.Sprintf("0193a000-0000-7000-8000-%012x", 0x1000+n) }

// lease returns the nth lease id of a test's run.
func lease(n int) string { return fmt.Sprintf("0193a000-0000-7000-8001-%012x", 0x1000+n) }

// opened is n deliveries started in a row under their own leases, from job
// first, one millisecond apart.
func opened(first, n, ms int) []string {
	var s []string
	for i := first; i < first+n; i++ {
		s = append(s, start(job(i), lease(i), ms+i))
	}
	return s
}

func TestTheBoundCountsFromStartToTheTerminalLine(t *testing.T) {
	l := lines(t, "a", append([]string{starting("4", 0)}, opened(0, 4, 10)...)...)
	peaks, p := ConcurrencyProblems([]Log{l})
	require.Empty(t, p)
	require.Equal(t, []Peak{{Pod: "a", Process: 1, Concurrency: 4, Max: 4}}, peaks)

	l = lines(t, "a", append([]string{starting("4", 0)}, opened(0, 5, 10)...)...)
	peaks, p = ConcurrencyProblems([]Log{l})
	require.Len(t, p, 1, "%q", p)
	requireHas(t, p, "pod a process 1", "5 deliveries open at once", "concurrency 4")
	require.Equal(t, 5, peaks[0].Max)
}

func TestATerminalLineFreesItsPlace(t *testing.T) {
	for msg := range terminalMessages {
		t.Run(msg, func(t *testing.T) {
			op := ""
			if msg == msgJobConfirmed {
				op = "ack"
			}
			l := lines(t, "a", starting("1", 0),
				start(job(0), lease(0), 10), finish(job(0), lease(0), 20), sp(msg, job(0), lease(0), op, 30),
				start(job(1), lease(1), 40))
			peaks, p := ConcurrencyProblems([]Log{l})
			require.Empty(t, p)
			require.Equal(t, 1, peaks[0].Max)
		})
	}
}

func TestTheSameJobUnderANewLeaseIsANewDelivery(t *testing.T) {
	// The same job under two leases is two deliveries: the second opens while
	// the first is still open only if the first never ended.
	l := lines(t, "a", starting("1", 0),
		start(job(0), lease(0), 10), finish(job(0), lease(0), 20), ack(job(0), lease(0), 30),
		start(job(0), lease(1), 40))
	_, p := ConcurrencyProblems([]Log{l})
	require.Empty(t, p)
}

func TestAnUnstartedNackHoldsAPlace(t *testing.T) {
	l := lines(t, "a", starting("1", 0),
		unstarted(job(0), lease(0), 10), start(job(1), lease(1), 15), nack(job(0), lease(0), 20))
	peaks, p := ConcurrencyProblems([]Log{l})
	require.Equal(t, 2, peaks[0].Max)
	requireHas(t, p, "2 deliveries open at once", "concurrency 1")
}

func TestEachProcessHasItsOwnBound(t *testing.T) {
	l := lines(t, "a", append(append([]string{starting("4", 0)}, opened(0, 4, 10)...), // killed with 4 open
		append([]string{starting("4", 40000)}, opened(10, 4, 40010)...)...)...)
	peaks, p := ConcurrencyProblems([]Log{l})
	require.Empty(t, p)
	require.Equal(t, []Peak{
		{Pod: "a", Process: 1, Concurrency: 4, Max: 4},
		{Pod: "a", Process: 2, Concurrency: 4, Max: 4},
	}, peaks)
}

func TestFileOrderNotTimesDecides(t *testing.T) {
	// File order: A starts, A ends, B starts. The times disagree with it by
	// microseconds in practice (two writers, one lock); here by a lot.
	inOrder := lines(t, "a", starting("1", 0),
		start(job(0), lease(0), 100), ack(job(0), lease(0), 200), start(job(1), lease(1), 300))
	swapped := lines(t, "a", starting("1", 0),
		start(job(0), lease(0), 300), ack(job(0), lease(0), 200), start(job(1), lease(1), 100))
	for name, l := range map[string]Log{"times in order": inOrder, "times against the file": swapped} {
		peaks, p := ConcurrencyProblems([]Log{l})
		require.Empty(t, p, name)
		require.Equal(t, 1, peaks[0].Max, name)
	}
}

func TestAProcessWithoutConcurrencyIsAProblem(t *testing.T) {
	l := lines(t, "a", append([]string{starting("", 0)}, opened(0, 1, 10)...)...)
	peaks, p := ConcurrencyProblems([]Log{l})
	requireHas(t, p, "pod a process 1", "no concurrency")
	require.Equal(t, 1, peaks[0].Max)

	idle := lines(t, "b", starting("", 0))
	_, p = ConcurrencyProblems([]Log{idle})
	require.Empty(t, p, "a process that ran nothing needs no bound")
}

func TestLinesWithoutAJobAreIgnored(t *testing.T) {
	l := lines(t, "a", starting("1", 0),
		start(job(0), lease(0), 10),
		sp("shutdown started", "", "", "", 20),
		sp(msgJobConfirmed, "", "", "ack", 30), // no job_id: closes nothing
		sp(msgJobStarted, "", "", "", 35),      // no job_id: opens nothing
		sp("scheduler tick fired", "", "", "", 40))
	peaks, p := ConcurrencyProblems([]Log{l})
	require.Empty(t, p)
	require.Equal(t, 1, peaks[0].Max)
}

func TestARepeatedFirstLineOpensNothingNew(t *testing.T) {
	l := lines(t, "a", starting("1", 0),
		start(job(0), lease(0), 10), start(job(0), lease(0), 11), finish(job(0), lease(0), 12), ack(job(0), lease(0), 13))
	peaks, p := ConcurrencyProblems([]Log{l})
	require.Empty(t, p)
	require.Equal(t, 1, peaks[0].Max)
}

func TestTheSameJobUnderTwoOpenLeasesIsTwoDeliveries(t *testing.T) {
	// The reaper took a lease back while its handler still ran, and the job was
	// leased to the same process again: two tokens are held.
	l := lines(t, "a", starting("1", 0), start(job(0), lease(0), 10), start(job(0), lease(1), 20))
	peaks, p := ConcurrencyProblems([]Log{l})
	require.Equal(t, 2, peaks[0].Max)
	requireHas(t, p, "2 deliveries open at once")
}

func TestMaxIsThePeakNotTheLastCount(t *testing.T) {
	l := lines(t, "a", starting("4", 0),
		start(job(0), lease(0), 10), start(job(1), lease(1), 11),
		finish(job(0), lease(0), 20), ack(job(0), lease(0), 21),
		finish(job(1), lease(1), 22), ack(job(1), lease(1), 23),
		start(job(2), lease(2), 30))
	peaks, p := ConcurrencyProblems([]Log{l})
	require.Empty(t, p)
	require.Equal(t, 2, peaks[0].Max)
}
