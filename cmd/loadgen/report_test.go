package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSummaryIsExactNearestRank(t *testing.T) {
	samples := make([]time.Duration, 0, 100)
	for i := 100; i >= 1; i-- { // unsorted on purpose
		samples = append(samples, time.Duration(i)*time.Millisecond)
	}
	s := summarize(samples)
	require.Equal(t, Summary{N: 100, P50: 50, P90: 90, P99: 99, Max: 100}, s)
}

func TestSummaryOfFewSamples(t *testing.T) {
	require.Equal(t, Summary{}, summarize(nil))
	one := summarize([]time.Duration{1500 * time.Microsecond})
	require.Equal(t, Summary{N: 1, P50: 1.5, P90: 1.5, P99: 1.5, Max: 1.5}, one)
	// With three samples, p50 is the second and p90 and p99 the third.
	three := summarize([]time.Duration{3 * time.Millisecond, time.Millisecond, 2 * time.Millisecond})
	require.Equal(t, Summary{N: 3, P50: 2, P90: 3, P99: 3, Max: 3}, three)
}

func TestReportFailsOnErrorsOrUnfinishedJobs(t *testing.T) {
	require.False(t, (&Report{Jobs: 10}).Failed())
	require.True(t, (&Report{Jobs: 10, Errors: 1}).Failed())
	require.True(t, (&Report{Jobs: 10, Waited: true, Finished: 9, Unfinished: 1}).Failed())
	require.True(t, (&Report{Jobs: 10, Waited: true, Finished: 9}).Failed(), "a job loadgen never found is not finished")
	require.False(t, (&Report{Jobs: 10, Waited: true, Finished: 10}).Failed())
}

func TestSummaryLineNamesEveryHeadlineNumber(t *testing.T) {
	var b bytes.Buffer
	r := &Report{Run: "r", Jobs: 5, Calls: 5, EnqueueRate: 100, Waited: true, Finished: 5,
		Enqueue: Summary{P50: 1, P99: 2}, Pickup: Summary{P50: 3, P99: 4}, EndToEnd: Summary{P50: 5, P99: 6}, Throughput: 50}
	r.writeSummary(&b)
	for _, want := range []string{"run=r", "enqueue_ms p50=1.00 p99=2.00", "pickup_ms p50=3.0 p99=4.0", "e2e_ms p50=5.0 p99=6.0", "throughput=50/s"} {
		require.Contains(t, b.String(), want)
	}
}
