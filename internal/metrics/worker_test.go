package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	workersdk "github.com/rpasco787/hopper/worker"
)

func TestWorkerMetricsTrackInflightAndOutcomes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewWorker(reg, []string{"q"})
	require.NoError(t, err)

	m.HandlerStarted("q")
	m.HandlerStarted("q")
	require.Equal(t, float64(2), testutil.ToFloat64(m.inflight))
	m.HandlerFinished("q", workersdk.HandlerError, 250*time.Millisecond)

	require.Equal(t, float64(1), testutil.ToFloat64(m.inflight))
	count, sum := histogram(t, reg, "hopper_worker_handler_seconds", map[string]string{
		"queue": "q", "status": "error",
	})
	require.Equal(t, uint64(1), count)
	require.InDelta(t, 0.25, sum, 0.000001)
}

func TestWorkerMetricsPrecreateRegisteredQueues(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewWorker(reg, []string{"a", "b"})
	require.NoError(t, err)

	family := metricFamily(t, reg, "hopper_worker_handler_seconds")
	require.Len(t, family.Metric, 6)
	seen := make(map[string]bool)
	for _, metric := range family.Metric {
		labels := make(map[string]string)
		for _, pair := range metric.Label {
			labels[pair.GetName()] = pair.GetValue()
		}
		seen[labels["queue"]+"/"+labels["status"]] = true
		require.Zero(t, metric.GetHistogram().GetSampleCount())
	}
	require.Equal(t, map[string]bool{
		"a/ok": true, "a/error": true, "a/panic": true,
		"b/ok": true, "b/error": true, "b/panic": true,
	}, seen)

	m.HandlerFinished("unlisted", workersdk.HandlerOK, time.Second)
	require.Len(t, metricFamily(t, reg, "hopper_worker_handler_seconds").Metric, 6)
}
