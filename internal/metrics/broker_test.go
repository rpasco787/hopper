package metrics

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func newTestBroker(t *testing.T) (*prometheus.Registry, *Broker) {
	t.Helper()
	reg := prometheus.NewRegistry()
	b, err := NewBroker(reg, []string{"q", "r"})
	require.NoError(t, err)
	return reg, b
}

func labelsMatch(metric *dto.Metric, labels map[string]string) bool {
	if len(metric.Label) != len(labels) {
		return false
	}
	for _, pair := range metric.Label {
		if labels[pair.GetName()] != pair.GetValue() {
			return false
		}
	}
	return true
}

func histogram(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (uint64, float64) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if labelsMatch(metric, labels) {
				h := metric.GetHistogram()
				return h.GetSampleCount(), h.GetSampleSum()
			}
		}
	}
	t.Fatalf("histogram %s%v not found", name, labels)
	return 0, 0
}

func metricFamily(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %s not found", name)
	return nil
}

func TestNewRegistryHasRuntimeCollectorsOnly(t *testing.T) {
	reg := NewRegistry()
	families, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]bool, len(families))
	for _, family := range families {
		names[family.GetName()] = true
		require.NotContains(t, family.GetName(), "hopper_")
	}
	require.True(t, names["go_goroutines"])
	switch runtime.GOOS {
	case "aix", "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd", "solaris":
		require.True(t, names["process_start_time_seconds"])
	}
}

func TestBrokerCountsTransitions(t *testing.T) {
	_, b := newTestBroker(t)
	now := time.Now()
	b.Enqueued("q")
	b.Completed("q", now, now.Add(time.Second))
	b.Nacked("q", false)
	b.Nacked("q", true)
	b.LeaseExpired("q", false)
	b.LeaseExpired("q", true)

	require.Equal(t, float64(1), testutil.ToFloat64(b.enqueued.WithLabelValues("q")))
	require.Equal(t, float64(1), testutil.ToFloat64(b.completed.WithLabelValues("q")))
	require.Equal(t, float64(2), testutil.ToFloat64(b.failed.WithLabelValues("q")))
	require.Equal(t, float64(2), testutil.ToFloat64(b.dead.WithLabelValues("q")))
	require.Equal(t, float64(2), testutil.ToFloat64(b.expired.WithLabelValues("q")))
}

func TestBrokerObservesFirstLeaseWaitOnly(t *testing.T) {
	reg, b := newTestBroker(t)
	leasedAt := time.Now()
	b.Leased([]store.Job{
		{Queue: "q", Attempts: 1, RunAt: leasedAt.Add(-2 * time.Second), LeasedAt: &leasedAt},
		{Queue: "q", Attempts: 3, RunAt: leasedAt.Add(-5 * time.Second), LeasedAt: &leasedAt},
	})

	count, sum := histogram(t, reg, "hopper_job_wait_seconds", map[string]string{"queue": "q"})
	require.Equal(t, uint64(1), count)
	require.InDelta(t, 2, sum, 0.000001)
}

func TestBrokerObservesRunTimeOnCompletion(t *testing.T) {
	reg, b := newTestBroker(t)
	now := time.Now()
	b.Completed("q", now, now.Add(1500*time.Millisecond))
	b.Completed("q", time.Time{}, now)

	count, sum := histogram(t, reg, "hopper_job_run_seconds", map[string]string{"queue": "q"})
	require.Equal(t, uint64(1), count)
	require.InDelta(t, 1.5, sum, 0.000001)
	require.Equal(t, float64(2), testutil.ToFloat64(b.completed.WithLabelValues("q")))
}

func TestBrokerHistogramEdges(t *testing.T) {
	reg, b := newTestBroker(t)
	runAt := time.Now()
	leasedAt := runAt.Add(-time.Second)
	b.Leased([]store.Job{{Queue: "q", Attempts: 1, RunAt: runAt, LeasedAt: &leasedAt}})
	b.Completed("q", runAt, runAt.Add(900*time.Second))

	count, sum := histogram(t, reg, "hopper_job_wait_seconds", map[string]string{"queue": "q"})
	require.Equal(t, uint64(1), count)
	require.Zero(t, sum)
	runFamily := metricFamily(t, reg, "hopper_job_run_seconds")
	for _, metric := range runFamily.Metric {
		if !labelsMatch(metric, map[string]string{"queue": "q"}) {
			continue
		}
		require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
		for _, bucket := range metric.GetHistogram().Bucket {
			if bucket.GetUpperBound() == 600 {
				require.Zero(t, bucket.GetCumulativeCount())
				return
			}
		}
	}
	t.Fatal("run histogram queue=q or le=600 bucket not found")
}

func TestBrokerPrecreatesAllowlistedSeries(t *testing.T) {
	reg, _ := newTestBroker(t)
	for _, name := range []string{
		"hopper_jobs_enqueued_total", "hopper_jobs_completed_total",
		"hopper_jobs_failed_total", "hopper_jobs_dead_total",
		"hopper_lease_expirations_total", "hopper_job_wait_seconds",
		"hopper_job_run_seconds",
	} {
		family := metricFamily(t, reg, name)
		require.Len(t, family.Metric, 3, name)
		queues := make(map[string]bool)
		for _, metric := range family.Metric {
			queues[metric.Label[0].GetValue()] = true
			if metric.Histogram != nil {
				require.Zero(t, metric.GetHistogram().GetSampleCount())
			} else {
				require.Zero(t, metric.GetCounter().GetValue())
			}
		}
		require.Equal(t, map[string]bool{"q": true, "r": true, OtherQueue: true}, queues)
	}
}

func TestBrokerFoldsUnlistedQueues(t *testing.T) {
	reg, b := newTestBroker(t)
	for i := range 1000 {
		b.Enqueued(fmt.Sprintf("hostile-%d", i))
	}
	require.Equal(t, float64(1000), testutil.ToFloat64(b.enqueued.WithLabelValues(OtherQueue)))
	for _, name := range []string{
		"hopper_jobs_enqueued_total", "hopper_jobs_completed_total",
		"hopper_jobs_failed_total", "hopper_jobs_dead_total",
		"hopper_lease_expirations_total", "hopper_job_wait_seconds",
		"hopper_job_run_seconds",
	} {
		require.Len(t, metricFamily(t, reg, name).Metric, 3, name)
	}
}

func TestNewBrokerReturnsRegistrationErrors(t *testing.T) {
	reg := prometheus.NewRegistry()
	_, err := NewBroker(reg, []string{"q", "r"})
	require.NoError(t, err)
	require.NotPanics(t, func() {
		_, err = NewBroker(reg, []string{"q", "r"})
	})
	require.Error(t, err)
}

func TestBrokerIsConcurrencySafe(t *testing.T) {
	_, b := newTestBroker(t)
	const goroutines, calls = 50, 100
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range calls {
				switch i % 5 {
				case 0:
					b.Enqueued("q")
				case 1:
					b.Completed("q", time.Time{}, time.Time{})
				case 2:
					b.Nacked("q", false)
				case 3:
					b.Nacked("q", true)
				case 4:
					b.LeaseExpired("q", true)
				}
			}
		}()
	}
	wg.Wait()
	want := float64(goroutines * calls / 5)
	require.Equal(t, want, testutil.ToFloat64(b.enqueued.WithLabelValues("q")))
	require.Equal(t, want, testutil.ToFloat64(b.completed.WithLabelValues("q")))
	require.Equal(t, 2*want, testutil.ToFloat64(b.failed.WithLabelValues("q")))
	require.Equal(t, 2*want, testutil.ToFloat64(b.dead.WithLabelValues("q")))
	require.Equal(t, want, testutil.ToFloat64(b.expired.WithLabelValues("q")))
}
