package metrics

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func newDepthBroker(t *testing.T) (*prometheus.Registry, *Broker) {
	t.Helper()
	reg := prometheus.NewRegistry()
	b, err := NewBroker(reg, []string{"a"})
	require.NoError(t, err)
	return reg, b
}

func depthRows(n int64) []store.QueueDepth {
	return []store.QueueDepth{
		{Queue: "a", State: store.StateScheduled, Count: n},
		{Queue: "a", State: store.StateAvailable, Count: n},
		{Queue: "a", State: store.StateLeased, Count: n},
		{Queue: "a", State: store.StateDead, Count: n},
	}
}

func depthValues(t *testing.T, reg *prometheus.Registry, queue string) map[string]float64 {
	t.Helper()
	family := metricFamily(t, reg, "hopper_queue_depth")
	got := make(map[string]float64)
	for _, metric := range family.Metric {
		labels := metricLabels(metric)
		if labels["queue"] == queue {
			got[labels["state"]] = metric.GetGauge().GetValue()
		}
	}
	return got
}

func metricLabels(metric *dto.Metric) map[string]string {
	labels := make(map[string]string, len(metric.Label))
	for _, pair := range metric.Label {
		labels[pair.GetName()] = pair.GetValue()
	}
	return labels
}

func sampleTimestamp(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	family := metricFamily(t, reg, "hopper_queue_depth_last_sample_timestamp_seconds")
	require.Len(t, family.Metric, 1)
	return family.Metric[0].GetGauge().GetValue()
}

func TestDepthReportsEveryStateForLabelledQueues(t *testing.T) {
	_, b := newDepthBroker(t)
	require.NoError(t, b.UpdateDepth([]store.QueueDepth{{Queue: "a", State: store.StateAvailable, Count: 3}}, time.Unix(10, 0)))
	require.NoError(t, testutil.CollectAndCompare(b.depth, strings.NewReader(`# HELP hopper_queue_depth Jobs in each non-terminal work state at the last successful sample.
# TYPE hopper_queue_depth gauge
hopper_queue_depth{queue="_other",state="available"} 0
hopper_queue_depth{queue="_other",state="dead"} 0
hopper_queue_depth{queue="_other",state="leased"} 0
hopper_queue_depth{queue="_other",state="scheduled"} 0
hopper_queue_depth{queue="a",state="available"} 3
hopper_queue_depth{queue="a",state="dead"} 0
hopper_queue_depth{queue="a",state="leased"} 0
hopper_queue_depth{queue="a",state="scheduled"} 0
`), "hopper_queue_depth"))
}

func TestDrainedQueueReadsZero(t *testing.T) {
	reg, b := newDepthBroker(t)
	require.NoError(t, b.UpdateDepth([]store.QueueDepth{{Queue: "a", State: store.StateAvailable, Count: 3}}, time.Unix(10, 0)))
	require.NoError(t, b.UpdateDepth(nil, time.Unix(20, 0)))
	require.Equal(t, map[string]float64{
		"scheduled": 0, "available": 0, "leased": 0, "dead": 0,
	}, depthValues(t, reg, "a"))
}

func TestDepthFoldsUnlistedQueuesIntoOther(t *testing.T) {
	reg, b := newDepthBroker(t)
	require.NoError(t, b.UpdateDepth([]store.QueueDepth{
		{Queue: "x", State: store.StateAvailable, Count: 2},
		{Queue: "y", State: store.StateAvailable, Count: 5},
	}, time.Unix(10, 0)))
	require.Equal(t, float64(7), depthValues(t, reg, OtherQueue)["available"])
	family := metricFamily(t, reg, "hopper_queue_depth")
	for _, metric := range family.Metric {
		queue := metricLabels(metric)["queue"]
		require.NotEqual(t, "x", queue)
		require.NotEqual(t, "y", queue)
	}
}

type fakeDepthSource struct {
	mu      sync.Mutex
	rows    []store.QueueDepth
	err     error
	calls   int
	blocked bool
}

func (f *fakeDepthSource) QueueDepths(ctx context.Context) ([]store.QueueDepth, error) {
	f.mu.Lock()
	f.calls++
	blocked := f.blocked
	rows := append([]store.QueueDepth(nil), f.rows...)
	err := f.err
	f.mu.Unlock()
	if blocked {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return rows, err
}

func (f *fakeDepthSource) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestDepthRejectsInvalidSamples(t *testing.T) {
	tests := map[string][]store.QueueDepth{
		"empty queue":    {{Queue: "", State: store.StateAvailable, Count: 1}},
		"terminal state": {{Queue: "a", State: store.StateCompleted, Count: 1}},
		"negative count": {{Queue: "a", State: store.StateAvailable, Count: -1}},
		"duplicate": {
			{Queue: "a", State: store.StateAvailable, Count: 1},
			{Queue: "a", State: store.StateAvailable, Count: 2},
		},
	}
	for name, rows := range tests {
		t.Run(name, func(t *testing.T) {
			reg, b := newDepthBroker(t)
			at := time.Unix(10, 0)
			require.NoError(t, b.UpdateDepth(depthRows(4), at))
			var logs bytes.Buffer
			sampler := &DepthSampler{
				Source:   &fakeDepthSource{rows: rows},
				Metrics:  b,
				Interval: time.Second,
				Logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
			}

			require.Error(t, sampler.Once(context.Background()))
			require.Equal(t, depthValues(t, reg, "a"), map[string]float64{
				"scheduled": 4, "available": 4, "leased": 4, "dead": 4,
			})
			require.Equal(t, float64(at.Unix()), sampleTimestamp(t, reg))
			require.Equal(t, 1, strings.Count(logs.String(), `"level":"WARN"`))
		})
	}
}

func TestScrapeNeverSeesAHalfUpdate(t *testing.T) {
	reg, b := newDepthBroker(t)
	require.NoError(t, b.UpdateDepth(depthRows(1), time.Now()))
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := int64(1); ; n = 3 - n {
			select {
			case <-stop:
				return
			default:
				require.NoError(t, b.UpdateDepth(depthRows(n), time.Now()))
			}
		}
	}()
	for range 1000 {
		values := depthValues(t, reg, "a")
		require.Len(t, values, 4)
		var first float64
		for _, state := range []string{"scheduled", "available", "leased", "dead"} {
			if first == 0 {
				first = values[state]
			}
			require.Equal(t, first, values[state])
		}
	}
	close(stop)
	<-done
}

func TestSamplerKeepsTheLastSnapshotOnError(t *testing.T) {
	reg, b := newDepthBroker(t)
	source := &fakeDepthSource{rows: depthRows(3)}
	var logs bytes.Buffer
	sampler := &DepthSampler{
		Source: source, Metrics: b, Interval: time.Second,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	require.NoError(t, sampler.Once(context.Background()))
	before := sampleTimestamp(t, reg)
	source.mu.Lock()
	source.err = errors.New("db down")
	source.rows = nil
	source.mu.Unlock()

	require.ErrorContains(t, sampler.Once(context.Background()), "db down")
	require.Equal(t, map[string]float64{
		"scheduled": 3, "available": 3, "leased": 3, "dead": 3,
	}, depthValues(t, reg, "a"))
	require.Equal(t, before, sampleTimestamp(t, reg))
	require.Equal(t, 1, strings.Count(logs.String(), `"level":"WARN"`))
}

func TestSamplerRunsImmediatelyThenOnInterval(t *testing.T) {
	_, b := newDepthBroker(t)
	source := &fakeDepthSource{}
	sampler := &DepthSampler{Source: source, Metrics: b, Interval: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sampler.Run(ctx) }()
	require.Eventually(t, func() bool { return source.count() >= 3 }, 100*time.Millisecond, 5*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sampler did not stop promptly")
	}
}

func TestSamplerBoundsEachSample(t *testing.T) {
	_, b := newDepthBroker(t)
	source := &fakeDepthSource{blocked: true}
	sampler := &DepthSampler{Source: source, Metrics: b, Interval: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sampler.Run(ctx) }()
	require.Eventually(t, func() bool { return source.count() >= 2 }, 100*time.Millisecond, 5*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
