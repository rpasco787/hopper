package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/rpasco787/hopper/client"
	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/metrics"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
	"github.com/rpasco787/hopper/internal/sweep"
	workersdk "github.com/rpasco787/hopper/worker"
)

func TestMetricsMatchTheDatabase(t *testing.T) {
	ctx := ctxT(t)
	brokerReg := prometheus.NewRegistry()
	bm, err := metrics.NewBroker(brokerReg, []string{"mq"})
	require.NoError(t, err)
	st := postgres.New(pool).
		WithRetryPolicy(store.RetryPolicy{Base: time.Millisecond, Cap: time.Millisecond}).
		WithObserver(bm)
	addr := startBrokerWithConfig(t, st, broker.Config{
		LeaseDuration: time.Second, PollInterval: 5 * time.Millisecond,
		Logger: slog.New(slog.DiscardHandler),
	})
	promoter := sweep.Promoter(st)
	promoter.Interval = 5 * time.Millisecond
	startMaintenance(t, promoter)

	workerReg := prometheus.NewRegistry()
	wm, err := metrics.NewWorker(workerReg, []string{"mq", "unlisted"})
	require.NoError(t, err)
	w := workersdk.New(addr, workersdk.Options{
		ID: "metrics-worker", Concurrency: 8, ShutdownTimeout: 5 * time.Second,
		Logger: slog.New(slog.DiscardHandler), Observer: wm,
	})
	handler := func(_ context.Context, job workersdk.Job) error {
		var payload struct {
			Poison bool `json:"poison"`
		}
		if err := job.Unmarshal(&payload); err != nil {
			return err
		}
		if payload.Poison {
			return errPoison
		}
		return nil
	}
	w.Handle("mq", handler)
	w.Handle("unlisted", handler)
	workerRun := startSDKWorker(t, w)

	producer := producerAt(t, addr)
	for i := range 20 {
		_, err := producer.Enqueue(ctx, client.Job{
			Queue: "mq", Payload: json.RawMessage(`{}`), IdempotencyKey: jobKey(i),
		})
		require.NoError(t, err)
	}
	_, err = producer.Enqueue(ctx, client.Job{
		Queue: "mq", Payload: json.RawMessage(`{"poison":true}`), MaxAttempts: 2,
	})
	require.NoError(t, err)
	duplicate, err := producer.Enqueue(ctx, client.Job{
		Queue: "mq", Payload: json.RawMessage(`{}`), IdempotencyKey: jobKey(0),
	})
	require.NoError(t, err)
	require.True(t, duplicate.Deduplicated)

	require.Eventually(t, func() bool {
		var completed, dead int
		err := pool.QueryRow(ctx, `
			select count(*) filter (where state = 'completed'),
			       count(*) filter (where state = 'dead')
			from jobs where queue = 'mq'`).Scan(&completed, &dead)
		return err == nil && completed == 20 && dead == 1
	}, 20*time.Second, 10*time.Millisecond)

	require.Equal(t, float64(21), gatheredValue(t, brokerReg, "hopper_jobs_enqueued_total", map[string]string{"queue": "mq"}))
	require.Equal(t, float64(20), gatheredValue(t, brokerReg, "hopper_jobs_completed_total", map[string]string{"queue": "mq"}))
	require.Equal(t, float64(2), gatheredValue(t, brokerReg, "hopper_jobs_failed_total", map[string]string{"queue": "mq"}))
	require.Equal(t, float64(1), gatheredValue(t, brokerReg, "hopper_jobs_dead_total", map[string]string{"queue": "mq"}))
	require.Zero(t, gatheredValue(t, brokerReg, "hopper_lease_expirations_total", map[string]string{"queue": "mq"}))
	require.Equal(t, uint64(21), gatheredHistogram(t, brokerReg, "hopper_job_wait_seconds", map[string]string{"queue": "mq"}).GetSampleCount())
	require.Equal(t, uint64(20), gatheredHistogram(t, brokerReg, "hopper_job_run_seconds", map[string]string{"queue": "mq"}).GetSampleCount())
	require.Equal(t, uint64(20), gatheredHistogram(t, workerReg, "hopper_worker_handler_seconds", map[string]string{"queue": "mq", "status": "ok"}).GetSampleCount())
	require.Equal(t, uint64(2), gatheredHistogram(t, workerReg, "hopper_worker_handler_seconds", map[string]string{"queue": "mq", "status": "error"}).GetSampleCount())
	require.Zero(t, gatheredValue(t, workerReg, "hopper_worker_inflight_jobs", nil))

	require.NoError(t, (&metrics.DepthSampler{Source: st, Metrics: bm}).Once(ctx))
	require.Zero(t, gatheredValue(t, brokerReg, "hopper_queue_depth", map[string]string{"queue": "mq", "state": "available"}))
	require.Zero(t, gatheredValue(t, brokerReg, "hopper_queue_depth", map[string]string{"queue": "mq", "state": "leased"}))
	require.Equal(t, float64(1), gatheredValue(t, brokerReg, "hopper_queue_depth", map[string]string{"queue": "mq", "state": "dead"}))

	raw := newClientAt(t, addr)
	reapJob, err := raw.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "reap", MaxAttempts: 2}})
	require.NoError(t, err)
	stream, err := raw.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"reap"}, WorkerId: "reap-test", Credit: 1,
			LeaseDuration: durationpb.New(50 * time.Millisecond),
		}},
	}))
	leased, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, reapJob.GetJob().GetId(), leased.GetJob().GetId())
	time.Sleep(75 * time.Millisecond)
	reaped, err := st.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	require.Equal(t, float64(1), gatheredValue(t, brokerReg, "hopper_lease_expirations_total", map[string]string{"queue": metrics.OtherQueue}))
	require.Equal(t, float64(1), gatheredValue(t, brokerReg, "hopper_jobs_dead_total", map[string]string{"queue": "mq"}))

	for range 3 {
		_, err := producer.Enqueue(ctx, client.Job{Queue: "unlisted", Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
	}
	waitForCompleted(t, ctx, "unlisted", 3)
	require.Equal(t, float64(3), gatheredValue(t, brokerReg, "hopper_jobs_completed_total", map[string]string{"queue": metrics.OtherQueue}))
	for _, family := range mustGather(t, brokerReg) {
		for _, metric := range family.GetMetric() {
			require.False(t, metricLabelsMatch(metric, map[string]string{"queue": "unlisted"}), family.GetName())
		}
	}
	require.NoError(t, workerRun.stop(t))
}

var errPoison = &poisonError{}

type poisonError struct{}

func (*poisonError) Error() string { return "poison" }

func jobKey(i int) string { return "metrics-key-" + string(rune('a'+i)) }

func mustGather(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	return families
}

func gatheredFamily(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	for _, family := range mustGather(t, reg) {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %s not found", name)
	return nil
}

func metricLabelsMatch(metric *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return len(metric.GetLabel()) == 0
	}
	for name, want := range labels {
		found := false
		for _, pair := range metric.GetLabel() {
			if pair.GetName() == name && pair.GetValue() == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func gatheredMetric(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	for _, metric := range gatheredFamily(t, reg, name).GetMetric() {
		if metricLabelsMatch(metric, labels) {
			return metric
		}
	}
	t.Fatalf("metric %s%v not found", name, labels)
	return nil
}

func gatheredValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	metric := gatheredMetric(t, reg, name, labels)
	if metric.Gauge != nil {
		return metric.GetGauge().GetValue()
	}
	return metric.GetCounter().GetValue()
}

func gatheredHistogram(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) *dto.Histogram {
	t.Helper()
	return gatheredMetric(t, reg, name, labels).GetHistogram()
}
