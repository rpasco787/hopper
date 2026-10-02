package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// listeningBrokers counts the named processes' sessions that are listening for
// wakeups.
func listeningBrokers(t *testing.T, names ...string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `
		select count(*) from pg_stat_activity
		where application_name = any($1) and query = 'listen hopper_available'`, names).Scan(&n))
	return n
}

// An enqueue on one broker wakes an idle lease stream on another. Both
// brokers poll once an hour, so a delivery within seconds can only have come
// through the database's NOTIFY. It also checks that the receiving broker exports its
// pool, with the listener's connection among the acquired ones.
func TestAnEnqueueOnOneBrokerWakesAStreamOnAnother(t *testing.T) {
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)
	binary := buildBrokerProcess(t)
	a := startBrokerProcess(t, binary, "hopper-wake-a", "", "-scheduler=false", "-poll-interval=1h", "-log-level=debug")
	b := startBrokerProcess(t, binary, "hopper-wake-b", "", "-scheduler=false", "-poll-interval=1h", "-log-level=debug")
	t.Cleanup(func() { stopBrokerProcess(t, a); stopBrokerProcess(t, b) })
	aBroker, _ := a.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "broker listening" })
	bBroker, _ := b.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "broker listening" })
	bMetrics, _ := b.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "metrics listening" })
	require.Eventually(t, func() bool { return listeningBrokers(t, "hopper-wake-a", "hopper-wake-b") == 2 },
		10*time.Second, 50*time.Millisecond, "both brokers listen for wakeups")

	// The initial all-queues wake is complete before the stream opens.
	b.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "wakeup listener ready" })

	stream, err := newClientAt(t, bBroker.Addr).LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"wake"}, WorkerId: "w", Credit: 1,
		}},
	}))
	b.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "lease stream opened" })
	// An idle session whose last query was a lease proves the first query
	// finished before enqueue; the table is still empty at this point.
	require.Eventually(t, func() bool {
		var finished bool
		err := pool.QueryRow(ctx, `select exists (
			select 1 from pg_stat_activity where application_name = 'hopper-wake-b'
			and state = 'idle' and query like '%with next as%'
			and query like '%leased_by%')`).Scan(&finished)
		return err == nil && finished
	}, 10*time.Second, 20*time.Millisecond, "the stream's first empty lease completed")

	enqueued, err := newClientAt(t, aBroker.Addr).Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "wake"}})
	require.NoError(t, err)
	delivered := make(chan *hopperv1.Job, 1)
	go func() {
		if resp, err := stream.Recv(); err == nil {
			delivered <- resp.GetJob()
		}
	}()
	select {
	case job := <-delivered:
		require.Equal(t, enqueued.GetJob().GetId(), job.GetId())
	case <-time.After(5 * time.Second):
		t.Fatal("the stream on the other broker was not woken; it would have polled in an hour")
	}

	require.Equal(t, float64(16), scrapedValue(t, bMetrics.Addr, "hopper_db_pool_max_connections", nil))
	require.GreaterOrEqual(t, scrapedValue(t, bMetrics.Addr, "hopper_db_pool_connections", map[string]string{"state": "acquired"}),
		float64(1), "the wakeup listener holds a connection")
}
