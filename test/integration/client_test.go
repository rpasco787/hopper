package integration

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rpasco787/hopper/client"
	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

// producer dials the real broker with the public client package. It also
// returns the generated client for the same broker, because a producer's Result
// carries only an ID and a state and the tests need to see the stored row.
func producer(t *testing.T) (*client.Client, hopperv1.HopperServiceClient) {
	t.Helper()

	addr := startBroker(t, postgres.New(pool))
	c, err := client.Dial(addr)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return c, hopperv1.NewHopperServiceClient(conn)
}

func TestProducerEnqueueRoundTripsAStructPayload(t *testing.T) {
	ctx := ctxT(t)
	c, raw := producer(t)

	type order struct {
		ID    int      `json:"id"`
		Items []string `json:"items"`
	}
	res, err := c.Enqueue(ctx, client.Job{Queue: "orders", Payload: order{ID: 7, Items: []string{"a", "b"}}})
	require.NoError(t, err)
	require.False(t, res.Deduplicated)
	require.Equal(t, "JOB_STATE_AVAILABLE", res.State)

	stored, err := raw.GetJob(ctx, &hopperv1.GetJobRequest{Id: res.ID.String()})
	require.NoError(t, err)
	require.JSONEq(t, `{"id":7,"items":["a","b"]}`, string(stored.GetJob().GetPayload()))
}

func TestProducerIdempotencyKeyDeduplicates(t *testing.T) {
	ctx := ctxT(t)
	c, _ := producer(t)

	job := client.Job{Queue: "orders", IdempotencyKey: "order-1"}
	first, err := c.Enqueue(ctx, job)
	require.NoError(t, err)
	require.False(t, first.Deduplicated)

	second, err := c.Enqueue(ctx, job)
	require.NoError(t, err)
	require.True(t, second.Deduplicated)
	require.Equal(t, first.ID, second.ID)
}

func TestProducerGetJobUnknownIDMatchesErrNotFound(t *testing.T) {
	ctx := ctxT(t)
	c, _ := producer(t)

	_, err := c.GetJob(ctx, uuid.New())
	require.ErrorIs(t, err, client.ErrNotFound)
}

func TestProducerCancelIsIdempotent(t *testing.T) {
	ctx := ctxT(t)
	c, _ := producer(t)

	res, err := c.Enqueue(ctx, client.Job{Queue: "q"})
	require.NoError(t, err)

	first, err := c.Cancel(ctx, res.ID)
	require.NoError(t, err)
	require.Equal(t, "JOB_STATE_CANCELLED", first.State)

	again, err := c.Cancel(ctx, res.ID)
	require.NoError(t, err, "a retried Cancel is not a failure")
	require.Equal(t, "JOB_STATE_CANCELLED", again.State)
}

// TestProducerCancelCompletedMatchesErrTerminal: the one terminal state the
// producer is told about, because it is not the state it asked for.
func TestProducerCancelCompletedMatchesErrTerminal(t *testing.T) {
	ctx := ctxT(t)
	c, raw := producer(t)

	res, err := c.Enqueue(ctx, client.Job{Queue: "q"})
	require.NoError(t, err)
	job := leaseOne(t, ctx, raw, "q", "w", 0)
	_, err = raw.Ack(ctx, &hopperv1.AckRequest{Id: job.GetId(), LeaseId: job.GetLeaseId()})
	require.NoError(t, err)

	_, err = c.Cancel(ctx, res.ID)
	require.ErrorIs(t, err, client.ErrTerminal)
	require.NotErrorIs(t, err, client.ErrNotFound)
}

func TestProducerEnqueueBatchRetryIsIdempotent(t *testing.T) {
	ctx := ctxT(t)
	c, _ := producer(t)

	jobs := []client.Job{
		{Queue: "b", IdempotencyKey: "k1"},
		{Queue: "b", IdempotencyKey: "k2"},
	}
	first, err := c.EnqueueBatch(ctx, jobs)
	require.NoError(t, err)
	require.Len(t, first, 2)

	// The caller cannot tell whether a lost response meant the writes happened,
	// so it retries the whole batch; stable keys make that safe.
	second, err := c.EnqueueBatch(ctx, jobs)
	require.NoError(t, err)
	require.Len(t, second, 2)
	for i := range second {
		require.True(t, second[i].Deduplicated)
		require.Equal(t, first[i].ID, second[i].ID)
	}
	require.Equal(t, 2, jobCount(t, ctx))
}
