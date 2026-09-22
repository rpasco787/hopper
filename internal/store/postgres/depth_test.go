package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestQueueDepthsCountsEachState(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	deadJob(t, ctx, s, "a")
	leased := leaseOne(t, ctx, s, "a", "w")
	require.NotNil(t, leased.LeaseID)
	enqueueN(t, ctx, s, "a", 2, 0)
	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "a", RunAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	enqueueN(t, ctx, s, "b", 1, 0)
	completed := leaseOne(t, ctx, s, "completed", "w")
	require.NoError(t, s.Ack(ctx, completed.ID, *completed.LeaseID))
	cancelled, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "cancelled"})
	require.NoError(t, err)
	_, err = s.Cancel(ctx, cancelled.ID)
	require.NoError(t, err)

	got, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []store.QueueDepth{
		{Queue: "a", State: store.StateAvailable, Count: 2},
		{Queue: "a", State: store.StateScheduled, Count: 1},
		{Queue: "a", State: store.StateLeased, Count: 1},
		{Queue: "a", State: store.StateDead, Count: 1},
		{Queue: "b", State: store.StateAvailable, Count: 1},
	}, got)
}

func TestQueueDepthsOmitsEmptyQueues(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "empty", "w")
	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))

	got, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestQueueDepthsEmptyTable(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	got, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	require.Empty(t, got)
}
