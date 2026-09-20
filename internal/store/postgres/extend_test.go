package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestExtendLeasePushesExpiry(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w") // 30s lease

	before := time.Now()
	require.NoError(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, 5*time.Minute))

	got := readJob(t, j.ID)
	require.Equal(t, store.StateLeased, got.State)
	require.NotNil(t, got.LeaseExpires)
	require.WithinDuration(t, before.Add(5*time.Minute), *got.LeaseExpires, 2*time.Second)
	require.Equal(t, *j.LeaseID, *got.LeaseID, "a heartbeat keeps the same lease token")
	require.Equal(t, j.Attempts, got.Attempts, "a heartbeat is not a new delivery")
	require.False(t, got.UpdatedAt.Before(j.UpdatedAt))
}

func TestExtendLeaseRejectsNonPositiveDuration(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")

	require.Error(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, 0))
	require.Error(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, -time.Second))

	got := readJob(t, j.ID)
	require.WithinDuration(t, *j.LeaseExpires, *got.LeaseExpires, time.Millisecond, "rejected call must not touch the row")
}

// TestExtendLeaseMissClassification walks the docs/semantics.md §5 table for
// the heartbeat path. Unlike Ack and Nack there is no "already applied" row:
// every miss is a lost lease or a cancellation.
func TestExtendLeaseMissClassification(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	// Reaper applied backoff: scheduled, lease_id still ours.
	j := leaseOne(t, ctx, s, "q", "w")
	forceState(t, j.ID, store.StateScheduled)
	require.ErrorIs(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, time.Minute), store.ErrLeaseLost)

	// Already acked under our lease: the job is finished, the lease is gone.
	j = leaseOne(t, ctx, s, "q2", "w")
	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))
	require.ErrorIs(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, time.Minute), store.ErrLeaseLost)

	// Cancelled while leased.
	j = leaseOne(t, ctx, s, "q3", "w")
	forceState(t, j.ID, store.StateCancelled)
	require.ErrorIs(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, time.Minute), store.ErrCancelled)

	// No such job.
	require.ErrorIs(t, s.ExtendLease(ctx, uuid.New(), uuid.New(), time.Minute), store.ErrNotFound)
}
