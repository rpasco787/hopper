package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// leaseOne enqueues a single job on queue and leases it to worker, returning
// the leased row (so callers have a live lease_id).
func leaseOne(t *testing.T, ctx context.Context, s *Store, queue, worker string) store.Job {
	t.Helper()
	_, err := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	require.NoError(t, err)
	got, err := s.Lease(ctx, []string{queue}, 1, 30*time.Second, worker)
	require.NoError(t, err)
	require.Len(t, got, 1)
	return got[0]
}

// forceState rewrites a row's state with raw SQL. It stands in for Cancel and
// the promoter, which do not exist until Session 3, and lets a test put a row
// in a state without going through the lifecycle. Production code must never
// bypass the lease guard this way.
func forceState(t *testing.T, id uuid.UUID, state store.State) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`update jobs set state = $2, updated_at = now() where id = $1`, id, state)
	require.NoError(t, err)
}

func TestAckCompletes(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")

	before := time.Now()
	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))

	got := readJob(t, j.ID)
	require.Equal(t, store.StateCompleted, got.State)
	require.NotNil(t, got.FinishedAt)
	require.WithinDuration(t, before, *got.FinishedAt, 2*time.Second)
	require.NotNil(t, got.LeaseID)
	require.Equal(t, *j.LeaseID, *got.LeaseID, "lease_id is never cleared; it is what makes re-ack detectable")
	require.False(t, got.UpdatedAt.Before(j.UpdatedAt))
}

func TestAckIsIdempotentWithSameLease(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")

	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))
	first := readJob(t, j.ID)

	// The SDK retries an Ack whose response was lost; that must be a no-op.
	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))
	second := readJob(t, j.ID)
	require.Equal(t, first.FinishedAt.UnixMicro(), second.FinishedAt.UnixMicro(),
		"a re-ack must not rewrite the row")
}

// TestStaleLeaseRejected is the docs/semantics.md §13 row: a worker whose
// lease expired and was re-granted to someone else can neither ack nor nack
// the job. The reaper is simulated with raw SQL.
func TestStaleLeaseRejected(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	old := leaseOne(t, ctx, s, "q", "w-old")

	// Reaper takes it back...
	forceState(t, old.ID, store.StateAvailable)
	// ...and another worker picks it up with a fresh lease_id.
	got, err := s.Lease(ctx, []string{"q"}, 1, 30*time.Second, "w-new")
	require.NoError(t, err)
	require.Len(t, got, 1)
	fresh := got[0]
	require.Equal(t, old.ID, fresh.ID)
	require.NotEqual(t, *old.LeaseID, *fresh.LeaseID)
	require.Equal(t, int32(2), fresh.Attempts)

	// The stale token is rejected on all three paths.
	require.ErrorIs(t, s.Ack(ctx, old.ID, *old.LeaseID), store.ErrLeaseLost)
	require.ErrorIs(t, s.Nack(ctx, old.ID, *old.LeaseID, "too late"), store.ErrLeaseLost)
	require.ErrorIs(t, s.ExtendLease(ctx, old.ID, *old.LeaseID, time.Minute), store.ErrLeaseLost)

	// And neither stale call disturbed the live lease.
	cur := readJob(t, old.ID)
	require.Equal(t, store.StateLeased, cur.State)
	require.Equal(t, *fresh.LeaseID, *cur.LeaseID)
	require.WithinDuration(t, *fresh.LeaseExpires, *cur.LeaseExpires, time.Millisecond, "stale heartbeat must not extend the live lease")
	require.Nil(t, cur.LastError)

	// The holder of the fresh lease can still finish the job.
	require.NoError(t, s.Ack(ctx, fresh.ID, *fresh.LeaseID))
	require.Equal(t, store.StateCompleted, readJob(t, old.ID).State)
}

func TestAckAfterReaperIsLeaseLost(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")

	// Reaper applied backoff: the row is scheduled, lease_id still ours.
	forceState(t, j.ID, store.StateScheduled)
	require.ErrorIs(t, s.Ack(ctx, j.ID, *j.LeaseID), store.ErrLeaseLost)
	require.Equal(t, store.StateScheduled, readJob(t, j.ID).State)
}

func TestAckOnCancelledIsErrCancelled(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")

	forceState(t, j.ID, store.StateCancelled)
	require.ErrorIs(t, s.Ack(ctx, j.ID, *j.LeaseID), store.ErrCancelled)
}

func TestAckUnknownIDIsNotFound(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	require.ErrorIs(t, s.Ack(ctx, uuid.New(), uuid.New()), store.ErrNotFound)
}
