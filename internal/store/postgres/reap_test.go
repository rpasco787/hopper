package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// expireLease backdates a lease so the reaper sees it as expired without the
// test having to sleep. Only lease_expires moves; state and lease_id stay, so
// the row is exactly what a crashed worker leaves behind.
func expireLease(t *testing.T, id uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`update jobs set lease_expires = now() - interval '1 second' where id = $1`, id)
	require.NoError(t, err)
}

func TestReapReschedulesExpiredLease(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t).WithRetryPolicy(store.RetryPolicy{Base: 10 * time.Second, Cap: time.Minute})
	j := leaseOne(t, ctx, s, "q", "w-crashed")
	expireLease(t, j.ID)

	before := time.Now()
	n, err := s.ReapExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got := readJob(t, j.ID)
	require.Equal(t, store.StateScheduled, got.State)
	require.Equal(t, int32(1), got.Attempts, "the crashed attempt was already counted at lease time")
	require.NotNil(t, got.LastError)
	require.Equal(t, "lease expired", *got.LastError)
	require.Nil(t, got.FinishedAt)
	require.Equal(t, *j.LeaseID, *got.LeaseID, "lease_id is never cleared")

	// Same window as TestNackReschedulesWithBackoff: 10s * U(0.75, 1.25) ± 1s.
	delay := got.RunAt.Sub(before)
	require.GreaterOrEqual(t, delay, 7500*time.Millisecond-time.Second, "run_at too early: %v", delay)
	require.LessOrEqual(t, delay, 12500*time.Millisecond+time.Second, "run_at too late: %v", delay)

	// The crashed worker's token is now stale on every path.
	require.ErrorIs(t, s.Ack(ctx, j.ID, *j.LeaseID), store.ErrLeaseLost)
	require.ErrorIs(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, time.Minute), store.ErrLeaseLost)
}

func TestReapDeadLettersAtMaxAttempts(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", MaxAttempts: 1})
	require.NoError(t, err)
	got, err := s.Lease(ctx, []string{"q"}, 1, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 1)
	expireLease(t, got[0].ID)

	before := time.Now()
	n, err := s.ReapExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	dead := readJob(t, got[0].ID)
	require.Equal(t, store.StateDead, dead.State)
	require.NotNil(t, dead.FinishedAt)
	require.WithinDuration(t, before, *dead.FinishedAt, 2*time.Second)
	require.Equal(t, "lease expired", *dead.LastError)
	require.Equal(t, int32(1), dead.Attempts)
}

func TestReapIgnoresLiveLeases(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	expired := leaseOne(t, ctx, s, "q", "w")
	live := leaseOne(t, ctx, s, "q", "w")
	expireLease(t, expired.ID)

	n, err := s.ReapExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.Equal(t, store.StateScheduled, readJob(t, expired.ID).State)
	cur := readJob(t, live.ID)
	require.Equal(t, store.StateLeased, cur.State)
	require.Nil(t, cur.LastError)
	require.WithinDuration(t, *live.LeaseExpires, *cur.LeaseExpires, time.Millisecond)
}

func TestReapHonoursLimit(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	enqueueN(t, ctx, s, "q", 5, 0)
	leased, err := s.Lease(ctx, []string{"q"}, 5, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, leased, 5)
	for _, j := range leased {
		expireLease(t, j.ID)
	}

	n, err := s.ReapExpiredLeases(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	var stillLeased int
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) from jobs where state = 'leased'`).Scan(&stillLeased))
	require.Equal(t, 3, stillLeased)

	n, err = s.ReapExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	n, err = s.ReapExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n, "nothing left to reap")

	n, err = s.ReapExpiredLeases(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 0, n, "limit <= 0 is a no-op")
}
