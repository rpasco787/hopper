package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestNackReschedulesWithBackoff(t *testing.T) {
	ctx := ctxT(t)
	// base 10s so the jitter window (7.5s..12.5s) is wide enough to assert on
	// without a fake clock.
	s := newStore(t).WithRetryPolicy(store.RetryPolicy{Base: 10 * time.Second, Cap: time.Minute})
	j := leaseOne(t, ctx, s, "q", "w")

	before := time.Now()
	require.NoError(t, s.Nack(ctx, j.ID, *j.LeaseID, "boom"))

	got := readJob(t, j.ID)
	require.Equal(t, store.StateScheduled, got.State)
	require.Equal(t, int32(1), got.Attempts, "attempts was counted at lease time and does not move on nack")
	require.NotNil(t, got.LastError)
	require.Equal(t, "boom", *got.LastError)
	require.Nil(t, got.FinishedAt)
	require.Equal(t, *j.LeaseID, *got.LeaseID, "lease_id survives so a retried nack is recognised")

	delay := got.RunAt.Sub(before)
	// 10s * U(0.75, 1.25), plus a second of slack for clock drift between the
	// test and the container.
	require.GreaterOrEqual(t, delay, 7500*time.Millisecond-time.Second, "run_at too early: %v", delay)
	require.LessOrEqual(t, delay, 12500*time.Millisecond+time.Second, "run_at too late: %v", delay)

	// A scheduled row is not leasable.
	again, err := s.Lease(ctx, []string{"q"}, 1, time.Second, "w2")
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestNackDeadLettersAtMaxAttempts(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", MaxAttempts: 1})
	require.NoError(t, err)
	got, err := s.Lease(ctx, []string{"q"}, 1, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 1)
	j := got[0]

	before := time.Now()
	require.NoError(t, s.Nack(ctx, j.ID, *j.LeaseID, "fatal"))

	dead := readJob(t, j.ID)
	require.Equal(t, store.StateDead, dead.State)
	require.NotNil(t, dead.FinishedAt)
	require.WithinDuration(t, before, *dead.FinishedAt, 2*time.Second)
	require.NotNil(t, dead.LastError)
	require.Equal(t, "fatal", *dead.LastError)
	require.Equal(t, int32(1), dead.Attempts)
}

func TestNackIsIdempotentWithSameLease(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t).WithRetryPolicy(store.RetryPolicy{Base: 10 * time.Second, Cap: time.Minute})
	j := leaseOne(t, ctx, s, "q", "w")

	require.NoError(t, s.Nack(ctx, j.ID, *j.LeaseID, "first"))
	first := readJob(t, j.ID)

	require.NoError(t, s.Nack(ctx, j.ID, *j.LeaseID, "second"), "a retried nack is a no-op, not an error")
	second := readJob(t, j.ID)

	require.Equal(t, first.RunAt.UnixMicro(), second.RunAt.UnixMicro(), "re-nack must not push run_at again")
	require.Equal(t, "first", *second.LastError, "re-nack must not overwrite the recorded error")
	require.Equal(t, store.StateScheduled, second.State)
}

func TestNackIsIdempotentOnDeadRow(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", MaxAttempts: 1})
	require.NoError(t, err)
	got, err := s.Lease(ctx, []string{"q"}, 1, time.Second, "w")
	require.NoError(t, err)
	j := got[0]

	require.NoError(t, s.Nack(ctx, j.ID, *j.LeaseID, "fatal"))
	require.NoError(t, s.Nack(ctx, j.ID, *j.LeaseID, "fatal again"))
	require.Equal(t, store.StateDead, readJob(t, j.ID).State)
}

func TestNackMissClassification(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")

	forceState(t, j.ID, store.StateCancelled)
	require.ErrorIs(t, s.Nack(ctx, j.ID, *j.LeaseID, "x"), store.ErrCancelled)

	require.ErrorIs(t, s.Nack(ctx, uuid.New(), *j.LeaseID, "x"), store.ErrNotFound)
}
