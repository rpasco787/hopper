package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func finishedAgo(t *testing.T, id uuid.UUID, ago time.Duration) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`update jobs set finished_at = now() - $2::interval where id = $1`, id, ago)
	require.NoError(t, err)
}

func completedJob(t *testing.T, ctx context.Context, s *Store, queue string) uuid.UUID {
	t.Helper()
	j := leaseOne(t, ctx, s, queue, "w")
	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))
	return j.ID
}

func TestDeleteFinishedRemovesOldCompletedAndCancelled(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	oldDone := completedJob(t, ctx, s, "q")
	finishedAgo(t, oldDone, 48*time.Hour)

	toCancel, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q"})
	require.NoError(t, err)
	_, err = s.Cancel(ctx, toCancel.ID)
	require.NoError(t, err)
	finishedAgo(t, toCancel.ID, 48*time.Hour)

	recent := completedJob(t, ctx, s, "q")
	oldDead := deadJob(t, ctx, s, "q").ID
	finishedAgo(t, oldDead, 48*time.Hour)
	live, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q"})
	require.NoError(t, err)

	n, err := s.DeleteFinished(ctx, time.Now().Add(-24*time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	require.False(t, rowExists(t, oldDone))
	require.False(t, rowExists(t, toCancel.ID))
	require.True(t, rowExists(t, recent), "inside the window")
	require.True(t, rowExists(t, oldDead), "dead rows are Purge's, never the janitor's")
	require.True(t, rowExists(t, live.ID), "an unfinished job has no finished_at to compare")
}

func TestDeleteFinishedHonoursLimitOldestFirst(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	ids := make([]uuid.UUID, 3)
	for i, ago := range []time.Duration{72 * time.Hour, 48 * time.Hour, 36 * time.Hour} {
		ids[i] = completedJob(t, ctx, s, "q")
		finishedAgo(t, ids[i], ago)
	}
	cutoff := time.Now().Add(-time.Hour)

	n, err := s.DeleteFinished(ctx, cutoff, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.False(t, rowExists(t, ids[0]), "the oldest goes first")
	require.True(t, rowExists(t, ids[1]))
	require.True(t, rowExists(t, ids[2]))

	n, err = s.DeleteFinished(ctx, cutoff, 100)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	n, err = s.DeleteFinished(ctx, cutoff, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n, "nothing left inside the cutoff")

	n, err = s.DeleteFinished(ctx, time.Now(), 0)
	require.NoError(t, err)
	require.Equal(t, 0, n, "limit <= 0 is a no-op")
}

func TestRetentionEndsIdempotencyProtection(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	key := "nightly-report"
	first, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.NoError(t, err)
	require.True(t, inserted)
	leased, err := s.Lease(ctx, []string{"q"}, 1, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.NoError(t, s.Ack(ctx, leased[0].ID, *leased[0].LeaseID))

	dup, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.NoError(t, err)
	require.False(t, inserted, "still protected while the completed row exists")
	require.Equal(t, first.ID, dup.ID)

	finishedAgo(t, first.ID, 48*time.Hour)
	n, err := s.DeleteFinished(ctx, time.Now().Add(-24*time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	again, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.NoError(t, err)
	require.True(t, inserted, "retention released the key")
	require.NotEqual(t, first.ID, again.ID)
}
