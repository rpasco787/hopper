package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// deadJob enqueues a one-shot job on queue and fails it, returning the dead row.
func deadJob(t *testing.T, ctx context.Context, s *Store, queue string) store.Job {
	t.Helper()
	_, err := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, MaxAttempts: 1})
	require.NoError(t, err)
	got, err := s.Lease(ctx, []string{queue}, 1, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, s.Nack(ctx, got[0].ID, *got[0].LeaseID, "fatal"))
	j := readJob(t, got[0].ID)
	require.Equal(t, store.StateDead, j.State)
	return j
}

func TestListDeadPagesInFinishedOrder(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	want := make(map[uuid.UUID]bool)
	for range 5 {
		want[deadJob(t, ctx, s, "q").ID] = true
	}
	other := deadJob(t, ctx, s, "other") // wrong queue: must not appear
	completed := leaseOne(t, ctx, s, "q", "w")
	require.NoError(t, s.Ack(ctx, completed.ID, *completed.LeaseID)) // wrong state: must not appear

	var (
		seen   = make(map[uuid.UUID]bool)
		cursor *store.DeadCursor
		pages  int
		last   store.Job
	)
	for {
		page, err := s.ListDead(ctx, "q", 2, cursor)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		pages++
		require.LessOrEqual(t, len(page), 2)
		for _, j := range page {
			require.Equal(t, "q", j.Queue)
			require.Equal(t, store.StateDead, j.State)
			require.NotNil(t, j.FinishedAt)
			require.False(t, seen[j.ID], "job %s returned twice", j.ID)
			seen[j.ID] = true
			if pages > 1 || len(seen) > 1 {
				// Strictly after the previous row in (finished_at, id) order.
				require.False(t, j.FinishedAt.Before(*last.FinishedAt))
			}
			last = j
		}
		cursor = &store.DeadCursor{FinishedAt: *last.FinishedAt, ID: last.ID}
	}

	require.Equal(t, 3, pages, "5 rows at 2 per page")
	require.Equal(t, want, seen)
	require.False(t, seen[other.ID])
	require.False(t, seen[completed.ID])
}

func TestListDeadValidation(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	deadJob(t, ctx, s, "q")

	_, err := s.ListDead(ctx, "", 10, nil)
	require.Error(t, err, "queue is required")

	got, err := s.ListDead(ctx, "q", 0, nil)
	require.NoError(t, err)
	require.Nil(t, got, "limit <= 0 returns nothing, not an error")

	got, err = s.ListDead(ctx, "empty", 10, nil)
	require.NoError(t, err)
	require.Nil(t, got, "no dead jobs on that queue")
}
