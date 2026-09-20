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

// pinFinishedAt forces the given rows' finished_at to an exact, identical
// timestamp via raw SQL, following the forceState/expireLease pattern (see
// ack_test.go and reap_test.go): deadJob's Enqueue/Lease/Nack round-trip each
// call its own now(), so a tie can never occur through the public API alone.
func pinFinishedAt(t *testing.T, ids []uuid.UUID, at time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`update jobs set finished_at = $2 where id = any($1)`, ids, at)
	require.NoError(t, err)
}

// TestListDeadTiebreaksOnID proves the id half of the (finished_at, id)
// cursor comparison actually breaks ties. It forces two dead jobs to share
// one exact finished_at, then pages with limit=1 and asserts the lower id
// comes first and the higher id comes second — never repeated, never skipped.
func TestListDeadTiebreaksOnID(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	a := deadJob(t, ctx, s, "q")
	b := deadJob(t, ctx, s, "q")

	tie := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pinFinishedAt(t, []uuid.UUID{a.ID, b.ID}, tie)

	lo, hi := a, b
	if hi.ID.String() < lo.ID.String() {
		lo, hi = hi, lo
	}
	require.NotEqual(t, lo.ID, hi.ID)

	// Sanity check: paging from nil with the same limit twice, without ever
	// advancing the cursor, must deterministically return the same first row.
	first1, err := s.ListDead(ctx, "q", 1, nil)
	require.NoError(t, err)
	require.Len(t, first1, 1)
	first2, err := s.ListDead(ctx, "q", 1, nil)
	require.NoError(t, err)
	require.Len(t, first2, 1)
	require.Equal(t, first1[0].ID, first2[0].ID)
	require.Equal(t, lo.ID, first1[0].ID, "lower id must sort first on a finished_at tie")

	cursor := &store.DeadCursor{FinishedAt: *first1[0].FinishedAt, ID: first1[0].ID}
	second, err := s.ListDead(ctx, "q", 1, cursor)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, hi.ID, second[0].ID, "higher id must be the next row, not repeated or skipped")
	require.NotEqual(t, first1[0].ID, second[0].ID)
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
