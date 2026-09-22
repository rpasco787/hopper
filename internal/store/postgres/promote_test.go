package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// scheduleInPast backdates a scheduled row's run_at. Enqueue cannot produce an
// overdue scheduled row — it would have inserted it 'available' — so a test
// that needs one writes it directly, the same way expireLease does for leases.
func scheduleInPast(t *testing.T, id uuid.UUID, ago time.Duration) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`update jobs set run_at = now() - $2::interval where id = $1 and state = 'scheduled'`,
		id, ago)
	require.NoError(t, err)
}

func TestPromoteDueIgnoresFutureJobs(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	require.Equal(t, store.StateScheduled, j.State)

	n, err := s.PromoteDue(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n, "not due for an hour")
	require.Equal(t, store.StateScheduled, readJob(t, j.ID).State)
}

// TestPromoteDueMakesDelayedJobsLeasable is the §14 row: a delayed job is not
// leasable when it is enqueued and is leasable after the promoter has run.
func TestPromoteDueMakesDelayedJobsLeasable(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(200 * time.Millisecond)})
	require.NoError(t, err)
	require.Equal(t, store.StateScheduled, j.State)

	got, err := s.Lease(ctx, []string{"q"}, 10, time.Second, "w")
	require.NoError(t, err)
	require.Empty(t, got, "a scheduled job is not leasable, due or not")

	time.Sleep(300 * time.Millisecond) // let run_at pass on the DB clock

	n, err := s.PromoteDue(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	promoted := readJob(t, j.ID)
	require.Equal(t, store.StateAvailable, promoted.State)
	require.WithinDuration(t, j.RunAt, promoted.RunAt, time.Millisecond, "promotion does not move run_at")
	require.Equal(t, int32(0), promoted.Attempts, "promotion is not a delivery")
	require.Nil(t, promoted.LeaseID)
	require.False(t, promoted.UpdatedAt.Before(j.UpdatedAt))

	got, err = s.Lease(ctx, []string{"q"}, 10, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, j.ID, got[0].ID)
}

// TestPromoteDuePromotesOldestFirst pins the ORDER BY, which is what makes a
// backlog drain in due order instead of physical order. No sleeps: the rows are
// backdated, so their relative overdue-ness is exact.
func TestPromoteDuePromotesOldestFirst(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	// Enqueued least-overdue first, so insertion order is the opposite of the
	// order the promoter must choose.
	var ids []uuid.UUID
	for _, ago := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second} {
		j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		scheduleInPast(t, j.ID, ago)
		ids = append(ids, j.ID)
	}

	n, err := s.PromoteDue(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.Equal(t, store.StateAvailable, readJob(t, ids[2]).State, "3s overdue goes first")
	require.Equal(t, store.StateScheduled, readJob(t, ids[1]).State)
	require.Equal(t, store.StateScheduled, readJob(t, ids[0]).State)
}

func TestPromoteDueHonoursLimit(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	for range 5 {
		j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		scheduleInPast(t, j.ID, time.Second)
	}

	n, err := s.PromoteDue(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	var scheduled int
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) from jobs where state = 'scheduled'`).Scan(&scheduled))
	require.Equal(t, 3, scheduled)

	n, err = s.PromoteDue(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	n, err = s.PromoteDue(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n, "nothing left to promote")

	n, err = s.PromoteDue(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 0, n, "limit <= 0 is a no-op")
}
