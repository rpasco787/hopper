package postgres

import (
	"bytes"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func ids(jobs []store.Job) []uuid.UUID {
	out := make([]uuid.UUID, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

// TestListJobsPagesInIDOrder threads the cursor through every page. Two of the
// five rows get random UUIDv4 ids, so their ID order is unrelated to their
// creation order: a query that leaned on created_at, or on insertion order,
// would return them in the wrong sequence.
func TestListJobsPagesInIDOrder(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	jobs := enqueueN(t, ctx, s, "q", 5, 0)
	for _, j := range []store.Job{jobs[1], jobs[3]} {
		_, err := pool.Exec(ctx, `update jobs set id = $1 where id = $2`, uuid.New(), j.ID)
		require.NoError(t, err)
	}
	var want []uuid.UUID
	rows, err := pool.Query(ctx, `select id from jobs order by id`)
	require.NoError(t, err)
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		want = append(want, id)
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.True(t, slices.IsSortedFunc(want, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) }),
		"the database order is byte order, which is what the cursor compares")

	var got []uuid.UUID
	var cursor *uuid.UUID
	pages := 0
	for {
		page, err := s.ListJobs(ctx, "q", nil, 2, cursor)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		pages++
		got = append(got, ids(page)...)
		last := page[len(page)-1].ID
		cursor = &last
	}
	require.Equal(t, want, got, "every row exactly once, in ascending ID order")
	require.Equal(t, 3, pages, "2 + 2 + 1, and the fourth page is empty")
}

// TestListJobsSurvivesTheCursorRowBeingDeleted: the cursor is a bare ID, not a
// reference to a row, so the row it names vanishing between pages (retention,
// Purge, Cancel-then-sweep) costs nothing. An OFFSET would have skipped a row.
func TestListJobsSurvivesTheCursorRowBeingDeleted(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	enqueueN(t, ctx, s, "q", 5, 0)

	first, err := s.ListJobs(ctx, "q", nil, 2, nil)
	require.NoError(t, err)
	require.Len(t, first, 2)
	cursor := first[1].ID

	_, err = pool.Exec(ctx, `delete from jobs where id = $1`, cursor)
	require.NoError(t, err)

	rest, err := s.ListJobs(ctx, "q", nil, 10, &cursor)
	require.NoError(t, err)
	require.Len(t, rest, 3, "the three rows after the deleted cursor, none skipped")
	for _, j := range rest {
		require.Positive(t, bytes.Compare(j.ID[:], cursor[:]))
	}
}

func TestListJobsFiltersByState(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	enqueueN(t, ctx, s, "q", 1, 0) // stays available
	leased := leaseOne(t, ctx, s, "q", "w")
	completed := leaseOne(t, ctx, s, "q", "w")
	require.NoError(t, s.Ack(ctx, completed.ID, *completed.LeaseID))
	// leaseOne enqueues its own job each time; the first call above left one
	// leased, the second one completed, and the loose enqueue is available.

	got, err := s.ListJobs(ctx, "q", []store.State{store.StateLeased, store.StateCompleted}, 10, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []uuid.UUID{leased.ID, completed.ID}, ids(got))

	all, err := s.ListJobs(ctx, "q", nil, 10, nil)
	require.NoError(t, err)
	require.Len(t, all, 3, "no filter means every state")

	// An empty, non-nil slice means the same as nil: the whole set, not nothing.
	all, err = s.ListJobs(ctx, "q", []store.State{}, 10, nil)
	require.NoError(t, err)
	require.Len(t, all, 3)

	// A repeated state must not repeat rows.
	dup, err := s.ListJobs(ctx, "q", []store.State{store.StateLeased, store.StateLeased}, 10, nil)
	require.NoError(t, err)
	require.Len(t, dup, 1)
}

func TestListJobsIsScopedToOneQueue(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	a := enqueueN(t, ctx, s, "a", 2, 0)
	enqueueN(t, ctx, s, "b", 3, 0)

	got, err := s.ListJobs(ctx, "a", nil, 10, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, ids(a), ids(got))

	none, err := s.ListJobs(ctx, "nonexistent", nil, 10, nil)
	require.NoError(t, err)
	require.Empty(t, none)
}

func TestListJobsRejectsEmptyQueueAndZeroLimit(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	enqueueN(t, ctx, s, "q", 1, 0)

	_, err := s.ListJobs(ctx, "", nil, 10, nil)
	require.Error(t, err)

	for _, limit := range []int{0, -1} {
		got, err := s.ListJobs(ctx, "q", nil, limit, nil)
		require.NoError(t, err)
		require.Empty(t, got, "limit %d returns nothing without an error", limit)
	}
}
