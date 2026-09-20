package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// enqueueN enqueues n jobs on queue with the given priority and returns them.
func enqueueN(t *testing.T, ctx context.Context, s *Store, queue string, n int, priority int16) []store.Job {
	t.Helper()
	out := make([]store.Job, 0, n)
	for range n {
		j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, Priority: priority})
		require.NoError(t, err)
		out = append(out, j)
	}
	return out
}

func TestLeaseOne(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	enq, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q"})
	require.NoError(t, err)

	before := time.Now()
	got, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "worker-1")
	require.NoError(t, err)
	require.Len(t, got, 1)

	j := got[0]
	require.Equal(t, enq.ID, j.ID)
	require.Equal(t, store.StateLeased, j.State)
	require.Equal(t, int32(1), j.Attempts, "attempts is counted at lease time")
	require.NotNil(t, j.LeaseID)
	require.NotNil(t, j.LeasedBy)
	require.Equal(t, "worker-1", *j.LeasedBy)
	require.NotNil(t, j.LeasedAt)
	require.NotNil(t, j.LeaseExpires)
	require.WithinDuration(t, before.Add(30*time.Second), *j.LeaseExpires, 2*time.Second)
	require.True(t, j.UpdatedAt.After(enq.UpdatedAt) || j.UpdatedAt.Equal(enq.UpdatedAt))

	// The row is now held; nothing is left to lease.
	again, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "worker-2")
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestLeaseLimitAndQueueFilter(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	enqueueN(t, ctx, s, "a", 5, 0)
	enqueueN(t, ctx, s, "b", 5, 0)

	got, err := s.Lease(ctx, []string{"a"}, 3, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, j := range got {
		require.Equal(t, "a", j.Queue)
		require.Equal(t, store.StateLeased, j.State)
	}

	// 2 left on a, 5 untouched on b.
	rest, err := s.Lease(ctx, []string{"a", "b"}, 100, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, rest, 7)
}

func TestLeaseSkipsScheduled(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)

	got, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "w")
	require.NoError(t, err)
	require.Empty(t, got, "scheduled rows are not leasable until the promoter moves them")
}

func TestLeaseOrdersByPriorityThenRunAt(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	low, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", Priority: 0})
	require.NoError(t, err)
	// Enqueued later but higher priority: must come out first.
	high, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", Priority: 10})
	require.NoError(t, err)
	// Same priority as low, enqueued later: must come after low.
	low2, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", Priority: 0})
	require.NoError(t, err)

	got, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, high.ID, got[0].ID)
	require.Equal(t, low.ID, got[1].ID)
	require.Equal(t, low2.ID, got[2].ID)
}

func TestLeaseArgumentValidation(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	enqueueN(t, ctx, s, "q", 1, 0)

	got, err := s.Lease(ctx, nil, 10, time.Second, "w")
	require.NoError(t, err)
	require.Nil(t, got, "no queues means nothing to lease, not an error")

	got, err = s.Lease(ctx, []string{"q"}, 0, time.Second, "w")
	require.NoError(t, err)
	require.Nil(t, got)

	_, err = s.Lease(ctx, []string{"q"}, 1, 0, "w")
	require.Error(t, err, "a zero lease would expire the instant it was granted")

	// Nothing above should have consumed the job.
	got, err = s.Lease(ctx, []string{"q"}, 1, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 1)
}

// TestLeaseDisjoint pins the exclusivity claim in docs/semantics.md §13:
// concurrent Lease calls never return the same job. 50 goroutines each lease 4
// from a pool of 200 available jobs, all released at once so they genuinely
// overlap on the row locks. Every job must be handed out exactly once.
//
// The race detector cannot see a double lease (that would be a Postgres bug,
// not a Go data race); only the map assertion below can. -race is still worth
// running here because the result collection itself is shared state.
func TestLeaseDisjoint(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	const (
		workers = 50
		perCall = 4
		total   = workers * perCall // 200
	)
	enqueueN(t, ctx, s, "q", total, 0)

	var (
		mu     sync.Mutex
		seen   = make(map[uuid.UUID]int, total)
		errs   = make([]error, workers)
		counts = make([]int, workers)
		wg     sync.WaitGroup
		start  = make(chan struct{})
	)

	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // starting gun: everyone hits Postgres in the same instant
			jobs, err := s.Lease(ctx, []string{"q"}, perCall, 30*time.Second, fmt.Sprintf("w-%d", i))
			if err != nil {
				errs[i] = err // never require.* off the test goroutine
				return
			}
			counts[i] = len(jobs)
			mu.Lock()
			defer mu.Unlock()
			for _, j := range jobs {
				seen[j.ID]++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "worker %d", i)
	}
	for i, n := range counts {
		require.Equalf(t, perCall, n, "worker %d got %d jobs; with 200 available every caller should fill its limit", i, n)
	}
	require.Len(t, seen, total, "every job leased exactly once")
	for id, n := range seen {
		require.Equalf(t, 1, n, "job %s was leased %d times", id, n)
	}

	// And nothing was left behind or double-counted in the database either.
	var leased, available int
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) filter (where state = 'leased'), count(*) filter (where state = 'available') from jobs`,
	).Scan(&leased, &available))
	require.Equal(t, total, leased)
	require.Equal(t, 0, available)
	t.Logf("%d workers leased %d distinct jobs", workers, len(seen))
}
