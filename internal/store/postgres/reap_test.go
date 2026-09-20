package postgres

import (
	"context"
	"sync"
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

// TestReapRacesNackWithoutDoubleProcessing pins the doc comment's claim in
// reap.go: the reaper's SELECT ... FOR UPDATE SKIP LOCKED and Nack's SELECT
// ... FOR UPDATE can run concurrently on overlapping rows without either
// blocking forever or double-applying a failure. It follows TestLeaseDisjoint's
// pattern (goroutines released together via a start channel, no require.* off
// the test goroutine, results collected under a mutex and asserted after
// wg.Wait()) rather than reasoning about the race in prose.
//
// Every job gets its lease expired and is then contended by two things at
// once: half are Nacked directly by their original lease_id (expireLease only
// backdates lease_expires, so lease_id and state are untouched, exactly what a
// crashed worker leaves behind), and several reaper goroutines hammer
// ReapExpiredLeases concurrently, sweeping up whatever Nack hasn't claimed
// yet -- including, for the other half, the only path that will ever fail
// them. If SKIP LOCKED were dropped from the reaper's query, this test would
// deadlock or time out against ctxT's 30s budget instead of finishing; if a
// row were ever double-processed, one of the assertions below would fail.
func TestReapRacesNackWithoutDoubleProcessing(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	const n = 20
	enqueueN(t, ctx, s, "q", n, 0)
	leased, err := s.Lease(ctx, []string{"q"}, n, 30*time.Second, "w-crashed")
	require.NoError(t, err)
	require.Len(t, leased, n)
	for _, j := range leased {
		expireLease(t, j.ID)
	}

	const reapers = 3
	var (
		mu       sync.Mutex
		nackErrs []error
		reapErrs []error
		wg       sync.WaitGroup
		start    = make(chan struct{})
	)

	// Nack the even-indexed half directly, using each job's original
	// lease_id -- exactly what a worker that is still alive, but whose lease
	// has since lapsed, would do.
	for i, j := range leased {
		if i%2 != 0 {
			continue
		}
		wg.Add(1)
		go func(j store.Job) {
			defer wg.Done()
			<-start // starting gun: everyone hits Postgres in the same instant
			err := s.Nack(ctx, j.ID, *j.LeaseID, "explicit nack")
			mu.Lock()
			nackErrs = append(nackErrs, err)
			mu.Unlock()
		}(j)
	}

	// Several reapers hammer ReapExpiredLeases concurrently with the Nacks
	// above, racing for the same rows, until each sees nothing left to take.
	for range reapers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				got, err := s.ReapExpiredLeases(ctx, 3)
				if err != nil {
					mu.Lock()
					reapErrs = append(reapErrs, err)
					mu.Unlock()
					return
				}
				if got == 0 {
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()

	for i, err := range nackErrs {
		require.NoErrorf(t, err, "nack %d", i)
	}
	for i, err := range reapErrs {
		require.NoErrorf(t, err, "reaper %d", i)
	}

	// The reapers above stop as soon as ReapExpiredLeases returns 0, which
	// can happen while a Nack it raced against is still mid-transaction on a
	// row the reaper skipped. One deterministic sweep after every goroutine
	// has finished cleans up anything left expired and leased; a
	// double-processed row would surface in the per-job assertions below,
	// not here.
	_, err = s.ReapExpiredLeases(ctx, n)
	require.NoError(t, err)

	for i, j := range leased {
		got := readJob(t, j.ID)
		require.NotEqualf(t, store.StateLeased, got.State, "job %d (%s) is still leased", i, j.ID)
		require.Containsf(t, []store.State{store.StateScheduled, store.StateDead}, got.State,
			"job %d (%s) ended up in unexpected state %v", i, j.ID, got.State)
		require.NotNilf(t, got.LastError, "job %d (%s) has no last_error", i, j.ID)
		require.Containsf(t, []string{"explicit nack", leaseExpiredError}, *got.LastError,
			"job %d (%s) has unexpected last_error %q", i, j.ID, *got.LastError)
	}
}
