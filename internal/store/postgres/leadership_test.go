package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// advisoryLockCount is the independent check from docs/plans decision 2: it
// reads pg_locks directly rather than trusting the cheap `select 1` that
// Check itself performs, so it can verify a Check pass or fail is honest.
func advisoryLockCount(t *testing.T, ctx context.Context, key int64) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `
		select count(*) from pg_locks
		where locktype = 'advisory' and granted
		  and (classid::bigint << 32) | objid::bigint = $1`,
		key).Scan(&n))
	return n
}

func TestOneLeaderAtATime(t *testing.T) {
	ctx := ctxT(t)
	s1 := newStore(t)
	s2 := New(pool)

	lead1, err := s1.TryLeadership(ctx)
	require.NoError(t, err)

	_, err = s2.TryLeadership(ctx)
	require.ErrorIs(t, err, store.ErrNotLeader)

	require.NoError(t, lead1.Release(ctx))

	lead2, err := s2.TryLeadership(ctx)
	require.NoError(t, err)
	require.NoError(t, lead2.Release(ctx))
}

func TestLeadershipIsRealInPgLocks(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	require.Zero(t, advisoryLockCount(t, ctx, schedulerLockKey))

	lead, err := s.TryLeadership(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, advisoryLockCount(t, ctx, schedulerLockKey))

	require.NoError(t, lead.Release(ctx))
	require.Zero(t, advisoryLockCount(t, ctx, schedulerLockKey))
}

// TestCheckFailsAfterTheSessionDies kills the leader's own backend from a
// second connection, which is what an ungraceful death of the leading process
// looks like to PostgreSQL. It finds the pid from pg_locks rather than
// exposing a test-only accessor on Leadership.
func TestCheckFailsAfterTheSessionDies(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	lead, err := s.TryLeadership(ctx)
	require.NoError(t, err)
	// Once the backend is killed below, lead's connection is permanently
	// broken. Release's error path hijacks and closes it (the discard rule
	// from Decision 1), which is what tells the pool to stop waiting on it;
	// without this, pool.Close() in TestMain blocks forever on a connection
	// that was checked out and never returned.
	t.Cleanup(func() { lead.Release(context.Background()) }) //nolint:errcheck // best-effort cleanup

	var pid int32
	require.NoError(t, pool.QueryRow(ctx, `
		select pid from pg_locks
		where locktype = 'advisory' and granted
		  and (classid::bigint << 32) | objid::bigint = $1`,
		schedulerLockKey).Scan(&pid))

	_, err = pool.Exec(ctx, `select pg_terminate_backend($1)`, pid)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return lead.Check(ctx) != nil
	}, 5*time.Second, 50*time.Millisecond, "Check must observe the terminated session")

	require.Eventually(t, func() bool {
		lead2, err := s.TryLeadership(ctx)
		if err != nil {
			return false
		}
		t.Cleanup(func() { lead2.Release(context.Background()) })
		return true
	}, 5*time.Second, 50*time.Millisecond, "a second contender must win once the dead session is gone")
}

func TestReleaseDoesNotPoisonThePool(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	for range 20 {
		lead, err := s.TryLeadership(ctx)
		require.NoError(t, err)
		require.NoError(t, lead.Release(ctx))
	}

	_, err := s.Now(ctx)
	require.NoError(t, err)
	require.Zero(t, advisoryLockCount(t, ctx, schedulerLockKey))
}

// TestLeadershipSurvivesPoolPressure proves the held connection is not
// recycled under load. cfg.MaxConns is 60 (postgres_test.go); leadership
// pins one, so this cycles the other 59 well past once each.
func TestLeadershipSurvivesPoolPressure(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	lead, err := s.TryLeadership(ctx)
	require.NoError(t, err)
	defer lead.Release(context.Background()) //nolint:errcheck // best-effort cleanup

	const n = 120
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Now(ctx)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "query %d failed", i)
	}

	require.NoError(t, lead.Check(ctx))
}

func TestDoubleReleaseIsSafe(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	lead, err := s.TryLeadership(ctx)
	require.NoError(t, err)
	require.NoError(t, lead.Release(ctx))
	require.NoError(t, lead.Release(ctx))
}
