package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// testLeaderTTL is long enough that no test holder lapses unless it means to.
const testLeaderTTL = time.Minute

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

	lead1, err := s1.TryLeadership(ctx, testLeaderTTL)
	require.NoError(t, err)

	_, err = s2.TryLeadership(ctx, testLeaderTTL)
	require.ErrorIs(t, err, store.ErrNotLeader)

	require.NoError(t, lead1.Release(ctx))

	lead2, err := s2.TryLeadership(ctx, testLeaderTTL)
	require.NoError(t, err)
	require.NoError(t, lead2.Release(ctx))
}

func TestLeadershipIsRealInPgLocks(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	require.Zero(t, advisoryLockCount(t, ctx, schedulerLockKey))

	lead, err := s.TryLeadership(ctx, testLeaderTTL)
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

	lead, err := s.TryLeadership(ctx, testLeaderTTL)
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
		lead2, err := s.TryLeadership(ctx, testLeaderTTL)
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
		lead, err := s.TryLeadership(ctx, testLeaderTTL)
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

	lead, err := s.TryLeadership(ctx, testLeaderTTL)
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

	lead, err := s.TryLeadership(ctx, testLeaderTTL)
	require.NoError(t, err)
	require.NoError(t, lead.Release(ctx))
	require.NoError(t, lead.Release(ctx))
}

// TestLeadershipLapsesWhenTheHolderGoesSilent is the server half of the
// silent-leader fix: a holder that runs nothing on its session for ttl loses
// the role without doing anything, as a paused or partitioned process would.
func TestLeadershipLapsesWhenTheHolderGoesSilent(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	const ttl = 500 * time.Millisecond

	start := time.Now()
	lead, err := s.TryLeadership(ctx, ttl)
	require.NoError(t, err)
	t.Cleanup(func() { lead.Release(context.Background()) }) //nolint:errcheck // the session is gone; Release discards it

	var lapsed time.Duration
	require.Eventually(t, func() bool {
		lead2, err := s.TryLeadership(ctx, testLeaderTTL)
		if err != nil {
			return false
		}
		lapsed = time.Since(start)
		t.Cleanup(func() { lead2.Release(context.Background()) }) //nolint:errcheck // best-effort cleanup
		return true
	}, 5*time.Second, 20*time.Millisecond, "the silent holder's session must end and free the role")
	require.GreaterOrEqual(t, lapsed, ttl, "the role moved before the holder had been silent for ttl")
	require.Error(t, lead.Check(ctx), "the silent holder must find its session gone")
}

// TestACheckingHolderKeepsTheRole: Check runs a statement on the session, so
// a holder that checks well within ttl is never idle long enough to lose it.
func TestACheckingHolderKeepsTheRole(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	const ttl = 500 * time.Millisecond

	lead, err := s.TryLeadership(ctx, ttl)
	require.NoError(t, err)
	defer lead.Release(context.Background()) //nolint:errcheck // best-effort cleanup

	for end := time.Now().Add(4 * ttl); time.Now().Before(end); time.Sleep(ttl / 5) {
		require.NoError(t, lead.Check(ctx))
	}
	require.Equal(t, 1, advisoryLockCount(t, ctx, schedulerLockKey))
	_, err = New(pool).TryLeadership(ctx, testLeaderTTL)
	require.ErrorIs(t, err, store.ErrNotLeader)
}

// TestLeadershipSetsAndReleaseResetsTheIdleTimeout: the timeout is on the
// leader's session only, and never reaches the pool.
func TestLeadershipSetsAndReleaseResetsTheIdleTimeout(t *testing.T) {
	ctx := ctxT(t)
	newStore(t) // truncates
	cfg := pool.Config().Copy()
	cfg.MaxConns = 1 // one session, so the query after Release runs on the leader's own
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(one.Close)

	lead, err := New(one).TryLeadership(ctx, 1500*time.Millisecond)
	require.NoError(t, err)
	var during string
	require.NoError(t, lead.(*leadership).conn.QueryRow(ctx, `show idle_session_timeout`).Scan(&during))
	require.Equal(t, "1500ms", during)
	require.NoError(t, lead.Release(ctx))

	var after string
	require.NoError(t, one.QueryRow(ctx, `show idle_session_timeout`).Scan(&after))
	require.Equal(t, "0", after, "Release must reset the timeout before the session goes back to the pool")
}

func TestTryLeadershipRejectsATTLUnderAMillisecond(t *testing.T) {
	ctx := ctxT(t)
	_, err := newStore(t).TryLeadership(ctx, 999*time.Microsecond)
	require.ErrorContains(t, err, "under a millisecond")
	require.Zero(t, advisoryLockCount(t, ctx, schedulerLockKey))
}
