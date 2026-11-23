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

// ghost is a synchronous standby that never connects: the name 'ghost' in
// the shared server's synchronous_standby_names. While it is in force, every
// commit of a transaction that has a transaction ID and wrote WAL waits for
// it, as commits on a primary whose standbys have fallen behind do, and a
// wait that is cancelled keeps its commit (docs/semantics.md §11 item 7).
// Reads, and transactions that wrote no WAL, are untouched.
type ghost struct {
	t    *testing.T
	once sync.Once
}

// ghostStandby puts the ghost in force until release or the end of the test.
// Set up rows before calling it: it holds every write, newStore's truncate
// and forceState included. Tests in this package never run in parallel
// (postgres_test.go), so no other test meets it.
func ghostStandby(t *testing.T) *ghost {
	t.Helper()
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, `create table if not exists ghost_probe (at timestamptz not null default now())`)
	require.NoError(t, err)
	g := &ghost{t: t}
	t.Cleanup(g.release)
	_, err = pool.Exec(ctx, `alter system set synchronous_standby_names = 'ghost'`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `select pg_reload_conf()`)
	require.NoError(t, err)
	// The checkpointer applies the reload when it next wakes, and until then
	// commits don't wait. Two waiting probes in a row rule out a slow fsync.
	require.Eventually(t, func() bool {
		return !commitsWithin(300*time.Millisecond) && !commitsWithin(300*time.Millisecond)
	}, 10*time.Second, 50*time.Millisecond, "commits never began waiting for the ghost standby")
	return g
}

// release removes the ghost, and PostgreSQL releases every commit still
// waiting for it. Calling it twice is harmless.
func (g *ghost) release() {
	g.once.Do(func() {
		ctx := context.Background()
		_, err := pool.Exec(ctx, `alter system reset synchronous_standby_names`)
		require.NoError(g.t, err)
		_, err = pool.Exec(ctx, `select pg_reload_conf()`)
		require.NoError(g.t, err)
		require.Eventually(g.t, func() bool { return commitsWithin(2 * time.Second) },
			10*time.Second, 50*time.Millisecond, "commits still wait after the ghost standby was removed")
	})
}

// commitsWithin reports whether a one-row insert commits within d. One that
// doesn't is cancelled at d, and PostgreSQL keeps its commit.
func commitsWithin(d time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, err := pool.Exec(ctx, `insert into ghost_probe default values`)
	return err == nil
}

// stillWaiting runs op under a short deadline and requires that op was still
// waiting when it came: no answer, right or wrong.
func stillWaiting(t *testing.T, msg string, op func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, op(ctx), context.DeadlineExceeded, msg)
}

// answersAtOnce runs op under a deadline any statement meets and a wait for
// the ghost never does, and requires want (nil for success).
func answersAtOnce(t *testing.T, msg string, want error, op func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := op(ctx)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "%s: it waited for the standby", msg)
	if want == nil {
		require.NoError(t, err, msg)
	} else {
		require.ErrorIs(t, err, want, msg)
	}
}

// countNow runs a count without failing the test, so Eventually can call it.
func countNow(sql string, args ...any) int {
	n := -1
	_ = pool.QueryRow(context.Background(), sql, args...).Scan(&n)
	return n
}

// stateNow reads a row's state without failing the test.
func stateNow(id uuid.UUID) store.State {
	var s store.State
	_ = pool.QueryRow(context.Background(), `select state from jobs where id = $1`, id).Scan(&s)
	return s
}

// TestWhatASynchronousCommitWaitsFor pins the PostgreSQL behavior the fix
// rests on (docs/design/cancelled-waits.md). A commit waits for a synchronous
// standby only when its transaction has a transaction ID and wrote WAL. A
// write's does, and when its wait is cancelled the write stays committed and
// becomes visible though no standby has it. A plain read's doesn't, nor does
// an insert that deduplicated against a committed row, nor a transaction ID
// alone. A row lock writes WAL, so its commit waits.
func TestWhatASynchronousCommitWaitsFor(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	key := "probe-key"
	j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "probe", IdempotencyKey: &key})
	require.NoError(t, err)
	ghostStandby(t)

	exec := func(sql string, args ...any) func(context.Context) error {
		return func(ctx context.Context) error {
			_, err := pool.Exec(ctx, sql, args...)
			return err
		}
	}
	stillWaiting(t, "a write's commit waits for the standby",
		exec(`update jobs set priority = 7 where id = $1`, j.ID))
	require.Eventually(t, func() bool {
		return countNow(`select count(*) from jobs where id = $1 and priority = 7`, j.ID) == 1
	}, 5*time.Second, 20*time.Millisecond, "a cancelled wait keeps the commit, and the write becomes visible")
	answersAtOnce(t, "a plain read writes no WAL", nil,
		exec(`select id from jobs where id = $1`, j.ID))
	answersAtOnce(t, "an insert that deduplicated against a committed row writes no WAL", nil,
		exec(`insert into jobs (queue, payload, state, run_at, idempotency_key)
		      values ('probe', '{}', 'available', now(), $1)
		      on conflict (queue, idempotency_key) where idempotency_key is not null do nothing`, key))
	answersAtOnce(t, "a transaction ID alone writes no WAL", nil,
		exec(`select pg_current_xact_id()`))
	stillWaiting(t, "a row lock writes WAL, so its commit waits",
		exec(`select id from jobs where id = $1 for key share`, j.ID))
}
