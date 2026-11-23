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

// TestARetriedEnqueueWaitsForTheStandby is docs/semantics.md §11 item 7 in
// one process. An Enqueue's commit waits for a standby that has fallen
// behind. Its deadline cancels the wait, and PostgreSQL keeps the commit, so
// the row is on this server alone. The producer's retry under the same key
// finds it. It must not be answered deduplicated until a standby has the row,
// or a crash now loses a job its producer was told exists.
func TestARetriedEnqueueWaitsForTheStandby(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	key := "in-doubt"
	p := store.EnqueueParams{Queue: "q", IdempotencyKey: &key}
	enqueue := func(ctx context.Context) error {
		_, _, err := s.Enqueue(ctx, p)
		return err
	}
	g := ghostStandby(t)

	stillWaiting(t, "the first call's commit waits for the standby", enqueue)
	require.Eventually(t, func() bool {
		return countNow(`select count(*) from jobs where idempotency_key = $1`, key) == 1
	}, 5*time.Second, 20*time.Millisecond, "the cancelled wait left the row visible on the primary")
	stillWaiting(t, "the retry was answered about a row no standby has", enqueue)

	g.release()
	_, inserted, err := s.Enqueue(ctx, p)
	require.NoError(t, err)
	require.False(t, inserted, "once a standby has the row, the retry finds the first call's job")
	require.Equal(t, 1, countNow(`select count(*) from jobs where idempotency_key = $1`, key))
}

// TestARetriedAckWaitsForTheStandby: an Ack's commit waits, its deadline
// cancels the wait, and the row is completed on the primary alone. The
// worker's retry must not be answered "already applied" until a standby has
// the completion, or a crash runs the acked job again.
func TestARetriedAckWaitsForTheStandby(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")
	ack := func(ctx context.Context) error { return s.Ack(ctx, j.ID, *j.LeaseID) }
	g := ghostStandby(t)

	stillWaiting(t, "the first Ack's commit waits for the standby", ack)
	require.Eventually(t, func() bool { return stateNow(j.ID) == store.StateCompleted },
		5*time.Second, 20*time.Millisecond, "the cancelled wait left the row completed on the primary")
	stillWaiting(t, "the retried Ack was answered as already applied about a completion no standby has", ack)

	g.release()
	require.NoError(t, ack(ctx), "once a standby has it, the retry is the idempotent re-ack")
}

// TestARetriedNackWaitsForTheStandby is the same for a Nack, whose
// already-applied case is the row scheduled under the same lease.
func TestARetriedNackWaitsForTheStandby(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")
	nack := func(ctx context.Context) error { return s.Nack(ctx, j.ID, *j.LeaseID, "boom") }
	g := ghostStandby(t)

	stillWaiting(t, "the first Nack's commit waits for the standby", nack)
	require.Eventually(t, func() bool { return stateNow(j.ID) == store.StateScheduled },
		5*time.Second, 20*time.Millisecond, "the cancelled wait left the row scheduled on the primary")
	stillWaiting(t, "the retried Nack was answered as already applied about a retry no standby has", nack)

	g.release()
	require.NoError(t, nack(ctx), "once a standby has it, the retry is the idempotent re-nack")
}

// TestARetriedCancelWaitsForTheStandby: a Cancel's commit waits, its deadline
// cancels the wait, and the job is cancelled on the primary alone. The
// broker answers a retry that finds it cancelled as success
// (already_cancelled), so the store must not report it until a standby has
// the cancellation, or a crash runs a job its client was told was called off.
func TestARetriedCancelWaitsForTheStandby(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q"})
	require.NoError(t, err)
	cancelJob := func(ctx context.Context) error {
		_, err := s.Cancel(ctx, j.ID)
		return err
	}
	g := ghostStandby(t)

	stillWaiting(t, "the first Cancel's commit waits for the standby", cancelJob)
	require.Eventually(t, func() bool { return stateNow(j.ID) == store.StateCancelled },
		5*time.Second, 20*time.Millisecond, "the cancelled wait left the job cancelled on the primary")
	stillWaiting(t, "the retried Cancel was answered already cancelled about a cancellation no standby has", cancelJob)

	g.release()
	_, err = s.Cancel(ctx, j.ID)
	var terminal *store.TerminalStateError
	require.ErrorAs(t, err, &terminal)
	require.Equal(t, store.StateCancelled, terminal.State)
}
