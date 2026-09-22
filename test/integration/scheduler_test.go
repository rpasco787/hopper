package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/scheduler"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

// schedulerLockKey mirrors the unexported constant in
// internal/store/postgres/leadership.go. The tests find the leader through the
// lock itself rather than a production test hook; if the two ever drift, the
// leader query below finds no row and the test fails loudly rather than
// passing vacuously.
const schedulerLockKey int64 = 7_243_015_119_421_078

// leaderQuery names the process holding the scheduler role, by joining the
// lock to the session that holds it.
const leaderQuery = `
	select a.application_name, l.pid
	from pg_locks l join pg_stat_activity a on a.pid = l.pid
	where l.locktype = 'advisory' and l.granted
	  and (l.classid::bigint << 32) | l.objid::bigint = $1`

// lockHolder is one row of leaderQuery.
type lockHolder struct {
	Name string `db:"application_name"`
	PID  int32  `db:"pid"`
}

// lockHolders returns every session holding the scheduler lock. It returns
// errors rather than calling require because require.Eventually runs its
// condition on another goroutine, where FailNow must not be called.
func lockHolders(ctx context.Context) ([]lockHolder, error) {
	rows, err := pool.Query(ctx, leaderQuery, schedulerLockKey)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[lockHolder])
}

// heldBy reports whether exactly one session holds the lock and its
// application name is one of names.
func heldBy(ctx context.Context, names []string) bool {
	holders, err := lockHolders(ctx)
	return err == nil && len(holders) == 1 && slices.Contains(names, holders[0].Name)
}

// requireLeader returns the single holder of the scheduler lock. More than
// one holder is impossible for a real advisory lock and fails the test.
func requireLeader(t *testing.T, ctx context.Context) lockHolder {
	t.Helper()
	holders, err := lockHolders(ctx)
	require.NoError(t, err)
	require.Len(t, holders, 1, "exactly one session must hold the scheduler lock")
	return holders[0]
}

// identifiedPool is a pool whose every session reports name as its
// application_name, so the lock's holder can be attributed to one scheduler.
// MaxConns 4: one connection is spent on the leadership term, the rest serve
// the term's queries.
func identifiedPool(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["application_name"] = name
	cfg.MaxConns = 4
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

// schedulerRun is one scheduler goroutine with its own cancel.
type schedulerRun struct {
	name   string
	cancel context.CancelFunc
	done   chan error
}

// schedulerInterval is every test scheduler's leadership-retry and tick
// period. awaitTakeover's bound is expressed in it.
const schedulerInterval = 50 * time.Millisecond

func startScheduler(name string, st store.Store) *schedulerRun {
	ctx, cancel := context.WithCancel(context.Background())
	r := &schedulerRun{name: name, cancel: cancel, done: make(chan error, 1)}
	s := &scheduler.Scheduler{
		Store:    st,
		Interval: schedulerInterval,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	go func() { r.done <- s.Run(ctx) }()
	return r
}

// stop cancels the scheduler and joins it. Run returns only on cancellation,
// so anything other than context.Canceled is a failure.
func (r *schedulerRun) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		require.ErrorIs(t, err, context.Canceled, "scheduler %s", r.name)
	case <-time.After(10 * time.Second):
		t.Fatalf("scheduler %s did not stop", r.name)
	}
}

// failoverBudget bounds how long an ungraceful death may leave the role
// vacant, measured in occurrences of the one-second schedule. Forty rounds
// under -race (2026-09-22, Docker Desktop on macOS) showed no gap at all: the
// pending tick fired 0.4-1.0s after the kill, which is the wait for the tick
// itself rather than for the election. Three occurrences leaves room for a
// loaded CI box without admitting a failover that is actually slow.
const failoverBudget = 3 * time.Second

// electionAllowance is what "takes over within one interval" is allowed on
// top of the interval itself. A loser sleeps at most one interval between
// attempts, but the attempt that wins still costs work the interval does not
// cover: PostgreSQL ending the dead session and dropping its lock, the
// winner's pool acquire and pg_try_advisory_lock round trip, and this test's
// own polling. Those are milliseconds each; 150ms leaves room for -race and a
// loaded CI box while still failing a takeover that waited a second interval
// or more.
const electionAllowance = 150 * time.Millisecond

// awaitTakeover polls until exactly one of survivors holds the scheduler lock
// and returns that holder and how long after since it was first observed.
// The latency is an overestimate by up to one poll plus whatever ran between
// since and the call, so a bound asserted on it is conservative.
func awaitTakeover(t *testing.T, ctx context.Context, survivors []string, since time.Time) (string, time.Duration) {
	t.Helper()
	deadline := time.Now().Add(2 * failoverBudget)
	for time.Now().Before(deadline) {
		holders, err := lockHolders(ctx)
		if err == nil && len(holders) == 1 && slices.Contains(survivors, holders[0].Name) {
			return holders[0].Name, time.Since(since)
		}
		time.Sleep(2 * time.Millisecond)
	}
	holders, err := lockHolders(ctx)
	t.Fatalf("no survivor of %v took the lock within %s of the kill (holders %v, err %v)",
		survivors, 2*failoverBudget, holders, err)
	return "", 0
}

// requireTakeoverWithinOneInterval is the spec's "another takes over within
// one interval", with the bound stated in electionAllowance.
func requireTakeoverWithinOneInterval(t *testing.T, latency, interval time.Duration) {
	t.Helper()
	require.LessOrEqual(t, latency, interval+electionAllowance,
		"takeover took %s; one interval is %s plus %s for the winning election", latency, interval, electionAllowance)
}

// TestSchedulerFailover is the session's acceptance evidence: three
// schedulers, one fires; its database session is killed; a different one
// takes over and fires the tick that was pending at the crash — twenty times.
//
// Duplicate suppression is not what this test proves. The unique index on
// (queue, idempotency_key) makes a duplicate key impossible, so a test that
// only counted rows would pass even if failover were broken. What it proves
// is that an ungraceful death hands the role to a *different* instance, that
// the tick pending at that moment is not lost, and that no occurrence is
// fired twice under different keys — which is what a rewound or
// mis-computed next_run_at would produce.
func TestSchedulerFailover(t *testing.T) {
	names := []string{"hopper-sched-0", "hopper-sched-1", "hopper-sched-2"}
	stores := map[string]store.Store{}
	for _, name := range names {
		stores[name] = postgres.New(identifiedPool(t, name))
	}
	for i := range 20 {
		t.Run(fmt.Sprintf("round-%02d", i), func(t *testing.T) {
			failoverRound(t, names, stores)
		})
	}
}

func failoverRound(t *testing.T, names []string, stores map[string]store.Store) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)

	// Seed on the whole-second lattice that @every 1s occurrences land on
	// (scheduler.TestEveryRoundsUpToTheNextSecondBoundary): still in the
	// past, so the first tick is due at once, but aligned so every later
	// occurrence is an exact number of seconds after it.
	var seed time.Time
	require.NoError(t, pool.QueryRow(ctx, "select date_trunc('second', now())").Scan(&seed))
	_, err = postgres.New(pool).UpsertSchedule(ctx, store.CronScheduleSpec{
		Name: "failover", Queue: "cron_demo", CronExpr: "@every 1s", Timezone: "UTC",
		Enabled: true, NextRunAt: seed,
	})
	require.NoError(t, err)

	runs := map[string]*schedulerRun{}
	for _, name := range names {
		runs[name] = startScheduler(name, stores[name])
	}
	stopped := map[string]bool{}
	defer func() {
		for name, r := range runs {
			if !stopped[name] {
				r.stop(t)
			}
		}
	}()

	require.Eventually(t, func() bool { return jobAt(ctx, seed) },
		5*time.Second, 10*time.Millisecond, "a leader must be elected and fire the seeded tick")

	holder := requireLeader(t, ctx)
	leader, pid := holder.Name, holder.PID
	require.Contains(t, names, leader)

	// P is the tick pending at the moment of the crash.
	var pending time.Time
	require.NoError(t, pool.QueryRow(ctx,
		"select next_run_at from cron_schedules where name = 'failover'").Scan(&pending))

	// The ungraceful loss first: the lock is dropped with no Release. Only
	// then is the dead instance cancelled, so the survivors win an election a
	// crash caused, and the instance that died cannot be the one that answers.
	_, err = pool.Exec(ctx, "select pg_terminate_backend($1)", pid)
	require.NoError(t, err)
	killedAt := time.Now()
	runs[leader].stop(t)
	stopped[leader] = true

	survivors := slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == leader })
	successor, latency := awaitTakeover(t, ctx, survivors, killedAt)
	requireTakeoverWithinOneInterval(t, latency, schedulerInterval)
	// Wait for the survivor to advance past P, not merely to enqueue it: the
	// job row appears one statement before the advance (decision 4), so a
	// condition on the job alone can let the cancel below land between the
	// two, and the round would never observe the survivor finish the fire.
	require.Eventually(t, func() bool {
		return heldBy(ctx, survivors) && jobAt(ctx, pending) && nextRunAt(ctx).After(pending)
	}, 2*failoverBudget, 10*time.Millisecond,
		"a survivor must take the lock, fire the tick pending at the crash (%s) and advance past it", pending)
	t.Logf("killed %s; %s took over in %s and fired tick %s %s after the kill", leader, successor,
		latency.Round(time.Millisecond), pending.Format(time.TimeOnly), time.Since(killedAt).Round(time.Millisecond))

	for _, name := range survivors {
		runs[name].stop(t)
		stopped[name] = true
	}

	assertScheduleIntegrity(t, ctx, "cron_demo", "failover", seed, pending)
}

// nextRunAt reads the schedule's pending tick, or the zero time on error.
// Like heldBy, it is safe inside a require.Eventually condition.
func nextRunAt(ctx context.Context) time.Time {
	var next time.Time
	if err := pool.QueryRow(ctx,
		"select next_run_at from cron_schedules where name = 'failover'").Scan(&next); err != nil {
		return time.Time{}
	}
	return next
}

// jobAt reports whether a cron_demo job exists for the tick runAt. Like
// heldBy, it is safe inside a require.Eventually condition.
func jobAt(ctx context.Context, runAt time.Time) bool {
	var found bool
	err := pool.QueryRow(ctx,
		"select exists (select 1 from jobs where queue = 'cron_demo' and run_at = $1)", runAt).Scan(&found)
	return err == nil && found
}

// assertScheduleIntegrity checks the properties the design promises across
// a failover, and deliberately not "no gaps": the catch-up policy skips
// occurrences a slow failover missed (docs/semantics.md §13), so a gap is
// the design working, and only a gap beyond the failover budget is a defect.
// pending lists the tick outstanding at each kill; every one must have fired.
func assertScheduleIntegrity(t *testing.T, ctx context.Context, queue, schedule string, seed time.Time, pending ...time.Time) {
	t.Helper()
	rows, err := pool.Query(ctx, "select run_at from jobs where queue = $1 order by run_at, id", queue)
	require.NoError(t, err)
	runAts, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
	require.NoError(t, err)
	require.NotEmpty(t, runAts)

	onLattice := func(ts time.Time) bool {
		return ts.UnixMicro()%1_000_000 == 0 && !ts.Before(seed) && ts.Sub(seed)%time.Second == 0
	}

	require.True(t, runAts[0].Equal(seed), "the seeded tick fires first: got %s, seeded %s", runAts[0], seed)
	for i, ts := range runAts {
		require.True(t, onLattice(ts), "job %d run_at %s is off the one-second lattice from %s", i, ts, seed)
		if i == 0 {
			continue
		}
		gap := ts.Sub(runAts[i-1])
		// Strictly increasing also proves count(*) == count(distinct run_at).
		require.Positive(t, gap, "occurrence %s fired twice", ts)
		require.LessOrEqual(t, gap, failoverBudget, "gap before %s exceeds the failover budget", ts)
		if gap > time.Second {
			t.Logf("gap of %s before %s (catch-up skipped %d occurrence(s))", gap, ts.Format(time.TimeOnly), int(gap/time.Second)-1)
		}
	}
	for _, p := range pending {
		require.Truef(t, slices.ContainsFunc(runAts, p.Equal), "the tick pending at the crash (%s) was lost", p)
	}

	var next time.Time
	require.NoError(t, pool.QueryRow(ctx,
		"select next_run_at from cron_schedules where name = $1", schedule).Scan(&next))
	// Ahead of or equal to the last job, not strictly ahead: stopping the
	// survivors can land between a tick's enqueue and its advance, which is
	// decision 4's crash window. The tick is then enqueued and still pending,
	// and the next leader re-fires it into the idempotency key. Behind the
	// last job would be a rewound schedule, and that is what this rules out.
	last := runAts[len(runAts)-1]
	require.False(t, next.Before(last), "next_run_at %s must not trail the last job %s", next, last)
	if next.Equal(last) {
		t.Logf("stopped inside the fire/advance window: %s enqueued, not yet advanced", last.Format(time.TimeOnly))
	}
	require.True(t, onLattice(next), "next_run_at %s is off the lattice", next)
}
