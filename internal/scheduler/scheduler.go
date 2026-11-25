package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/rpasco787/hopper/internal/store"
)

// Store is the part of store.Store the scheduler needs. Declaring it here
// rather than importing the whole interface keeps the loop testable against a
// fake and documents exactly what a scheduler is allowed to do.
//
// Now, UpsertSchedule and DeleteSchedulesExcept are declared now and used
// starting in the task that adds reconciliation; the interface is the
// scheduler's whole store contract and is not worth editing twice.
type Store interface {
	TryLeadership(ctx context.Context, ttl time.Duration) (store.Leadership, error)
	DueSchedules(ctx context.Context, limit int) ([]store.CronSchedule, time.Time, error)
	AdvanceSchedule(ctx context.Context, name string, from, to time.Time) (bool, error)
	Enqueue(ctx context.Context, p store.EnqueueParams) (store.Job, bool, error)
	Now(ctx context.Context) (time.Time, error)
	UpsertSchedule(ctx context.Context, spec store.CronScheduleSpec) (store.CronSchedule, error)
	DeleteSchedulesExcept(ctx context.Context, keep []string) (int, error)
}

var _ Store = (store.Store)(nil)

// errBadSchedule marks a schedule that cannot be fired this pass: an
// unparseable cron_expr or timezone, or a next occurrence that would not
// advance past the tick that just fired. It is logged once per term
// (docs/plans decision 8) and, unlike a store error, never ends the term: the
// other schedules are not at fault for one bad row.
var errBadSchedule = errors.New("scheduler: bad schedule")

// Scheduler fires cron schedules while it holds leadership. Every broker runs
// one; at most one is active. See docs/semantics.md §13.
type Scheduler struct {
	Store Store
	// Interval paces both leadership retry and, while leading, the tick.
	// Default 5s. A schedule fires within roughly one interval of its tick.
	Interval time.Duration
	// LeaderTTL is how long this scheduler's database session may run no
	// statement before PostgreSQL ends it and frees the role
	// (store.Store.TryLeadership): the bound on a silent leader. Zero takes
	// DefaultLeaderTTL(Interval). Anything under three intervals is raised to
	// three, because a healthy leader can be idle on its session for up to two.
	LeaderTTL time.Duration
	BatchSize int // default 100
	Logger    *slog.Logger
	// Definitions, when non-empty, are reconciled into cron_schedules at the
	// start of every term and are authoritative: names outside the set are
	// pruned. Empty means the table is managed elsewhere and left alone.
	Definitions []Definition
}

func (s *Scheduler) interval() time.Duration {
	if s.Interval <= 0 {
		return 5 * time.Second
	}
	return s.Interval
}

// minLeaderTTL is the floor under DefaultLeaderTTL, so a fast test interval
// doesn't make a loaded machine's slow statement look like a silent leader.
const minLeaderTTL = 3 * time.Second

// DefaultLeaderTTL is the leader TTL for a scheduler interval: three
// intervals, and never under three seconds (docs/semantics.md §13).
func DefaultLeaderTTL(interval time.Duration) time.Duration {
	return max(3*interval, minLeaderTTL)
}

func (s *Scheduler) leaderTTL() time.Duration {
	if s.LeaderTTL <= 0 {
		return DefaultLeaderTTL(s.interval())
	}
	return max(s.LeaderTTL, 3*s.interval())
}

// stepTimeout bounds each store call in a term: a third of the leader TTL.
// A healthy leader runs a statement on its session at least every pass plus
// one wait, at most two steps, so it never comes near the TTL; a cut-off
// leader's last pass ends within a step of its last answered Check, and its
// next Check fails within two, a step before PostgreSQL can free the role
// (docs/semantics.md §13).
func (s *Scheduler) stepTimeout() time.Duration {
	return s.leaderTTL() / 3
}

// within runs f with a context that ends step from now, or when ctx does.
func within(ctx context.Context, step time.Duration, f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, step)
	defer cancel()
	return f(ctx)
}

func (s *Scheduler) batchSize() int {
	if s.BatchSize <= 0 {
		return 100
	}
	return s.BatchSize
}

func (s *Scheduler) logger() *slog.Logger {
	if s.Logger == nil {
		return slog.Default()
	}
	return s.Logger
}

// Run attempts a term, then waits one interval or the context, until ctx is
// cancelled. It mirrors sweep.Loop.Run's shape: try work, then wait.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval())
	defer ticker.Stop()

	for {
		err := s.term(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, store.ErrNotLeader):
			// Ordinary contention: another replica leads. Retry next interval.
		case err != nil:
			s.logger().ErrorContext(ctx, "scheduler term ended", "err", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// term acquires leadership, reconciles Definitions when there are any, and
// runs Check -> pass -> wait until leadership is lost, a store error occurs,
// or ctx is cancelled (docs/plans decision 11). A reconcile failure ends the
// term before the first pass, so a half-applied configuration never fires.
//
// Every store call runs under stepTimeout: the attempt, the reconciliation,
// each Check, each pass and the release. Running out of time is a store
// error like any other, so a leader cut off from PostgreSQL ends its term
// instead of waiting on TCP. Release runs on a context.WithoutCancel
// derivative, so a cancelled Run still releases rather than depending on the
// session dying on its own.
func (s *Scheduler) term(ctx context.Context) error {
	ttl, step := s.leaderTTL(), s.stepTimeout()
	var lead store.Leadership
	if err := within(ctx, step, func(ctx context.Context) error {
		var err error
		lead, err = s.Store.TryLeadership(ctx, ttl)
		return err
	}); err != nil {
		return err
	}
	s.logger().InfoContext(ctx, "scheduler term won")
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), step)
		defer cancel()
		if err := lead.Release(rctx); err != nil {
			s.logger().WarnContext(ctx, "scheduler release failed", "err", err)
			return
		}
		s.logger().InfoContext(ctx, "scheduler term released")
	}()

	if len(s.Definitions) > 0 {
		if err := within(ctx, step, s.reconcile); err != nil {
			return err
		}
	}

	// Suppresses repeated bad-schedule log lines within this term; recreated
	// per term so a restarted term reports the problem again (decision 8).
	warned := map[string]string{}

	ticker := time.NewTicker(s.interval())
	defer ticker.Stop()

	for {
		if err := within(ctx, step, lead.Check); err != nil {
			return fmt.Errorf("scheduler: leadership lost: %w", err)
		}
		if err := within(ctx, step, func(ctx context.Context) error { return s.pass(ctx, warned) }); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// reconcile applies Definitions to cron_schedules: upsert every definition,
// then prune every other name. Upsert comes first because a prune that ran
// first would delete a row this same pass is about to recreate, losing its
// pending tick.
//
// Every NextRunAt is computed from one read of the database clock, never
// time.Now (decision 6). The value reaches the table only for a new row or
// one whose cron_expr or timezone changed — UpsertSchedule keeps the stored
// tick otherwise — so this clock decides a schedule's first tick, which is
// the one instant DueSchedules never judges. Reading it once means a long
// reconciliation cannot spread one configuration across a moving now.
func (s *Scheduler) reconcile(ctx context.Context) error {
	dbNow, err := s.Store.Now(ctx)
	if err != nil {
		return fmt.Errorf("scheduler: reconcile: read database clock: %w", err)
	}
	names := make([]string, 0, len(s.Definitions))
	for _, d := range s.Definitions {
		spec, err := ParseSpec(d.Cron, d.Timezone)
		if err != nil {
			return fmt.Errorf("scheduler: reconcile %s: %w", d.Name, err)
		}
		next, err := spec.Next(dbNow)
		if err != nil {
			return fmt.Errorf("scheduler: reconcile %s: %w", d.Name, err)
		}
		// LoadFile has range-checked Priority and MaxAttempts.
		if _, err := s.Store.UpsertSchedule(ctx, store.CronScheduleSpec{
			Name: d.Name, Queue: d.Queue, CronExpr: d.Cron, Timezone: d.Timezone,
			Payload: d.Payload, Priority: int16(d.Priority), MaxAttempts: int32(d.MaxAttempts),
			Enabled: d.enabled(), NextRunAt: next,
		}); err != nil {
			return fmt.Errorf("scheduler: reconcile %s: %w", d.Name, err)
		}
		names = append(names, d.Name)
	}
	pruned, err := s.Store.DeleteSchedulesExcept(ctx, names)
	if err != nil {
		return fmt.Errorf("scheduler: reconcile: prune: %w", err)
	}
	s.logger().InfoContext(ctx, "scheduler reconciled", "schedules", len(names), "pruned", pruned)
	return nil
}

// pass reads exactly one batch of due schedules and fires each. It must not
// loop internally to drain a backlog larger than BatchSize: that bound is
// what makes the check at the top of the next iteration close the window a
// displaced leader could otherwise keep writing into (decision 11).
func (s *Scheduler) pass(ctx context.Context, warned map[string]string) error {
	due, dbNow, err := s.Store.DueSchedules(ctx, s.batchSize())
	if err != nil {
		return fmt.Errorf("scheduler: due schedules: %w", err)
	}
	for _, cs := range due {
		if err := s.fire(ctx, cs, dbNow, warned); err != nil {
			if errors.Is(err, errBadSchedule) {
				continue
			}
			return err
		}
	}
	return nil
}

// fire implements decision 4 in order: enqueue the due tick, then advance
// past it. next is computed from dbNow, never time.Now, so a broker whose
// clock trails the database's cannot manufacture a backfill (decision 6).
//
// The next-occurrence guard runs before either statement: a schedule whose
// computed next does not strictly advance past the tick it is about to fire
// is treated as bad and produces neither an Enqueue nor an AdvanceSchedule
// call, the same as an unparseable expression (decision 7, decision 8).
func (s *Scheduler) fire(ctx context.Context, cs store.CronSchedule, dbNow time.Time, warned map[string]string) error {
	spec, err := ParseSpec(cs.CronExpr, cs.Timezone)
	if err != nil {
		s.skipBadSchedule(ctx, cs, warned, err)
		return errBadSchedule
	}
	next, err := spec.Next(dbNow)
	if err != nil {
		s.skipBadSchedule(ctx, cs, warned, err)
		return errBadSchedule
	}
	if !next.After(cs.NextRunAt) {
		s.skipBadSchedule(ctx, cs, warned,
			fmt.Errorf("next occurrence %s does not advance past fired tick %s", next, cs.NextRunAt))
		return errBadSchedule
	}

	key := IdempotencyKey(cs.Name, cs.NextRunAt)
	job, inserted, err := s.Store.Enqueue(ctx, store.EnqueueParams{
		Queue:          cs.Queue,
		Payload:        cs.Payload,
		Priority:       cs.Priority,
		RunAt:          cs.NextRunAt,
		MaxAttempts:    cs.MaxAttempts,
		IdempotencyKey: &key,
	})
	if err != nil {
		return fmt.Errorf("scheduler: enqueue %s: %w", cs.Name, err)
	}
	s.logger().InfoContext(ctx, "scheduler tick fired",
		"schedule", cs.Name, "tick", cs.NextRunAt, "job_id", job.ID, "deduplicated", !inserted)

	ok, err := s.Store.AdvanceSchedule(ctx, cs.Name, cs.NextRunAt, next)
	if err != nil {
		return fmt.Errorf("scheduler: advance %s: %w", cs.Name, err)
	}
	if !ok {
		// A zombie leader's advance names a tick the new leader already
		// advanced past; matching no row is the safe outcome (decision 3),
		// not an error, so the next pass simply re-reads the row.
		s.logger().WarnContext(ctx, "scheduler stale advance", "schedule", cs.Name, "tick", cs.NextRunAt)
	}
	return nil
}

// skipBadSchedule logs a skipped schedule once per (name, cron_expr,
// timezone) per leadership term. An unconditional log line every pass would
// bury everything else in the broker's output (decision 8).
func (s *Scheduler) skipBadSchedule(ctx context.Context, cs store.CronSchedule, warned map[string]string, cause error) {
	sig := cs.CronExpr + "|" + cs.Timezone
	if warned[cs.Name] == sig {
		return
	}
	warned[cs.Name] = sig
	s.logger().WarnContext(ctx, "scheduler skipped bad schedule",
		"schedule", cs.Name, "cron_expr", cs.CronExpr, "timezone", cs.Timezone, "err", cause)
}
