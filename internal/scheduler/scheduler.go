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
	TryLeadership(ctx context.Context) (store.Leadership, error)
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
	Interval  time.Duration
	BatchSize int // default 100
	Logger    *slog.Logger
}

func (s *Scheduler) interval() time.Duration {
	if s.Interval <= 0 {
		return 5 * time.Second
	}
	return s.Interval
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

// term acquires leadership and runs Check -> pass -> wait until leadership is
// lost, a store error occurs, or ctx is cancelled (docs/plans decision 11).
// Release always runs on a context.WithoutCancel derivative with its own
// short timeout, so a cancelled Run still releases rather than depending on
// the session dying on its own.
func (s *Scheduler) term(ctx context.Context) error {
	lead, err := s.Store.TryLeadership(ctx)
	if err != nil {
		return err
	}
	s.logger().InfoContext(ctx, "scheduler term won")
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := lead.Release(rctx); err != nil {
			s.logger().WarnContext(ctx, "scheduler release failed", "err", err)
			return
		}
		s.logger().InfoContext(ctx, "scheduler term released")
	}()

	// Suppresses repeated bad-schedule log lines within this term; recreated
	// per term so a restarted term reports the problem again (decision 8).
	warned := map[string]string{}

	ticker := time.NewTicker(s.interval())
	defer ticker.Stop()

	for {
		if err := lead.Check(ctx); err != nil {
			return fmt.Errorf("scheduler: leadership lost: %w", err)
		}
		if err := s.pass(ctx, warned); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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
