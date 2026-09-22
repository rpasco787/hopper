package postgres

// cron_schedules has no index of its own beyond its primary key. The table
// holds tens of rows; a sequential scan every few seconds is cheaper than the
// write amplification an index would add to every UpsertSchedule and
// AdvanceSchedule. Revisit this once the table holds thousands of schedules.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// cronColumns is the cron_schedules equivalent of jobColumns: the one place
// the column list is spelled out.
const cronColumns = `name, queue, payload, cron_expr, timezone, enabled,
	priority, max_attempts, next_run_at, last_enqueued_at, created_at, updated_at`

// UpsertSchedule inserts or updates one schedule and returns the stored row.
// The next_run_at CASE is what makes this safe without a read-modify-write: a
// redeploy that leaves cron_expr and timezone unchanged keeps the pending
// tick, and a redeploy that changes either adopts spec.NextRunAt.
func (s *Store) UpsertSchedule(ctx context.Context, spec store.CronScheduleSpec) (store.CronSchedule, error) {
	payload := spec.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	maxAttempts := spec.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = defaultMaxAttempts
	}

	rows, err := s.pool.Query(ctx, `
		insert into cron_schedules
		    (name, queue, payload, cron_expr, timezone, enabled, priority, max_attempts, next_run_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		on conflict (name) do update set
		    queue        = excluded.queue,
		    payload      = excluded.payload,
		    cron_expr    = excluded.cron_expr,
		    timezone     = excluded.timezone,
		    enabled      = excluded.enabled,
		    priority     = excluded.priority,
		    max_attempts = excluded.max_attempts,
		    next_run_at  = case
		        when cron_schedules.cron_expr is distinct from excluded.cron_expr
		          or cron_schedules.timezone  is distinct from excluded.timezone
		        then excluded.next_run_at
		        else cron_schedules.next_run_at
		    end,
		    updated_at = now()
		returning `+cronColumns,
		spec.Name, spec.Queue, payload, spec.CronExpr, spec.Timezone, spec.Enabled,
		spec.Priority, maxAttempts, spec.NextRunAt,
	)
	if err != nil {
		return store.CronSchedule{}, fmt.Errorf("store: upsert schedule: %w", err)
	}
	cs, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[store.CronSchedule])
	if err != nil {
		return store.CronSchedule{}, fmt.Errorf("store: upsert schedule: %w", err)
	}
	return cs, nil
}

// dueRow embeds store.CronSchedule so pgx.RowToStructByName can scan the
// schedule columns and db_now in one pass; pgx v5 walks anonymous struct
// fields when matching column names.
type dueRow struct {
	store.CronSchedule
	DBNow time.Time `db:"db_now"`
}

// DueSchedules returns up to limit enabled, due schedules soonest first,
// together with the database clock that judged them (docs/semantics.md §13).
// This is a plain read: exclusion between replicas comes from leadership, not
// from row locks, so it needs no FOR UPDATE.
func (s *Store) DueSchedules(ctx context.Context, limit int) ([]store.CronSchedule, time.Time, error) {
	rows, err := s.pool.Query(ctx, `
		select `+cronColumns+`, now() as db_now
		from cron_schedules
		where enabled and next_run_at <= now()
		order by next_run_at
		limit $1`,
		limit)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("store: due schedules: %w", err)
	}
	due, err := pgx.CollectRows(rows, pgx.RowToStructByName[dueRow])
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("store: due schedules: %w", err)
	}
	if len(due) == 0 {
		return nil, time.Time{}, nil
	}

	out := make([]store.CronSchedule, len(due))
	dbNow := due[0].DBNow
	for i, d := range due {
		out[i] = d.CronSchedule
	}
	return out, dbNow, nil
}

// AdvanceSchedule moves a schedule's pending tick from to to, guarded on the
// stored next_run_at still equalling from. A zero-row match is a normal
// failover outcome (docs/plans decision 3), not an error.
func (s *Store) AdvanceSchedule(ctx context.Context, name string, from, to time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		update cron_schedules
		set next_run_at = $3, last_enqueued_at = now(), updated_at = now()
		where name = $1 and next_run_at = $2`,
		name, from, to)
	if err != nil {
		return false, fmt.Errorf("store: advance schedule: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListSchedules returns every schedule ordered by name. Inspection only.
func (s *Store) ListSchedules(ctx context.Context) ([]store.CronSchedule, error) {
	rows, err := s.pool.Query(ctx, `select `+cronColumns+` from cron_schedules order by name`)
	if err != nil {
		return nil, fmt.Errorf("store: list schedules: %w", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByName[store.CronSchedule])
	if err != nil {
		return nil, fmt.Errorf("store: list schedules: %w", err)
	}
	return got, nil
}

// Now returns the database's current time (docs/plans decision 6): the one
// clock reconciliation computes a newly-inserted schedule's first tick from,
// so a broker whose clock is skewed cannot write a tick the database would
// not have chosen.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := s.pool.QueryRow(ctx, `select now()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("store: now: %w", err)
	}
	return now, nil
}

// DeleteSchedulesExcept deletes every schedule whose name is not in keep and
// returns how many it removed. An empty keep is rejected: the difference
// between "the file lists nothing" and "no file was loaded" is one a caller
// must state, not one this method should guess at.
func (s *Store) DeleteSchedulesExcept(ctx context.Context, keep []string) (int, error) {
	if len(keep) == 0 {
		return 0, errors.New("store: delete schedules except: keep is required")
	}
	tag, err := s.pool.Exec(ctx, `delete from cron_schedules where not (name = any($1))`, keep)
	if err != nil {
		return 0, fmt.Errorf("store: delete schedules except: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
