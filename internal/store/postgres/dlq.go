package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// ListDead pages the dead-letter queue (docs/semantics.md §7).
//
// The cursor is the (finished_at, id) of the last row seen and the predicate
// is a row-value comparison, so a page never repeats or skips a row when
// Redrive or Purge removes rows between calls. OFFSET would do both. The
// WHERE and ORDER BY lead with queue and finished_at to walk jobs_dead_idx;
// id is a tiebreaker for rows that died in the same microsecond.
func (s *Store) ListDead(ctx context.Context, queue string, limit int, after *store.DeadCursor) ([]store.Job, error) {
	if queue == "" {
		return nil, errors.New("store: list dead: queue is required")
	}
	if limit <= 0 {
		return nil, nil
	}
	// Nil pointers become SQL NULLs, which the `$2 is null` disjunct turns
	// into "no cursor".
	var (
		afterAt *time.Time
		afterID *uuid.UUID
	)
	if after != nil {
		afterAt, afterID = &after.FinishedAt, &after.ID
	}

	rows, err := s.pool.Query(ctx, `
		select `+jobColumns+` from jobs
		where queue = $1 and state = 'dead'
		  and ($2::timestamptz is null or (finished_at, id) > ($2::timestamptz, $3::uuid))
		order by finished_at, id
		limit $4`,
		queue, afterAt, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list dead: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[store.Job])
	if err != nil {
		return nil, fmt.Errorf("store: list dead: %w", err)
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	return jobs, nil
}

// Redrive is the operator's "try again" (docs/semantics.md §7). It resets the
// retry budget but keeps id, idempotency_key and lease_id: the first two so
// the job is still the same job to the outside world, the last because
// lease_id is never cleared anywhere. The state guard means a caller who lists
// dead jobs, waits, and redrives cannot accidentally reset a job that was
// already redriven and is now running.
func (s *Store) Redrive(ctx context.Context, ids []uuid.UUID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		update jobs
		set state = 'available', attempts = 0, last_error = null, finished_at = null,
		    run_at = now(), updated_at = now()
		where id = any($1) and state = 'dead'`,
		ids)
	if err != nil {
		return 0, fmt.Errorf("store: redrive: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Purge deletes dead rows and with them their idempotency keys. The state
// guard is the safety rail: an operator tool that passes the wrong id cannot
// delete a live job. Completed and cancelled rows are DeleteFinished's, not
// Purge's — the two split the table by state so neither can delete what the
// other is responsible for.
func (s *Store) Purge(ctx context.Context, ids []uuid.UUID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		delete from jobs where id = any($1) and state = 'dead'`,
		ids)
	if err != nil {
		return 0, fmt.Errorf("store: purge: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
