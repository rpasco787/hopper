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
