package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// ListJobs pages a queue by primary key.
//
// The cursor is just an ID. UUIDv7 values usually follow creation time, but
// application clocks, concurrent commits and the schema's UUIDv4 fallback
// prevent an exact created_at guarantee. ID keyset pagination still works.
// Keep queue/state filters fixed throughout a traversal. Concurrent changes
// may make rows disappear or add matching rows behind the cursor.
//
// The state filter is passed as a text array rather than built into the SQL,
// so this is one prepared statement whatever the caller asks for.
func (s *Store) ListJobs(ctx context.Context, queue string, states []store.State, limit int, after *uuid.UUID) ([]store.Job, error) {
	if queue == "" {
		return nil, errors.New("store: list jobs: queue is required")
	}
	if limit <= 0 {
		return nil, nil
	}

	// nil, not an empty slice: the `$2 is null` disjunct is what "every state"
	// means, and an empty array would match nothing.
	var filter []string
	for _, st := range states {
		filter = append(filter, string(st))
	}

	rows, err := s.pool.Query(ctx, `
		select `+jobColumns+` from jobs
		where queue = $1
		  and ($2::text[] is null or state = any($2::text[]))
		  and ($3::uuid is null or id > $3::uuid)
		order by id
		limit $4`,
		queue, filter, after, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[store.Job])
	if err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	return jobs, nil
}
