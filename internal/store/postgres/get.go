package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// GetJob reads one row by primary key.
//
// Like ListDead, GetJob is a plain read. It makes no transition and supplies
// no concurrency guard for a subsequent mutation. That distinction is worth
// keeping: the moment a caller reads a row here and then writes based on what
// it saw, the guard is gone and two callers can both act on the same stale
// state.
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (store.Job, error) {
	rows, err := s.pool.Query(ctx,
		`select `+jobColumns+` from jobs where id = $1`, id)
	if err != nil {
		return store.Job{}, wrap("get job", err)
	}
	job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[store.Job])
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return store.Job{}, store.ErrNotFound
	case err != nil:
		return store.Job{}, wrap("get job", err)
	}
	return job, nil
}
