package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// leaseExpiredError is what the reaper writes to last_error
// (docs/semantics.md §3).
const leaseExpiredError = "lease expired"

// ReapExpiredLeases is the "Expiry" paragraph of docs/semantics.md §4. It
// locks a batch of expired leases, applies the retry policy to each, and
// commits.
//
// Why a transaction with SELECT ... FOR UPDATE SKIP LOCKED rather than one
// UPDATE: the backoff depends on attempts and max_attempts per row, and it is
// computed in Go so it shares store.RetryPolicy with Nack. SKIP LOCKED matters
// here for a different reason than in Lease. A Nack holds a row lock for the
// few microseconds between its SELECT and its UPDATE; if the reaper waited on
// that lock it would then apply a second failure to a row Nack just handled.
// Skipping it means the row is simply picked up on the next tick if it is
// still expired, which by then it will not be.
//
// The other race, Ack versus reaper, needs no special handling. Ack's UPDATE
// blocks on our row lock, then re-evaluates its WHERE after we commit, sees
// state <> 'leased', updates nothing and reports ErrLeaseLost.
//
// ORDER BY lease_expires matches jobs_reap_idx, so the oldest expiries go
// first and the scan stops after limit rows.
func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: reap: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once Commit has succeeded

	rows, err := tx.Query(ctx, `
		select id, attempts, max_attempts from jobs
		where state = 'leased' and lease_expires < now()
		order by lease_expires
		limit $1
		for update skip locked`,
		limit)
	if err != nil {
		return 0, fmt.Errorf("store: reap: select: %w", err)
	}
	type expired struct {
		ID          uuid.UUID
		Attempts    int32
		MaxAttempts int32
	}
	batch, err := pgx.CollectRows(rows, pgx.RowToStructByPos[expired])
	if err != nil {
		return 0, fmt.Errorf("store: reap: collect: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	for _, e := range batch {
		if err := s.failLeased(ctx, tx, e.ID, e.Attempts, e.MaxAttempts, leaseExpiredError); err != nil {
			return 0, fmt.Errorf("store: reap %s: %w", e.ID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: reap: commit: %w", err)
	}
	return len(batch), nil
}
