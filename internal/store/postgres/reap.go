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
// computed in Go so it shares store.RetryPolicy with Nack. SKIP LOCKED is not
// needed for correctness here — if the reaper's SELECT blocked on a row Nack
// is mid-transaction on, Postgres would re-evaluate the WHERE after Nack
// commits (the same EvalPlanQual mechanism that makes Ack's UPDATE safe below)
// and the row would simply no longer match state = 'leased', so it could never
// be double-processed either way. SKIP LOCKED is here for liveness: without
// it the reaper would block for the duration of every concurrent Nack
// transaction on an overlapping row, which is unnecessary latency, not a
// correctness fix.
//
// The other race, Ack versus reaper, needs no special handling. Ack's and
// ExtendLease's UPDATEs block on our row lock, then re-evaluate their WHERE
// after we commit, see state <> 'leased', update nothing and report
// ErrLeaseLost — TestReapReschedulesExpiredLease asserts this for both.
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
		select id, queue, attempts, max_attempts from jobs
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
		Queue       string
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

	type outcome struct {
		queue string
		dead  bool
	}
	outcomes := make([]outcome, 0, len(batch))
	for _, e := range batch {
		dead, err := s.failLeased(ctx, tx, e.ID, e.Attempts, e.MaxAttempts, leaseExpiredError)
		if err != nil {
			return 0, fmt.Errorf("store: reap: row %s: %w", e.ID, err)
		}
		outcomes = append(outcomes, outcome{queue: e.Queue, dead: dead})
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: reap: commit: %w", err)
	}
	for _, result := range outcomes {
		s.obs.LeaseExpired(result.queue, result.dead)
	}
	return len(batch), nil
}
