package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// Ack marks a leased job completed. It is the guarded UPDATE in
// docs/semantics.md §5.
//
// This is compare-and-set without a transaction: the WHERE clause is the
// compare (this id, this lease_id, still leased) and the SET is the swap.
// Postgres evaluates them atomically for the row, so if anything changed under
// us the UPDATE touches zero rows and nothing is written. We then ask
// classifyMiss why, purely to give the caller a precise error. A transaction
// would add nothing: there is no read-then-write here whose interleaving could
// produce a wrong state.
//
// lease_id is deliberately not cleared. A completed row that still carries the
// caller's lease_id is how a retried Ack (first response lost) is recognised as
// already done rather than rejected.
func (s *Store) Ack(ctx context.Context, id, leaseID uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		update jobs
		set state = 'completed', finished_at = now(), updated_at = now()
		where id = $1 and lease_id = $2 and state = 'leased'`,
		id, leaseID)
	if err != nil {
		return fmt.Errorf("store: ack: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.classifyMiss(ctx, id, leaseID, store.StateCompleted)
}

// classifyMiss explains a guarded UPDATE that affected zero rows, mapping the
// row's current state and lease_id to the table in docs/semantics.md §5.
// done is the terminal state the caller was trying to reach: a row already in
// that state under the same lease_id means the caller's earlier attempt
// succeeded and its response was lost, so the result is nil. Ack passes
// completed; Nack passes scheduled or dead; Session 2's ExtendLease passes
// leased.
//
// The read here is racy by design: the row can change again between the
// caller's miss and this SELECT. That is acceptable because classifyMiss only
// chooses an error message, never writes. The worst case is that a caller who
// lost the lease is told ErrCancelled instead of ErrLeaseLost (or vice versa)
// because the row moved twice in a few microseconds; either way the caller
// learns it no longer holds the job and stops. No sequence of interleavings
// can turn a miss into a wrong state, because nothing here mutates.
func (s *Store) classifyMiss(ctx context.Context, id, leaseID uuid.UUID, done ...store.State) error {
	var (
		state    store.State
		curLease *uuid.UUID // nullable: a job that was never leased has none
	)
	err := s.pool.QueryRow(ctx,
		`select state, lease_id from jobs where id = $1`, id,
	).Scan(&state, &curLease)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return store.ErrNotFound
	case err != nil:
		return fmt.Errorf("store: classify miss: %w", err)
	}

	sameLease := curLease != nil && *curLease == leaseID
	for _, d := range done {
		if state == d && sameLease {
			return nil // idempotent retry of an operation that already applied
		}
	}
	if state == store.StateCancelled {
		return store.ErrCancelled
	}
	// Leased under a different lease_id, or scheduled/available/dead/completed
	// under any lease_id: the caller's lease is gone.
	return store.ErrLeaseLost
}
