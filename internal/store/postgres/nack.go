package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// Nack records a failed attempt and applies the retry policy from
// docs/semantics.md §6: dead when attempts >= max_attempts, otherwise
// scheduled with exponential backoff. attempts itself is untouched; it was
// counted at lease time.
//
// Unlike Ack, this is a read-then-write with a decision in between: the
// backoff depends on attempts and max_attempts, which we have to read first.
// A bare UPDATE could not express that safely, because between our read and
// our write the reaper could take the row back and re-lease it. So the read is
// a SELECT ... FOR UPDATE inside a transaction: it pins the row until commit,
// and it carries the same (id, lease_id, state = 'leased') guard as Ack so a
// stale caller gets zero rows rather than a lock.
//
// The delay could be computed in SQL with a CASE expression and a single
// UPDATE, avoiding the transaction. It is computed in Go instead because
// store.RetryPolicy is unit-tested there and shared verbatim with
// ReapExpiredLeases; two implementations of the same formula would drift. The
// transaction is a handful of microseconds: nothing slow runs inside it.
func (s *Store) Nack(ctx context.Context, id, leaseID uuid.UUID, errMsg string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: nack: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once Commit has succeeded

	var attempts, maxAttempts int32
	err = tx.QueryRow(ctx, `
		select attempts, max_attempts from jobs
		where id = $1 and lease_id = $2 and state = 'leased'
		for update`,
		id, leaseID,
	).Scan(&attempts, &maxAttempts)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Release the (never acquired) lock before the classifying read so
		// the diagnostic SELECT is not made inside a transaction we are
		// about to discard anyway. A nack that already applied left the row
		// scheduled or dead under our lease_id: both are success.
		_ = tx.Rollback(ctx)
		return s.classifyMiss(ctx, id, leaseID, store.StateScheduled, store.StateDead)
	case err != nil:
		return fmt.Errorf("store: nack: select: %w", err)
	}

	if err := s.failLeased(ctx, tx, id, attempts, maxAttempts, errMsg); err != nil {
		return fmt.Errorf("store: nack: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: nack: commit: %w", err)
	}
	return nil
}

// failLeased applies docs/semantics.md §6 to a row the caller has locked with
// SELECT ... FOR UPDATE inside tx: dead when attempts >= maxAttempts, otherwise
// scheduled with backoff from s.retry. Nack and ReapExpiredLeases both call it,
// which is what makes "nack" and "lease expired" wait the same way. The WHERE
// is id-only because the row lock, not a state guard, is what protects it.
func (s *Store) failLeased(ctx context.Context, tx pgx.Tx, id uuid.UUID, attempts, maxAttempts int32, errMsg string) error {
	var err error
	if attempts >= maxAttempts {
		_, err = tx.Exec(ctx, `
			update jobs
			set state = 'dead', finished_at = now(), last_error = $2, updated_at = now()
			where id = $1`,
			id, errMsg)
	} else {
		_, err = tx.Exec(ctx, `
			update jobs
			set state = 'scheduled', run_at = now() + $2::interval, last_error = $3, updated_at = now()
			where id = $1`,
			id, s.retry.Delay(attempts), errMsg)
	}
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	return nil
}
