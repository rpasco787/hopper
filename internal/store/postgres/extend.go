package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ExtendLease is the heartbeat in docs/semantics.md §4: it moves lease_expires
// forward only if the caller still holds the lease.
//
// Same shape as Ack: one guarded UPDATE, no transaction. If the row is no
// longer (id, lease_id, leased) the UPDATE touches nothing and classifyMiss
// names the reason. No "done" state is passed to classifyMiss because a
// heartbeat has no idempotent-success case: if the row were still leased under
// this lease_id the UPDATE would have hit. A miss always means the lease is
// gone (or the job was cancelled), and the SDK must stop the handler.
func (s *Store) ExtendLease(ctx context.Context, id, leaseID uuid.UUID, dur time.Duration) error {
	if dur <= 0 {
		return errors.New("store: extend lease: duration must be positive")
	}
	tag, err := s.pool.Exec(ctx, `
		update jobs
		set lease_expires = now() + $3::interval, updated_at = now()
		where id = $1 and lease_id = $2 and state = 'leased'`,
		id, leaseID, dur)
	if err != nil {
		return fmt.Errorf("store: extend lease: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.classifyMiss(ctx, id, leaseID)
}
