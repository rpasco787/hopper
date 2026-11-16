package postgres

import (
	"context"
	"errors"
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
	tag, err := s.pool.Exec(ctx, extendSQL, id, leaseID, dur)
	if err != nil {
		return wrap("extend lease", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.classifyMiss(ctx, id, leaseID)
}

// extendSQL is ExtendLease's statement: $1 job id, $2 lease id, $3 lease
// duration. It is a variable so tests can EXPLAIN the statement ExtendLease runs.
//
// The state guard is
//
//	(state || '') = 'leased'
//
// not state = 'leased', on purpose. The plain form lets the planner prove
// jobs_reap_idx's predicate and scan that index for the id. With statistics
// from a moment when nothing was leased it believes the index empty, and the
// scan then visits every leased row (docs/scaling.md H3a). The expression can
// only be served by the primary key, and means the same thing: state is never
// null.
var extendSQL = `
		update jobs
		set lease_expires = now() + $3::interval, updated_at = now()
		where id = $1 and lease_id = $2 and (state || '') = 'leased'`
