package postgres

import (
	"context"
	"errors"
	"slices"
	"time"

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
	var (
		queue      string
		leasedAt   *time.Time // nullable in the schema; always set by Lease
		finishedAt time.Time
	)
	err := s.pool.QueryRow(ctx, ackSQL, id, leaseID).Scan(&queue, &leasedAt, &finishedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return s.classifyMiss(ctx, id, leaseID, store.StateCompleted)
	case err != nil:
		return wrap("ack", err)
	}
	var la time.Time
	if leasedAt != nil {
		la = *leasedAt
	}
	s.obs.Completed(queue, la, finishedAt)
	return nil
}

// ackSQL is Ack's statement: $1 job id, $2 lease id. It is a variable so tests
// can EXPLAIN the statement Ack runs.
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
var ackSQL = `
		update jobs
		set state = 'completed', finished_at = now(), updated_at = now()
		where id = $1 and lease_id = $2 and (state || '') = 'leased'
		returning queue, leased_at, finished_at`

// classifyMiss explains a guarded UPDATE that affected zero rows, mapping the
// row's current state and lease_id to the table in docs/semantics.md §5.
// done is the terminal state the caller was trying to reach: a row already in
// that state under the same lease_id means the caller's earlier attempt
// succeeded and its response was lost, so the result is nil. Ack passes
// completed; Nack passes scheduled or dead; ExtendLease passes nothing,
// because a heartbeat that misses has no already-applied case.
//
// That nil is a promise, and it is given only once a synchronous standby can
// keep it. The earlier attempt's commit may be one whose wait for the standby
// was cancelled when its caller's deadline ended the statement: PostgreSQL
// keeps such a commit and makes it visible on the primary alone
// (docs/semantics.md §11 item 7). So when the plain read finds the
// already-applied case, the row is read again under FOR KEY SHARE, and the
// answer comes from that read. The lock gives it a transaction ID and a WAL
// record, so its commit waits for the standby like a write's, and WAL is one
// ordered stream: a standby that has this commit has the one it read. A
// refusal (lease lost, cancelled, not found) promises nothing a crash could
// take back, so it never waits (docs/design/cancelled-waits.md).
//
// The reads here are racy by design: the row can change again between the
// caller's miss and either SELECT. That is acceptable because classifyMiss
// only chooses an error message, never writes. The worst case is that a
// caller who lost the lease is told ErrCancelled instead of ErrLeaseLost (or
// vice versa) because the row moved twice in a few microseconds; either way
// the caller learns it no longer holds the job and stops. No sequence of
// interleavings can turn a miss into a wrong state, because nothing here
// mutates.
func (s *Store) classifyMiss(ctx context.Context, id, leaseID uuid.UUID, done ...store.State) error {
	state, curLease, err := s.readMiss(ctx, missSQL, id)
	if err != nil {
		return err
	}
	if applied(state, curLease, leaseID, done) {
		if state, curLease, err = s.readMiss(ctx, missLockedSQL, id); err != nil {
			return err
		}
		if applied(state, curLease, leaseID, done) {
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

// missSQL reads the row a guarded UPDATE missed. missLockedSQL reads it
// under the lock that makes an already-applied answer wait for a standby.
var (
	missSQL       = `select state, lease_id from jobs where id = $1`
	missLockedSQL = missSQL + ` for key share`
)

// readMiss runs one of classifyMiss's reads.
func (s *Store) readMiss(ctx context.Context, sql string, id uuid.UUID) (store.State, *uuid.UUID, error) {
	var (
		state    store.State
		curLease *uuid.UUID // nullable: a job that was never leased has none
	)
	err := s.pool.QueryRow(ctx, sql, id).Scan(&state, &curLease)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", nil, store.ErrNotFound
	case err != nil:
		return "", nil, wrap("classify miss", err)
	}
	return state, curLease, nil
}

// applied reports whether a row in state under curLease is the caller's own
// earlier attempt to reach one of done under leaseID.
func applied(state store.State, curLease *uuid.UUID, leaseID uuid.UUID, done []store.State) bool {
	return curLease != nil && *curLease == leaseID && slices.Contains(done, state)
}
