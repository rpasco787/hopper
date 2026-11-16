package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/rpasco787/hopper/internal/store"
)

// ackManySQL is AckMany's statement: $1 job ids and $2 lease ids, paired by
// position. It is Ack's guarded UPDATE (docs/semantics.md §5) for every pair
// at once, in one implicit transaction and so one commit.
//
// The CTE locks the rows it will change with SKIP LOCKED, so the statement
// never waits for a row lock, and batches on two brokers sharing rows can't
// deadlock. A row it skips, like a row whose guard fails, is missing from
// RETURNING. AckMany answers it ErrNotApplied, and the broker applies it with
// the one-row Ack on that caller's own goroutine, after this statement has
// committed (docs/design/confirmation-batching.md).
//
// The guard is
//
//	(j.state || '') = 'leased'
//
// for the reason ackSQL's comment gives: the plain form lets the planner walk
// jobs_reap_idx (H3a).
// TestBatchConfirmationsUseThePrimaryKeyUnderStaleStatistics checks the plan.
var ackManySQL = `
	with mine as (
		select j.id
		from unnest($1::uuid[], $2::uuid[]) as c(id, lease_id)
		join jobs j on j.id = c.id and j.lease_id = c.lease_id
		where (j.state || '') = 'leased'
		for update of j skip locked
	)
	update jobs j
	set state = 'completed', finished_at = now(), updated_at = now()
	from mine
	where j.id = mine.id
	returning j.id, j.lease_id, j.queue, j.leased_at, j.finished_at`

// AckMany is Ack for many deliveries in one statement (store.BatchConfirmer).
func (s *Store) AckMany(ctx context.Context, refs []store.LeaseRef) []error {
	errs := make([]error, len(refs))
	if len(refs) == 0 {
		return errs
	}
	ids, leases := pairs(refs)
	rows, err := s.pool.Query(ctx, ackManySQL, ids, leases)
	if err != nil {
		return fill(errs, wrap("ack many", err))
	}
	type completion struct {
		ref        store.LeaseRef
		queue      string
		leasedAt   *time.Time // nullable in the schema; always set by Lease
		finishedAt time.Time
	}
	var done []completion
	for rows.Next() {
		var c completion
		if err := rows.Scan(&c.ref.ID, &c.ref.LeaseID, &c.queue, &c.leasedAt, &c.finishedAt); err != nil {
			rows.Close()
			return fill(errs, wrap("ack many", err))
		}
		done = append(done, c)
	}
	if err := rows.Err(); err != nil {
		return fill(errs, wrap("ack many", err))
	}
	// Only once the statement is known to have committed is anything reported.
	applied := make(map[store.LeaseRef]bool, len(done))
	for _, c := range done {
		applied[c.ref] = true
		var la time.Time
		if c.leasedAt != nil {
			la = *c.leasedAt
		}
		s.obs.Completed(c.queue, la, c.finishedAt)
	}
	for i, ref := range refs {
		if !applied[ref] {
			errs[i] = store.ErrNotApplied
		}
	}
	return errs
}

// pairs splits refs into the statement's two arrays, once per distinct ref.
func pairs(refs []store.LeaseRef) (ids, leases []uuid.UUID) {
	seen := make(map[store.LeaseRef]bool, len(refs))
	for _, r := range refs {
		if !seen[r] {
			seen[r] = true
			ids = append(ids, r.ID)
			leases = append(leases, r.LeaseID)
		}
	}
	return ids, leases
}

// fill gives err to every slot that does not already hold an error of its own.
func fill(errs []error, err error) []error {
	for i := range errs {
		if errs[i] == nil {
			errs[i] = err
		}
	}
	return errs
}
