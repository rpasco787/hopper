package postgres

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// Lease atomically takes up to n available jobs from queues and marks them
// leased by workerID for leaseDur. It is the query in docs/semantics.md §4.
//
// The whole operation is one statement and therefore one implicit
// transaction: the CTE locks the candidate rows with FOR UPDATE SKIP LOCKED and
// the UPDATE ... FROM rewrites exactly those rows. Concurrent callers skip each
// other's locked rows rather than blocking on them, so two workers can never be
// handed the same job. Plain FOR UPDATE would make every worker queue behind
// the first one; NOWAIT would make them error instead.
//
// There is deliberately no run_at predicate. Lease trusts the state column;
// PromoteDue, driven by sweep.Promoter, is what moves scheduled rows to
// available when they come due. That keeps the partial index on available rows
// exactly the set of leasable rows, and keeps queue depth meaning "leasable
// right now".
func (s *Store) Lease(ctx context.Context, queues []string, n int, leaseDur time.Duration, workerID string) ([]store.Job, error) {
	if len(queues) == 0 || n <= 0 {
		return nil, nil
	}
	if leaseDur <= 0 {
		return nil, errors.New("store: lease: duration must be positive")
	}

	// Each queue gets one subquery, and a transaction does not skip rows it has
	// locked itself, so a queue named twice would offer its rows twice and the
	// batch would come back short.
	queues = slices.Compact(slices.Sorted(slices.Values(queues)))
	rows, err := s.pool.Query(ctx, leaseSQL, queues, n, workerID, leaseDur)
	if err != nil {
		return nil, wrap("lease", err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[store.Job])
	if err != nil {
		return nil, wrap("lease", err)
	}

	// UPDATE ... RETURNING emits rows in whatever order the executor touched
	// them, not the CTE's order, so restore the contract's ordering here.
	slices.SortStableFunc(jobs, func(a, b store.Job) int {
		if c := cmp.Compare(b.Priority, a.Priority); c != 0 { // desc
			return c
		}
		return a.RunAt.Compare(b.RunAt)
	})
	if len(jobs) == 0 {
		return nil, nil
	}
	s.obs.Leased(jobs)
	return jobs, nil
}

// leaseSQL is Lease's statement: $1 queues, $2 n, $3 worker id, $4 lease
// duration. It is a variable so tests can EXPLAIN the statement Lease runs.
//
// The candidates are chosen one queue at a time. For each queue, a LATERAL
// subquery walks jobs_lease_idx (queue, priority desc, run_at) in order and
// stops after n unlocked rows; the outer query sorts at most n rows per queue
// and keeps the best n. The walk also steps over rows other lessors hold and
// over index entries for rows leased since the last vacuum, so its work is
// not strictly n per queue, but it no longer depends on the backlog. Written
// as `queue = any($1)` instead, the index cannot
// return the subscribed queues in one order, so PostgreSQL read every
// available row in them and sorted the lot on every call: a lease cost
// O(backlog), and a deep queue drained slowest exactly when it most needed to
// drain fast. The per-queue LIMIT gives the same rows,
// because the best n overall are among the best n of each queue.
//
// A queue's subquery can lock up to n rows the outer LIMIT then drops. They
// stay locked only until this statement commits, and other lessors skip them
// in the meantime, which SKIP LOCKED already permits.
var leaseSQL = `
	with next as (
		select c.id
		from unnest($1::text[]) as q(queue),
		lateral (
			select id, priority, run_at from jobs
			where jobs.queue = q.queue and state = 'available'
			order by priority desc, run_at
			limit $2
			for update skip locked
		) c
		order by c.priority desc, c.run_at
		limit $2
	)
	update jobs j
	set state         = 'leased',
	    lease_id      = gen_random_uuid(),
	    leased_by     = $3,
	    leased_at     = now(),
	    lease_expires = now() + $4::interval,
	    attempts      = attempts + 1,
	    updated_at    = now()
	from next
	where j.id = next.id
	returning ` + qualify("j", jobColumns)
