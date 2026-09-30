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

	// The ORDER BY and WHERE here match jobs_lease_idx (queue, priority desc,
	// run_at) where state = 'available' term for term, so the planner walks
	// the index and stops after n rows instead of sorting the queue.
	rows, err := s.pool.Query(ctx, `
		with next as (
			select id from jobs
			where queue = any($1) and state = 'available'
			order by priority desc, run_at
			limit $2
			for update skip locked
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
		returning `+qualify("j", jobColumns),
		queues, n, workerID, leaseDur,
	)
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
