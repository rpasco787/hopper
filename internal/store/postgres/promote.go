package postgres

import (
	"context"
)

// PromoteDue is the promoter's statement from docs/semantics.md §4.
//
// One statement, so one implicit transaction: the CTE locks the due rows and
// the UPDATE ... FROM rewrites exactly those. It is the same shape as Lease
// and for the same reason — SKIP LOCKED lets several brokers run promoters
// without any of them blocking on another's batch, and a row another promoter
// has already taken is simply not in this batch. The CTE also exists because
// Postgres has no LIMIT on UPDATE; the batch has to be chosen by a SELECT.
//
// The UPDATE carries no state guard of its own, exactly like Lease. It does
// not need one: the CTE selected those rows under FOR UPDATE, so nothing can
// move them between the select and the write. A Cancel racing us blocks on
// that lock, then re-evaluates its own guard against the row we committed —
// 'available', which is still cancellable — and does the right thing.
//
// ORDER BY run_at matches jobs_promote_idx, so the longest-overdue jobs go
// first and a backlog is promoted in due order rather than in whatever order
// the heap hands rows back.
func (s *Store) PromoteDue(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		with due as (
			select id from jobs
			where state = 'scheduled' and run_at <= now()
			order by run_at
			limit $1
			for update skip locked
		)
		update jobs j
		set state = 'available', updated_at = now()
		from due
		where j.id = due.id`,
		limit)
	if err != nil {
		return 0, wrap("promote due", err)
	}
	return int(tag.RowsAffected()), nil
}
