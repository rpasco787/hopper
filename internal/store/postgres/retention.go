package postgres

import (
	"context"
	"time"
)

// DeleteFinished is the janitor's statement from docs/semantics.md §10.
//
// Batched with the same CTE shape as PromoteDue, for the same two reasons: an
// unbounded DELETE would hold a lock on every matching row for as long as the
// whole sweep takes and write one enormous WAL record, and SKIP LOCKED keeps
// several brokers' janitors from queueing behind each other. As there, the CTE
// is also how a DELETE gets a LIMIT at all.
//
// The state list is the guard that matters, and 'dead' is deliberately absent:
// dead rows are the evidence an operator inspects, and only Purge removes
// them. Nothing here looks at queue, so one sweep covers the whole table.
//
// ORDER BY finished_at matches jobs_retention_idx and drains oldest-first, so
// a janitor that can never quite keep up still bounds how old the oldest
// retained row is, instead of deleting an arbitrary sample and leaving ancient
// rows behind forever.
func (s *Store) DeleteFinished(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		with old as (
			select id from jobs
			where state in ('completed', 'cancelled') and finished_at < $1
			order by finished_at
			limit $2
			for update skip locked
		)
		delete from jobs j using old where j.id = old.id`,
		cutoff, limit)
	if err != nil {
		return 0, wrap("delete finished", err)
	}
	return int(tag.RowsAffected()), nil
}
