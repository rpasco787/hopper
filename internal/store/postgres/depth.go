package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

var _ store.QueueDepthSource = (*Store)(nil)

// QueueDepths uses one statement so all four branches share one snapshot. Each
// branch repeats its partial index predicate exactly, while completed and
// cancelled terminal rows are excluded because they do not describe work owed
// or dead-letter evidence. Each branch still scans every row in its state, so
// cost grows with the non-terminal backlog. Cost is unmeasured beyond Task 12's
// burst; Session 11 owns larger-scale measurement.
func (s *Store) QueueDepths(ctx context.Context) ([]store.QueueDepth, error) {
	rows, err := s.pool.Query(ctx, `
		select queue, 'available' as state, count(*) as count from jobs where state = 'available' group by queue
		union all
		select queue, 'scheduled', count(*) from jobs where state = 'scheduled' group by queue
		union all
		select queue, 'leased', count(*) from jobs where state = 'leased' group by queue
		union all
		select queue, 'dead', count(*) from jobs where state = 'dead' group by queue`)
	if err != nil {
		return nil, fmt.Errorf("store: queue depths: %w", err)
	}
	depths, err := pgx.CollectRows(rows, pgx.RowToStructByName[store.QueueDepth])
	if err != nil {
		return nil, fmt.Errorf("store: queue depths: %w", err)
	}
	return depths, nil
}
