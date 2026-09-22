package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func explainPlan(t *testing.T, sql string, args ...any) string {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx, "set local enable_seqscan = off")
	require.NoError(t, err)
	rows, err := tx.Query(ctx, "explain "+sql, args...)
	require.NoError(t, err)
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return strings.Join(lines, "\n")
}

func TestHotQueriesUseTheirIndexes(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	deadJob(t, ctx, s, "q")
	done := leaseOne(t, ctx, s, "done", "w")
	require.NoError(t, s.Ack(ctx, done.ID, *done.LeaseID))
	leased := leaseOne(t, ctx, s, "leased", "w")
	require.NotNil(t, leased.LeaseID)
	enqueueN(t, ctx, s, "q", 3, 0)
	future, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	require.Equal(t, store.StateScheduled, future.State)

	for _, tc := range []struct {
		name      string
		index     string
		allowSort bool
		sql       string
		args      []any
	}{
		{
			name:      "lease",
			index:     "jobs_lease_idx",
			allowSort: true,
			sql: `select id from jobs
			      where queue = any($1) and state = 'available'
			      order by priority desc, run_at
			      limit $2 for update skip locked`,
			args: []any{[]string{"q"}, 10},
		},
		{
			name:  "promote",
			index: "jobs_promote_idx",
			sql: `select id from jobs
			      where state = 'scheduled' and run_at <= now()
			      order by run_at
			      limit $1 for update skip locked`,
			args: []any{10},
		},
		{
			name:  "reap",
			index: "jobs_reap_idx",
			sql: `select id, queue, attempts, max_attempts from jobs
			      where state = 'leased' and lease_expires < now()
			      order by lease_expires
			      limit $1 for update skip locked`,
			args: []any{10},
		},
		{
			name:      "list dead",
			index:     "jobs_dead_idx",
			allowSort: true,
			sql: `select id from jobs
			      where queue = $1 and state = 'dead'
			        and ($2::timestamptz is null or (finished_at, id) > ($2::timestamptz, $3::uuid))
			      order by finished_at, id
			      limit $4`,
			args: []any{"q", nil, nil, 10},
		},
		{
			name:  "retention",
			index: "jobs_retention_idx",
			sql: `select id from jobs
			      where state in ('completed', 'cancelled') and finished_at < $1
			      order by finished_at
			      limit $2 for update skip locked`,
			args: []any{time.Now(), 10},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := explainPlan(t, tc.sql, tc.args...)
			require.Contains(t, plan, tc.index, "plan:\n%s", plan)
			require.NotContains(t, plan, "Seq Scan", "plan:\n%s", plan)
			if !tc.allowSort {
				require.NotContains(t, plan, "Sort", "plan:\n%s", plan)
			}
		})
	}
}
