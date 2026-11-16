package postgres

import (
	"context"
	"fmt"
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
		indexes   []string
		allowSort bool
		sql       string
		args      []any
	}{
		{
			// The sort is over at most n candidates per queue;
			// TestLeaseCostDoesNotGrowWithTheBacklog and
			// TestLeaseCostSkipsLockedCandidates bound what it reads.
			name:      "lease",
			indexes:   []string{"jobs_lease_idx"},
			allowSort: true,
			sql:       leaseSQL,
			args:      []any{[]string{"q", "r"}, 10, "w", time.Minute},
		},
		{
			name:    "promote",
			indexes: []string{"jobs_promote_idx"},
			sql: `select id from jobs
			      where state = 'scheduled' and run_at <= now()
			      order by run_at
			      limit $1 for update skip locked`,
			args: []any{10},
		},
		{
			name:    "reap",
			indexes: []string{"jobs_reap_idx"},
			sql: `select id, queue, attempts, max_attempts from jobs
			      where state = 'leased' and lease_expires < now()
			      order by lease_expires
			      limit $1 for update skip locked`,
			args: []any{10},
		},
		{
			name:      "list dead",
			indexes:   []string{"jobs_dead_idx"},
			allowSort: true,
			sql: `select id from jobs
			      where queue = $1 and state = 'dead'
			        and ($2::timestamptz is null or (finished_at, id) > ($2::timestamptz, $3::uuid))
			      order by finished_at, id
			      limit $4`,
			args: []any{"q", nil, nil, 10},
		},
		{
			name:    "retention",
			indexes: []string{"jobs_retention_idx"},
			sql: `select id from jobs
			      where state in ('completed', 'cancelled') and finished_at < $1
			      order by finished_at
			      limit $2 for update skip locked`,
			args: []any{time.Now(), 10},
		},
		{
			name:      "depth",
			indexes:   []string{"jobs_lease_idx", "jobs_promote_idx", "jobs_reap_idx", "jobs_dead_idx"},
			allowSort: true,
			sql: `select queue, 'available' as state, count(*) as count from jobs where state = 'available' group by queue
			      union all
			      select queue, 'scheduled', count(*) from jobs where state = 'scheduled' group by queue
			      union all
			      select queue, 'leased', count(*) from jobs where state = 'leased' group by queue
			      union all
			      select queue, 'dead', count(*) from jobs where state = 'dead' group by queue`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := explainPlan(t, tc.sql, tc.args...)
			for _, index := range tc.indexes {
				require.Contains(t, plan, index, "plan:\n%s", plan)
			}
			require.NotContains(t, plan, "Seq Scan", "plan:\n%s", plan)
			if !tc.allowSort {
				require.NotContains(t, plan, "Sort", "plan:\n%s", plan)
			}
		})
	}
}

// explainPrepared plans sql as a named prepared statement under mode
// (force_custom_plan or force_generic_plan) on a fresh connection, so the
// prepared statement never reaches the shared pool. args are SQL literals.
func explainPrepared(t *testing.T, mode, sql, args string) string {
	t.Helper()
	ctx := ctxT(t)
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)
	simple := pgx.QueryExecModeSimpleProtocol
	_, err = conn.Exec(ctx, "set plan_cache_mode = "+mode, simple)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "prepare probe as "+sql, simple)
	require.NoError(t, err)
	rows, err := conn.Query(ctx, "explain (costs off) execute probe("+args+")", simple)
	require.NoError(t, err)
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return strings.Join(lines, "\n")
}

// TestPerJobUpdatesUseThePrimaryKeyUnderStaleStatistics: statistics gathered
// while nothing was leased say jobs_reap_idx is empty. A plan that believes
// them finds one id by walking every leased row (docs/scaling.md H3a).
func TestPerJobUpdatesUseThePrimaryKeyUnderStaleStatistics(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	_, err := pool.Exec(ctx, `insert into jobs (queue, payload, state, run_at)
		select 'q', '{}', 'available', now() from generate_series(1, 20000)`)
	require.NoError(t, err)
	// VACUUM refuses to run in a transaction block; the simple protocol sends
	// it bare.
	_, err = pool.Exec(ctx, "vacuum analyze jobs", pgx.QueryExecModeSimpleProtocol)
	require.NoError(t, err)
	leased, err := s.Lease(ctx, []string{"q"}, 128, time.Minute, "w")
	require.NoError(t, err)
	require.Len(t, leased, 128)
	id, lease := leased[0].ID, *leased[0].LeaseID

	for _, tc := range []struct{ name, sql, args string }{
		{"ack", ackSQL, fmt.Sprintf("'%s', '%s'", id, lease)},
		{"heartbeat", extendSQL, fmt.Sprintf("'%s', '%s', '1 minute'", id, lease)},
	} {
		for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				plan := explainPrepared(t, mode, tc.sql, tc.args)
				require.Contains(t, plan, "jobs_pkey", "plan:\n%s", plan)
				require.NotContains(t, plan, "jobs_reap_idx", "plan:\n%s", plan)
			})
		}
	}
}
