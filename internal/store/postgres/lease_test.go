package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// enqueueN enqueues n jobs on queue with the given priority and returns them.
func enqueueN(t *testing.T, ctx context.Context, s *Store, queue string, n int, priority int16) []store.Job {
	t.Helper()
	out := make([]store.Job, 0, n)
	for range n {
		j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, Priority: priority})
		require.NoError(t, err)
		out = append(out, j)
	}
	return out
}

func TestLeaseOne(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	enq, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q"})
	require.NoError(t, err)

	before := time.Now()
	got, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "worker-1")
	require.NoError(t, err)
	require.Len(t, got, 1)

	j := got[0]
	require.Equal(t, enq.ID, j.ID)
	require.Equal(t, store.StateLeased, j.State)
	require.Equal(t, int32(1), j.Attempts, "attempts is counted at lease time")
	require.NotNil(t, j.LeaseID)
	require.NotNil(t, j.LeasedBy)
	require.Equal(t, "worker-1", *j.LeasedBy)
	require.NotNil(t, j.LeasedAt)
	require.NotNil(t, j.LeaseExpires)
	require.WithinDuration(t, before.Add(30*time.Second), *j.LeaseExpires, 2*time.Second)
	require.True(t, j.UpdatedAt.After(enq.UpdatedAt) || j.UpdatedAt.Equal(enq.UpdatedAt))

	// The row is now held; nothing is left to lease.
	again, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "worker-2")
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestLeaseLimitAndQueueFilter(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	enqueueN(t, ctx, s, "a", 5, 0)
	enqueueN(t, ctx, s, "b", 5, 0)

	got, err := s.Lease(ctx, []string{"a"}, 3, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, j := range got {
		require.Equal(t, "a", j.Queue)
		require.Equal(t, store.StateLeased, j.State)
	}

	// 2 left on a, 5 untouched on b.
	rest, err := s.Lease(ctx, []string{"a", "b"}, 100, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, rest, 7)
}

func TestLeaseSkipsScheduled(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)

	got, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "w")
	require.NoError(t, err)
	require.Empty(t, got, "scheduled rows are not leasable until the promoter moves them")
}

func TestLeaseOrdersByPriorityThenRunAt(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	low, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", Priority: 0})
	require.NoError(t, err)
	// Enqueued later but higher priority: must come out first.
	high, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", Priority: 10})
	require.NoError(t, err)
	// Same priority as low, enqueued later: must come after low.
	low2, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", Priority: 0})
	require.NoError(t, err)

	got, err := s.Lease(ctx, []string{"q"}, 10, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, high.ID, got[0].ID)
	require.Equal(t, low.ID, got[1].ID)
	require.Equal(t, low2.ID, got[2].ID)
}

func TestLeaseArgumentValidation(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	enqueueN(t, ctx, s, "q", 1, 0)

	got, err := s.Lease(ctx, nil, 10, time.Second, "w")
	require.NoError(t, err)
	require.Nil(t, got, "no queues means nothing to lease, not an error")

	got, err = s.Lease(ctx, []string{"q"}, 0, time.Second, "w")
	require.NoError(t, err)
	require.Nil(t, got)

	_, err = s.Lease(ctx, []string{"q"}, 1, 0, "w")
	require.Error(t, err, "a zero lease would expire the instant it was granted")

	// Nothing above should have consumed the job.
	got, err = s.Lease(ctx, []string{"q"}, 1, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 1)
}

// TestLeaseDisjoint pins the exclusivity claim in docs/semantics.md §15:
// concurrent Lease calls never return the same job. 50 goroutines each lease 4
// from a pool of 200 available jobs, all released at once so they genuinely
// overlap on the row locks. Every job must be handed out exactly once.
//
// The race detector cannot see a double lease (that would be a Postgres bug,
// not a Go data race); only the map assertion below can. -race is still worth
// running here because the result collection itself is shared state.
func TestLeaseDisjoint(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	const (
		workers = 50
		perCall = 4
		total   = workers * perCall // 200
	)
	enqueueN(t, ctx, s, "q", total, 0)

	var (
		mu     sync.Mutex
		seen   = make(map[uuid.UUID]int, total)
		errs   = make([]error, workers)
		counts = make([]int, workers)
		wg     sync.WaitGroup
		start  = make(chan struct{})
	)

	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // starting gun: everyone hits Postgres in the same instant
			jobs, err := s.Lease(ctx, []string{"q"}, perCall, 30*time.Second, fmt.Sprintf("w-%d", i))
			if err != nil {
				errs[i] = err // never require.* off the test goroutine
				return
			}
			counts[i] = len(jobs)
			mu.Lock()
			defer mu.Unlock()
			for _, j := range jobs {
				seen[j.ID]++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "worker %d", i)
	}
	for i, n := range counts {
		require.Equalf(t, perCall, n, "worker %d got %d jobs; with 200 available every caller should fill its limit", i, n)
	}
	require.Len(t, seen, total, "every job leased exactly once")
	for id, n := range seen {
		require.Equalf(t, 1, n, "job %s was leased %d times", id, n)
	}

	// And nothing was left behind or double-counted in the database either.
	var leased, available int
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) filter (where state = 'leased'), count(*) filter (where state = 'available') from jobs`,
	).Scan(&leased, &available))
	require.Equal(t, total, leased)
	require.Equal(t, 0, available)
	t.Logf("%d workers leased %d distinct jobs", workers, len(seen))
}

// TestPriority is the docs/semantics.md §15 row: a priority-10 job is leased
// before a hundred priority-0 jobs. The urgent job is enqueued last, so its
// run_at is the latest of the 101 and FIFO order would put it at the very
// back; priority is the only thing that can pull it to the front (§9).
func TestPriority(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	ordinary := enqueueN(t, ctx, s, "q", 100, 0)
	urgent, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", Priority: 10})
	require.NoError(t, err)

	// A one-job lease is the sharp form of the claim: of 101 leasable rows,
	// the single row the store chooses to hand out is the urgent one.
	got, err := s.Lease(ctx, []string{"q"}, 1, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, urgent.ID, got[0].ID)
	require.Equal(t, int16(10), got[0].Priority)

	// And the rest come out behind it, in run_at order.
	rest, err := s.Lease(ctx, []string{"q"}, 100, 30*time.Second, "w")
	require.NoError(t, err)
	require.Len(t, rest, len(ordinary))
	for i := 1; i < len(rest); i++ {
		require.False(t, rest[i].RunAt.Before(rest[i-1].RunAt),
			"priority-0 jobs are handed out oldest-first")
	}
}

// planNode is the part of EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) a cost test
// reads.
type planNode struct {
	NodeType     string     `json:"Node Type"`
	RelationName string     `json:"Relation Name"`
	Alias        string     `json:"Alias"`
	IndexName    string     `json:"Index Name"`
	ActualRows   float64    `json:"Actual Rows"`
	ActualLoops  float64    `json:"Actual Loops"`
	SharedHit    float64    `json:"Shared Hit Blocks"`
	SharedRead   float64    `json:"Shared Read Blocks"`
	Plans        []planNode `json:"Plans"`
}

// scansOf returns the scans of jobs under alias. The lease's candidate scans
// read jobs as "jobs"; the UPDATE's own lookups of the chosen rows read it as
// "j".
func scansOf(n planNode, alias string) []planNode {
	var out []planNode
	if n.RelationName == "jobs" && n.Alias == alias {
		out = append(out, n)
	}
	for _, child := range n.Plans {
		out = append(out, scansOf(child, alias)...)
	}
	return out
}

// explainLease runs the production lease statement under EXPLAIN ANALYZE in
// tx, checks that it still leased n jobs, and returns its candidate scans
// with the raw plan.
//
// A scan's actual rows are the rows it produced, over every loop: rows that
// SKIP LOCKED then passed over are among them, but index entries whose rows
// were leased since the last vacuum are not, since their heap visits find
// nothing. Buffers count both, so they are logged beside the rows. Actual Rows
// is a per-loop mean rounded to a whole row, so totals are good to half a row
// per loop.
func explainLease(t *testing.T, ctx context.Context, tx pgx.Tx, queues []string, n int) (scans []planNode, raw []byte) {
	t.Helper()
	require.NoError(t, tx.QueryRow(ctx, "explain (analyze, buffers, format json) "+leaseSQL,
		queues, n, "w", time.Minute).Scan(&raw))
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal(raw, &plans))
	require.Len(t, plans, 1)
	require.Equal(t, float64(n), plans[0].Plan.ActualRows, "the lease still leases n jobs")
	scans = scansOf(plans[0].Plan, "jobs")
	require.NotEmpty(t, scans, "no candidate scan of jobs; plan:\n%s", raw)
	var rows, blocks float64
	for _, s := range scans {
		rows += s.ActualRows * s.ActualLoops
		blocks += s.SharedHit + s.SharedRead
	}
	t.Logf("candidate scans produced %v rows and touched %v shared blocks", rows, blocks)
	return scans, raw
}

// rowsRead totals the rows the scans produced, over every loop.
func rowsRead(scans []planNode) float64 {
	total := 0.0
	for _, s := range scans {
		total += s.ActualRows * s.ActualLoops
	}
	return total
}

// requireLeaseIndex checks that every candidate scan walks jobs_lease_idx,
// the plan the bound depends on, as the planner chose it with sequential
// scans allowed.
func requireLeaseIndex(t *testing.T, scans []planNode, raw []byte) {
	t.Helper()
	for _, s := range scans {
		require.Equal(t, "Index Scan", s.NodeType, "plan:\n%s", raw)
		require.Equal(t, "jobs_lease_idx", s.IndexName, "plan:\n%s", raw)
	}
}

// seedBacklog adds 6,000 available jobs across queues a and b, analyzed so
// the planner sees the backlog it would see in production.
func seedBacklog(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		insert into jobs (id, queue, payload, state, run_at)
		select gen_random_uuid(), case when i % 2 = 0 then 'a' else 'b' end, '{}', 'available',
		       now() - make_interval(secs => i)
		from generate_series(1, 6000) as i`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `analyze jobs`)
	require.NoError(t, err)
}

// A lease's candidates are at most n unlocked rows from each subscribed queue,
// however deep the backlog. Until Session 11 it read and sorted every
// available row in the subscribed queues on every call, so its cost grew with
// the backlog it was meant to drain.
func TestLeaseCostDoesNotGrowWithTheBacklog(t *testing.T) {
	ctx := ctxT(t)
	newStore(t)
	seedBacklog(t, ctx)

	queues, n := []string{"a", "b", "c"}, 10
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) //nolint:errcheck // EXPLAIN ANALYZE ran the UPDATE; undo it
	scans, raw := explainLease(t, ctx, tx, queues, n)

	read := rowsRead(scans)
	require.LessOrEqual(t, read, float64(len(queues)*n),
		"a lease of %d read %v candidate rows; plan:\n%s", n, read, raw)
	requireLeaseIndex(t, scans, raw)
}

// Rows another lessor holds are skipped, never waited for or leased twice. The
// scan still produces them before SKIP LOCKED passes over them, so the bound
// is n per queue plus the rows held, which is still not the backlog.
func TestLeaseCostSkipsLockedCandidates(t *testing.T) {
	ctx := ctxT(t)
	newStore(t)
	seedBacklog(t, ctx)

	// Another lessor holds queue a's five best candidates until the test ends.
	other, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer other.Rollback(ctx) //nolint:errcheck // releases the locks
	rows, err := other.Query(ctx, `
		select id from jobs where queue = 'a' and state = 'available'
		order by priority desc, run_at limit 5 for update`)
	require.NoError(t, err)
	held, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	require.NoError(t, err)
	require.Len(t, held, 5)

	queues, n := []string{"a", "b", "c"}, 10
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) //nolint:errcheck // EXPLAIN ANALYZE ran the UPDATE; undo it
	scans, raw := explainLease(t, ctx, tx, queues, n)

	var taken int
	require.NoError(t, tx.QueryRow(ctx,
		`select count(*) from jobs where id = any($1) and state = 'leased'`, held).Scan(&taken))
	require.Zero(t, taken, "the lease took rows another transaction holds")
	read := rowsRead(scans)
	require.LessOrEqual(t, read, float64(len(queues)*n+len(held)),
		"a lease of %d past %d held rows read %v candidate rows; plan:\n%s", n, len(held), read, raw)
	requireLeaseIndex(t, scans, raw)
}

// Subscribing to a queue twice must not lease fewer distinct jobs: a
// transaction does not skip rows it has itself locked.
func TestLeaseWithARepeatedQueueLeasesDistinctJobs(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	enqueueN(t, ctx, s, "a", 3, 0)

	got, err := s.Lease(ctx, []string{"a", "a"}, 3, time.Minute, "w")
	require.NoError(t, err)
	ids := map[uuid.UUID]bool{}
	for _, j := range got {
		ids[j.ID] = true
	}
	require.Len(t, ids, 3)
}

// The batch is the best n across every subscribed queue, not n from the first.
func TestLeaseOrdersAcrossQueues(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	a5 := enqueueN(t, ctx, s, "a", 1, 5)[0]
	enqueueN(t, ctx, s, "a", 1, 1)
	b3 := enqueueN(t, ctx, s, "b", 1, 3)[0]
	enqueueN(t, ctx, s, "b", 1, 2)

	got, err := s.Lease(ctx, []string{"a", "b"}, 2, time.Minute, "w")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, a5.ID, got[0].ID)
	require.Equal(t, b3.ID, got[1].ID)
}
