//go:build chaos

package chaos

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/internal/store"
)

// shardQueue is the queue every job in the shard-down test goes to. Keys are
// routed with it (store.KeyShard), so it must be the queue the jobs carry.
const shardQueue = "sleep"

// enqueueKeyed adds n sleep jobs of ms each to the run, keyed <run>-<i> for i
// from from, in batches, and returns their keys. Unlike enqueue it takes the
// run's name rather than making one, so a test can route its keys.
func (s *stack) enqueueKeyed(cl *client.Client, run string, from, n, ms int) []string {
	s.t.Helper()
	payload := json.RawMessage(fmt.Sprintf(`{"ms":%d}`, ms))
	keys := make([]string, 0, n)
	for start := from; start < from+n; start += 500 {
		batch := make([]client.Job, 0, 500)
		for i := start; i < min(start+500, from+n); i++ {
			key := fmt.Sprintf("%s-%d", run, i)
			keys = append(keys, key)
			batch = append(batch, client.Job{Queue: shardQueue, Payload: payload, MaxAttempts: 20, IdempotencyKey: key})
		}
		ctx, cancel := s.within(rpcLimit)
		results, err := cl.EnqueueBatch(ctx, batch)
		cancel()
		require.NoError(s.t, err)
		for _, r := range results {
			require.False(s.t, r.Deduplicated, "run %s is not fresh", run)
		}
	}
	return keys
}

// keysFor returns a generator of fresh keys <prefix>-<j> that all route to
// shard i of two.
func keysFor(prefix string, i int) func() string {
	j := 0
	return func() string {
		for {
			key := fmt.Sprintf("%s-%d", prefix, j)
			j++
			if store.KeyShard(shardQueue, key, 2) == i {
				return key
			}
		}
	}
}

// completedOn counts the completed jobs on one shard. It never fails the test.
func (s *stack) completedOn(shard int) (int, error) {
	ctx, cancel := s.within(dbLimit)
	defer cancel()
	var n int
	err := s.dbs[shard].QueryRow(ctx, `select count(*) from jobs where state = 'completed'`).Scan(&n)
	return n, err
}

// latencies summarizes calls' durations in milliseconds.
type latencies struct {
	P50, P99, Max float64
}

func summarize(ds []time.Duration) latencies {
	if len(ds) == 0 {
		return latencies{}
	}
	ds = slices.Clone(ds)
	slices.Sort(ds)
	at := func(q float64) time.Duration {
		i := int(q*float64(len(ds)) + 0.999999) // nearest rank
		return ds[max(min(i, len(ds))-1, 0)]
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return latencies{P50: ms(at(0.50)), P99: ms(at(0.99)), Max: ms(ds[len(ds)-1])}
}

// callLog is what one kind of call saw during the outage.
type callLog struct {
	Calls       int            `json:"calls"`
	OK          int            `json:"ok"`
	Codes       map[string]int `json:"failures_by_code"`
	Latency     latencies      `json:"latency_ms"`
	FirstErrors []string       `json:"first_errors,omitempty"`
	durations   []time.Duration
}

func (c *callLog) record(took time.Duration, err error) {
	c.Calls++
	c.durations = append(c.durations, took)
	if err == nil {
		c.OK++
		return
	}
	if c.Codes == nil {
		c.Codes = map[string]int{}
	}
	c.Codes[status.Code(err).String()]++
	if len(c.FirstErrors) < 5 {
		c.FirstErrors = append(c.FirstErrors, err.Error())
	}
}

// outageEvidence is outage.json: what the test measured while shard 1 was
// down, and how long shard 1 took to complete a job once it was back.
type outageEvidence struct {
	Run                   string  `json:"run"`
	BacklogShard0         int     `json:"backlog_shard0"`
	BacklogShard1         int     `json:"backlog_shard1"`
	OutageSeconds         float64 `json:"outage_seconds"`
	SampledSeconds        float64 `json:"sampled_seconds"`
	KeyedShard0           callLog `json:"keyed_shard0"`
	KeyedShard1           callLog `json:"keyed_shard1"`
	Unkeyed               callLog `json:"unkeyed"`
	UnkeyedSuccessRate    float64 `json:"unkeyed_success_rate"`
	UnkeyedOffShard0      int     `json:"unkeyed_successes_not_on_shard0"`
	Shard0CompletedAtKill int     `json:"shard0_completed_at_kill"`
	Shard0CompletedAtEnd  int     `json:"shard0_completed_at_restart"`
	Shard0CompletedRise   int     `json:"shard0_completed_rise"`
	// RestartToFirstShard1Completed is from shard 1's postmaster start to the
	// first job it completed after it, both on shard 1's clock.
	RestartToFirstShard1Completed float64 `json:"restart_to_first_shard1_completed_seconds"`
	// StartCallToDatabase is from the docker start call until the suite's
	// pool reached every shard again, on the host's clock.
	StartCallToDatabase float64        `json:"start_call_to_database_seconds"`
	Repeats             map[string]int `json:"repeats_by_cause"`
	Summary             string         `json:"summary"`
}

// TestOneShardDownServesTheRest kills one of two shards' PostgreSQL for 30 s
// under load (docs/design/sharding.md, "Failure isolation"). While it is down:
// keyed enqueues for shard 1 fail UNAVAILABLE; keyed enqueues for shard 0
// succeed, with p99 under 1 s; unkeyed enqueues land on shard 0, at least 90%
// of them succeeding and the rest failing UNAVAILABLE; and jobs on shard 0
// keep completing. After shard 1 returns, every acknowledged job completes,
// accounting finds none lost, and every repeated start is explained.
func TestOneShardDownServesTheRest(t *testing.T) {
	const (
		outageFor = 30 * time.Second
		sampleFor = 25 * time.Second
		tick      = 100 * time.Millisecond
		p99Limit  = time.Second
	)
	s := up(t, "shard-down", "HOPPER_CHAOS_SHARDS=2", "HOPPER_CHAOS_WORKER_BROKER=brokers:9090")
	require.Len(t, s.dbs, 2, "the stack must run two shards")
	run := "shard-down"
	cl := s.client(s.brokerAddr)
	ev := outageEvidence{Run: run}

	// 1. A backlog on both shards: 2,000 keyed jobs of 20 ms.
	keys := s.enqueueKeyed(cl, run, 0, 2000, 20)
	for _, k := range keys {
		if store.KeyShard(shardQueue, k, 2) == 0 {
			ev.BacklogShard0++
		} else {
			ev.BacklogShard1++
		}
	}
	require.Positive(t, ev.BacklogShard0, "no backlog on shard 0")
	require.Positive(t, ev.BacklogShard1, "no backlog on shard 1")
	s.waitCompleted(run, 200, 2*time.Minute)

	// 2. Kill shard 1's database.
	pg1 := s.containers("postgres-1")[0]
	var err error
	ev.Shard0CompletedAtKill, err = s.completedOn(0)
	require.NoError(t, err)
	s.kill(pg1)
	outage := time.Now()

	// 3. During the outage, three producers sample every 100 ms for 25 s.
	var (
		mu         sync.Mutex
		unkeyedIDs []string
		wg         sync.WaitGroup
	)
	sample := func(c *callLog, job func() client.Job, onOK func(client.Result)) {
		defer wg.Done()
		for next := time.Now(); time.Since(outage) < sampleFor && s.ctx.Err() == nil; next = next.Add(tick) {
			time.Sleep(time.Until(next))
			ctx, cancel := s.within(rpcLimit)
			began := time.Now()
			r, err := cl.Enqueue(ctx, job())
			took := time.Since(began)
			cancel()
			mu.Lock()
			c.record(took, err)
			if err == nil && onOK != nil {
				onOK(r)
			}
			mu.Unlock()
		}
	}
	payload := json.RawMessage(`{"ms":20}`)
	key0, key1 := keysFor(run+"-o0", 0), keysFor(run+"-o1", 1)
	wg.Add(3)
	// a) keyed, shard 0: record latency; all must succeed.
	go sample(&ev.KeyedShard0, func() client.Job {
		return client.Job{Queue: shardQueue, Payload: payload, MaxAttempts: 20, IdempotencyKey: key0()}
	}, nil)
	// b) keyed, shard 1: all must fail UNAVAILABLE within rpcLimit. Their
	// keys carry the run prefix, so accounting expects none of them.
	go sample(&ev.KeyedShard1, func() client.Job {
		return client.Job{Queue: shardQueue, Payload: payload, MaxAttempts: 20, IdempotencyKey: key1()}
	}, nil)
	// c) unkeyed: every success must be on shard 0.
	go sample(&ev.Unkeyed, func() client.Job {
		return client.Job{Queue: shardQueue, Payload: payload, MaxAttempts: 20}
	}, func(r client.Result) { unkeyedIDs = append(unkeyedIDs, r.ID.String()) })
	wg.Wait()
	ev.SampledSeconds = time.Since(outage).Seconds()
	for _, c := range []*callLog{&ev.KeyedShard0, &ev.KeyedShard1, &ev.Unkeyed} {
		c.Latency = summarize(c.durations)
	}

	// d) shard 0's completed count over the outage.
	time.Sleep(time.Until(outage.Add(outageFor)))
	ev.Shard0CompletedAtEnd, err = s.completedOn(0)
	require.NoError(t, err)
	ev.Shard0CompletedRise = ev.Shard0CompletedAtEnd - ev.Shard0CompletedAtKill

	// 4. Bring shard 1 back and wait.
	ev.OutageSeconds = time.Since(outage).Seconds()
	restarted := time.Now()
	s.start(pg1)
	s.waitDatabase()
	ev.StartCallToDatabase = time.Since(restarted).Seconds()
	okShard0 := ev.KeyedShard0.OK
	for _, id := range unkeyedIDs {
		if shard, err := store.ShardOf(uuid.MustParse(id), 2); err != nil || shard != 0 {
			ev.UnkeyedOffShard0++
		}
	}
	if ev.Unkeyed.Calls > 0 {
		ev.UnkeyedSuccessRate = float64(ev.Unkeyed.OK) / float64(ev.Unkeyed.Calls)
	}
	// The evidence is written before the assertions, so a failed run keeps it.
	writeOutage := func() {
		b, err := json.MarshalIndent(ev, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(s.dir, "outage.json"), append(b, '\n'), 0o644))
	}
	writeOutage()
	t.Logf("outage %.1fs: keyed shard 0 %d/%d ok (p50 %.1fms p99 %.1fms max %.1fms); keyed shard 1 %d/%d failed %v (max %.1fms); "+
		"unkeyed %d/%d ok (%.1f%%) %v; shard 0 completed +%d",
		ev.OutageSeconds, ev.KeyedShard0.OK, ev.KeyedShard0.Calls, ev.KeyedShard0.Latency.P50, ev.KeyedShard0.Latency.P99, ev.KeyedShard0.Latency.Max,
		ev.KeyedShard1.Calls-ev.KeyedShard1.OK, ev.KeyedShard1.Calls, ev.KeyedShard1.Codes, ev.KeyedShard1.Latency.Max,
		ev.Unkeyed.OK, ev.Unkeyed.Calls, 100*ev.UnkeyedSuccessRate, ev.Unkeyed.Codes, ev.Shard0CompletedRise)

	// 3's claims.
	require.Positive(t, ev.KeyedShard0.Calls)
	require.Equal(t, ev.KeyedShard0.Calls, ev.KeyedShard0.OK, "keyed enqueues for shard 0 failed during the outage: %v", ev.KeyedShard0.FirstErrors)
	require.Less(t, ev.KeyedShard0.Latency.P99, float64(p99Limit.Milliseconds()), "keyed enqueues for shard 0: p99 over %s", p99Limit)
	require.Positive(t, ev.KeyedShard1.Calls)
	require.Zero(t, ev.KeyedShard1.OK, "a keyed enqueue for the dead shard succeeded")
	require.Equal(t, map[string]int{codes.Unavailable.String(): ev.KeyedShard1.Calls}, ev.KeyedShard1.Codes,
		"every keyed enqueue for shard 1 must fail UNAVAILABLE: %v", ev.KeyedShard1.FirstErrors)
	require.Less(t, ev.KeyedShard1.Latency.Max, float64(rpcLimit.Milliseconds()))
	require.Positive(t, ev.Unkeyed.Calls)
	if failed := ev.Unkeyed.Calls - ev.Unkeyed.OK; failed > 0 {
		require.Equal(t, map[string]int{codes.Unavailable.String(): failed}, ev.Unkeyed.Codes,
			"every failed unkeyed enqueue must be UNAVAILABLE: %v", ev.Unkeyed.FirstErrors)
	}
	require.GreaterOrEqual(t, ev.UnkeyedSuccessRate, 0.90, "unkeyed enqueues: under 90%% succeeded while one shard was down")
	require.Zero(t, ev.UnkeyedOffShard0, "an unkeyed enqueue succeeded on a shard other than 0 while shard 1 was down")
	require.GreaterOrEqual(t, ev.Shard0CompletedRise, 100, "shard 0 stopped completing jobs while shard 1 was down")

	// 4, continued: every acknowledged job completes. The run's rows are the
	// backlog and the shard-0 keyed enqueues; the unkeyed ones are checked by
	// id on shard 0.
	s.waitCompleted(run, len(keys)+okShard0, 5*time.Minute)
	s.eventually(func() bool {
		ctx, cancel := s.within(dbLimit)
		defer cancel()
		var n int
		err := s.dbs[0].QueryRow(ctx, `select count(*) from jobs where id = any($1::uuid[]) and state = 'completed'`, unkeyedIDs).Scan(&n)
		return err == nil && n == len(unkeyedIDs)
	}, 2*time.Minute, 500*time.Millisecond, "not every unkeyed job enqueued during the outage completed on shard 0")
	{
		ctx, cancel := s.within(dbLimit)
		var first *float64
		err := s.dbs[1].QueryRow(ctx, `select extract(epoch from min(finished_at) - pg_postmaster_start_time())::float8
		                               from jobs where state = 'completed' and finished_at > pg_postmaster_start_time()`).Scan(&first)
		cancel()
		require.NoError(t, err)
		require.NotNil(t, first, "shard 1 completed no job after it came back")
		ev.RestartToFirstShard1Completed = *first
	}

	// 5. Accounting over both shards.
	rep := s.reconcile(run)
	ev.Summary = summary(rep)
	ev.Repeats = map[string]int{}
	for _, r := range rep.Repeats {
		ev.Repeats[cmp.Or(r.Cause, "unexplained")]++
	}
	// 6. Evidence, now complete.
	writeOutage()
	t.Logf("run %s: %s; shard 1 completed its first job %.2fs after its postmaster started", run, ev.Summary, ev.RestartToFirstShard1Completed)
	require.Equal(t, len(keys)+okShard0, rep.Jobs, "the run's rows are the backlog and the shard-0 keyed enqueues; a shard-1 key must have no row")
	require.Equal(t, rep.Jobs, rep.Completed, "every acknowledged job completes")
	require.Empty(t, rep.AttemptProblems(), "no job lost, and every repeated start explained")
}
