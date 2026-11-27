package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rpasco787/hopper/client"
	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
	"github.com/rpasco787/hopper/internal/store/sharded"
	"github.com/rpasco787/hopper/internal/sweep"
	workersdk "github.com/rpasco787/hopper/worker"
)

const testShards = 2

// shardDatabases creates one fresh, migrated database per shard in the
// shared container, claims each one's identity as cmd/broker does, and drops
// them all at cleanup. It returns a pool on each, for the test's own checks.
func shardDatabases(t *testing.T, n int) []*pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("sh%d", i)
		_, err := pool.Exec(ctx, "drop database if exists "+names[i]+" with (force)")
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "create database "+names[i])
		require.NoError(t, err)
	}
	// Registered first, so it runs after every broker, worker and pool that
	// uses the databases has closed.
	t.Cleanup(func() {
		for _, name := range names {
			if _, err := pool.Exec(context.Background(), "drop database if exists "+name+" with (force)"); err != nil {
				t.Errorf("drop %s: %v", name, err)
			}
		}
	})
	pools := make([]*pgxpool.Pool, n)
	for i, name := range names {
		pools[i] = shardPool(t, name)
		require.NoError(t, postgres.Migrate(ctx, pools[i]))
		require.NoError(t, postgres.New(pools[i]).WithShard(i).ClaimShard(ctx, i, n))
	}
	return pools
}

// shardPool opens a pool on one of the container's databases.
func shardPool(t *testing.T, database string) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config().Copy()
	cfg.ConnConfig.Database = database
	cfg.MaxConns = 16
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

// startShardedBroker serves one broker the way cmd/broker composes it over
// several databases: its own pool and store per shard, a sharded store over
// them, wakeups on every shard, the sweeps per shard, and a prober per shard.
func startShardedBroker(t *testing.T, n int) string {
	t.Helper()
	shards := make([]sharded.Shard, n)
	stores := make([]*postgres.Store, n)
	for i := range n {
		stores[i] = postgres.New(shardPool(t, fmt.Sprintf("sh%d", i))).WithShard(i)
		shards[i] = stores[i]
	}
	sh, err := sharded.New(shards, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := confirmationConfig(t, sh, broker.Config{
		LeaseDuration: 30 * time.Second, PollInterval: 10 * time.Millisecond,
		Wakeups: sh, Listeners: sh.Listeners(),
	})
	srv := broker.NewServer(sh, cfg)
	go func() { _ = srv.Serve(lis) }()

	ctx, cancel := context.WithCancel(context.Background())
	wakeups := make(chan error, 1)
	go func() { wakeups <- srv.RunWakeups(ctx) }()
	watched := make(chan struct{})
	probes := make([]sharded.Probe, n)
	for i := range n {
		cfg := pool.Config().ConnConfig.Copy()
		cfg.Database = fmt.Sprintf("sh%d", i)
		p := postgres.NewProber(cfg)
		probes[i] = p.Probe
		t.Cleanup(p.Close) // runs after the watcher below has stopped
	}
	go func() { defer close(watched); sh.Watch(ctx, probes, 500*time.Millisecond, time.Second) }()
	t.Cleanup(func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, srv.Shutdown(shutdownCtx))
		cancel()
		if err := <-wakeups; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("wakeups stopped: %v", err)
		}
		<-watched
	})
	for _, s := range stores {
		for _, loop := range sweep.All(s, 24*time.Hour) {
			startMaintenance(t, loop)
		}
	}
	return lis.Addr().String()
}

func rawClientAt(t *testing.T, addr string) hopperv1.HopperServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return hopperv1.NewHopperServiceClient(conn)
}

// TestShardedBrokersEndToEnd: two brokers over two shards, with the Go client
// and worker SDK, from enqueue through completion, listing, cancel and dedup.
func TestShardedBrokersEndToEnd(t *testing.T) {
	ctx := ctxT(t)
	pools := shardDatabases(t, testShards)
	addrs := []string{startShardedBroker(t, testShards), startShardedBroker(t, testShards)}
	producers := []*client.Client{producerAt(t, addrs[0]), producerAt(t, addrs[1])}

	// Enqueue: 200 keyed and 200 unkeyed, half through each broker, then one
	// batch of 100 mixed items.
	keyed := map[string]uuid.UUID{}
	unkeyed := map[uuid.UUID]bool{}
	sleep := json.RawMessage(`{"ms":5}`)
	for i := range 200 {
		key := fmt.Sprintf("key-%d", i)
		res, err := producers[i/100].Enqueue(ctx, client.Job{Queue: "keyed", Payload: json.RawMessage(`{}`), IdempotencyKey: key})
		require.NoError(t, err)
		require.False(t, res.Deduplicated)
		keyed[key] = res.ID
	}
	for i := range 200 {
		res, err := producers[i%2].Enqueue(ctx, client.Job{Queue: "sleep", Payload: sleep})
		require.NoError(t, err)
		unkeyed[res.ID] = true
	}
	batch := make([]client.Job, 100)
	for i := range batch {
		batch[i] = client.Job{Queue: "sleep", Payload: sleep}
		if i%2 == 0 {
			batch[i].IdempotencyKey = fmt.Sprintf("batch-%d", i)
		}
	}
	results, err := producers[0].EnqueueBatch(ctx, batch)
	require.NoError(t, err)
	require.Len(t, results, len(batch))
	batchKeyed := map[string]uuid.UUID{}
	for i, res := range results {
		require.False(t, res.Deduplicated)
		if batch[i].IdempotencyKey != "" {
			batchKeyed[batch[i].IdempotencyKey] = res.ID
		} else {
			unkeyed[res.ID] = true
		}
	}

	// The spread: a keyed job is on its key's shard, and each database holds
	// a fair share of the 500 rows, each on the shard its ID names.
	for key, id := range keyed {
		got, err := store.ShardOf(id, testShards)
		require.NoError(t, err)
		require.Equal(t, store.KeyShard("keyed", key, testShards), got, key)
	}
	for key, id := range batchKeyed {
		got, err := store.ShardOf(id, testShards)
		require.NoError(t, err)
		require.Equal(t, store.KeyShard("sleep", key, testShards), got, key)
	}
	for i, p := range pools {
		rows, err := p.Query(ctx, "select id from jobs")
		require.NoError(t, err)
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.GreaterOrEqual(t, len(ids), 175, "shard %d holds %d of 500", i, len(ids))
		require.LessOrEqual(t, len(ids), 325, "shard %d holds %d of 500", i, len(ids))
		for _, id := range ids {
			got, err := store.ShardOf(id, testShards)
			require.NoError(t, err)
			require.Equal(t, i, got, "job %s is in shard %d's database", id, i)
		}
	}

	// Workers: one per broker, until every job is completed where it lives.
	handle := func(ctx context.Context, job workersdk.Job) error {
		var p struct{ MS int }
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return err
		}
		select {
		case <-time.After(time.Duration(p.MS) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i, addr := range addrs {
		w := workersdk.New(addr, workersdk.Options{
			ID: fmt.Sprintf("worker-%d", i), Concurrency: 8, ShutdownTimeout: 5 * time.Second,
			Logger: slog.New(slog.DiscardHandler),
		})
		w.Handle("keyed", handle)
		w.Handle("sleep", handle)
		startSDKWorker(t, w)
	}
	require.Eventually(t, func() bool {
		completed := 0
		for _, p := range pools {
			var n int
			if err := p.QueryRow(ctx, "select count(*) from jobs where state = 'completed'").Scan(&n); err != nil {
				return false
			}
			completed += n
		}
		return completed == 500
	}, 30*time.Second, 20*time.Millisecond)

	// Listing through one broker: every unkeyed and batch job exactly once,
	// in ascending ID order, across pages that straddle the shards.
	raw := rawClientAt(t, addrs[0])
	var listed []uuid.UUID
	token := ""
	for {
		resp, err := raw.ListJobs(ctx, &hopperv1.ListJobsRequest{Queue: "sleep", Limit: 37, PageToken: token})
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.GetJobs()), 37)
		for _, j := range resp.GetJobs() {
			listed = append(listed, uuid.MustParse(j.GetId()))
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}
	want := len(unkeyed) + len(batchKeyed)
	require.Equal(t, 300, want)
	require.Len(t, listed, want)
	seen := map[uuid.UUID]bool{}
	for i, id := range listed {
		require.False(t, seen[id], "listed %s twice", id)
		seen[id] = true
		require.True(t, unkeyed[id] || containsID(batchKeyed, id), "listed %s, which was not enqueued on sleep", id)
		if i > 0 {
			require.Negative(t, strings.Compare(listed[i-1].String(), id.String()), "listing is in ascending ID order")
		}
	}

	// Cancel and get: one delayed job per shard, each cancelled through the
	// other broker than the one that enqueued it.
	delayed := make([]uuid.UUID, testShards)
	for shard := range testShards {
		key := ""
		for j := 0; ; j++ {
			if k := fmt.Sprintf("delayed-%d", j); store.KeyShard("delayed", k, testShards) == shard {
				key = k
				break
			}
		}
		res, err := producers[shard].Enqueue(ctx, client.Job{
			Queue: "delayed", Payload: json.RawMessage(`{}`), IdempotencyKey: key, RunAt: time.Now().Add(time.Hour),
		})
		require.NoError(t, err)
		got, err := store.ShardOf(res.ID, testShards)
		require.NoError(t, err)
		require.Equal(t, shard, got)
		delayed[shard] = res.ID
	}
	for shard, id := range delayed {
		other := producers[(shard+1)%testShards]
		res, err := other.Cancel(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "JOB_STATE_CANCELLED", res.State)
		res, err = other.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "JOB_STATE_CANCELLED", res.State)
		res, err = producers[shard].GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "JOB_STATE_CANCELLED", res.State)
	}

	// Dedup: key-7 went in through broker 1; again through broker 2 it is the
	// same job.
	res, err := producers[1].Enqueue(ctx, client.Job{Queue: "keyed", Payload: json.RawMessage(`{}`), IdempotencyKey: "key-7"})
	require.NoError(t, err)
	require.True(t, res.Deduplicated)
	require.Equal(t, keyed["key-7"], res.ID)
}

func containsID(m map[string]uuid.UUID, id uuid.UUID) bool {
	for _, v := range m {
		if v == id {
			return true
		}
	}
	return false
}
