package sharded

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

// Three databases on one server stand in for three shards: the routing is
// what's under test, and each shard is a whole hopper database. Like the
// postgres package, tests here share the server and must not call
// t.Parallel().
const testShards = 3

// testLeaderTTL is long enough that no test's leader goes silent for it.
const testLeaderTTL = time.Minute

var pools []*pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	ctr, err := tcpg.Run(ctx, "postgres:16-alpine",
		tcpg.WithDatabase("hopper"), tcpg.WithUsername("hopper"), tcpg.WithPassword("hopper"),
		tcpg.BasicWaitStrategies())
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}
	code := 1
	defer func() {
		for _, p := range pools {
			p.Close()
		}
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			log.Printf("terminate container: %v", err)
		}
		os.Exit(code)
	}()
	admin, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return
	}
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		log.Printf("connect: %v", err)
		return
	}
	defer conn.Close(ctx)
	for i := range testShards {
		name := fmt.Sprintf("shard%d", i)
		if _, err := conn.Exec(ctx, "create database "+name); err != nil {
			log.Printf("create %s: %v", name, err)
			return
		}
		cfg, err := pgxpool.ParseConfig(strings.Replace(admin, "/hopper?", "/"+name+"?", 1))
		if err != nil {
			log.Printf("parse: %v", err)
			return
		}
		cfg.MaxConns = 20
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			log.Printf("pool: %v", err)
			return
		}
		pools = append(pools, p)
		if err := postgres.Migrate(ctx, p); err != nil {
			log.Printf("migrate %s: %v", name, err)
			return
		}
	}
	code = m.Run()
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newSharded empties every shard and returns a sharded store over them,
// with the shard stores for direct setup and assertions.
func newSharded(t *testing.T) (*Store, []*postgres.Store) {
	t.Helper()
	var shards []Shard
	var pgs []*postgres.Store
	for i, p := range pools {
		_, err := p.Exec(context.Background(), "truncate jobs, cron_schedules")
		require.NoError(t, err)
		st := postgres.New(p).WithShard(i)
		shards = append(shards, st)
		pgs = append(pgs, st)
	}
	s, err := New(shards, nil)
	require.NoError(t, err)
	return s, pgs
}

// withDead replaces shard i with a store whose every connection is refused.
func withDead(t *testing.T, i int) (*Store, []*postgres.Store) {
	t.Helper()
	_, pgs := newSharded(t)
	dead, err := pgxpool.New(context.Background(),
		"postgres://hopper:hopper@127.0.0.1:1/hopper?sslmode=disable&connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(dead.Close)
	pgs[i] = postgres.New(dead).WithShard(i)
	shards := make([]Shard, len(pgs))
	for k, p := range pgs {
		shards[k] = p
	}
	s, err := New(shards, nil)
	require.NoError(t, err)
	return s, pgs
}

func rowsOn(t *testing.T, shard int) int {
	t.Helper()
	var n int
	require.NoError(t, pools[shard].QueryRow(ctxT(t), "select count(*) from jobs").Scan(&n))
	return n
}

// keyFor finds a key in queue that routes to shard.
func keyFor(queue string, shard int) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("k%d", i)
		if store.KeyShard(queue, k, testShards) == shard {
			return k
		}
	}
}

func shardOf(t *testing.T, id uuid.UUID) int {
	t.Helper()
	i, err := store.ShardOf(id, testShards)
	require.NoError(t, err)
	return i
}

func TestNewNeedsTwoShards(t *testing.T) {
	_, err := New([]Shard{postgres.New(pools[0])}, nil)
	require.Error(t, err)
}

func TestIDCallsGoToTheOwningShard(t *testing.T) {
	s, pgs := newSharded(t)
	ctx := ctxT(t)
	for shard := range testShards {
		job, _, err := pgs[shard].Enqueue(ctx, store.EnqueueParams{Queue: "q"})
		require.NoError(t, err)
		got, err := s.GetJob(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, job.ID, got.ID)
		leased, err := pgs[shard].Lease(ctx, []string{"q"}, 1, time.Minute, "w")
		require.NoError(t, err)
		require.Len(t, leased, 1)
		require.NoError(t, s.ExtendLease(ctx, job.ID, *leased[0].LeaseID, time.Minute))
		require.NoError(t, s.Ack(ctx, job.ID, *leased[0].LeaseID))
		done, err := pgs[shard].GetJob(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, store.StateCompleted, done.State)
	}
	foreign := store.StampShard(uuid.Must(uuid.NewV7()), 7)
	_, err := s.GetJob(ctx, foreign)
	require.ErrorIs(t, err, store.ErrNotFound)
	require.ErrorIs(t, s.Ack(ctx, foreign, uuid.New()), store.ErrNotFound)
	_, err = s.Cancel(ctx, foreign)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestCronAndLeadershipLiveOnShardZero(t *testing.T) {
	s, pgs := newSharded(t)
	ctx := ctxT(t)
	_, err := s.UpsertSchedule(ctx, store.CronScheduleSpec{
		Name: "nightly", Queue: "q", CronExpr: "0 0 * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	for shard, p := range pgs {
		got, err := p.ListSchedules(ctx)
		require.NoError(t, err)
		require.Len(t, got, map[bool]int{true: 1, false: 0}[shard == 0], "shard %d", shard)
	}
	lead, err := s.TryLeadership(ctx, testLeaderTTL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lead.Release(context.Background()) })
	_, err = pgs[0].TryLeadership(ctx, testLeaderTTL)
	require.ErrorIs(t, err, store.ErrNotLeader, "the lock is shard 0's")
	other, err := pgs[1].TryLeadership(ctx, testLeaderTTL)
	require.NoError(t, err, "shard 1's lock is unrelated")
	_ = other.Release(ctx)
}
