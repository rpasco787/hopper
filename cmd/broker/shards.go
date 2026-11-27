package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rpasco787/hopper/internal/metrics"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

// prepareShards migrates every database, checks that each records itself as
// the shard the broker was told it is, and checks that all of them ran the
// same migrations, all before the broker serves. With one database it is
// shard 0 of 1, and its IDs stay unstamped.
func prepareShards(ctx context.Context, pools []*pgxpool.Pool) ([]*postgres.Store, error) {
	stores := make([]*postgres.Store, len(pools))
	var first []string
	for i, pool := range pools {
		if err := postgres.Migrate(ctx, pool); err != nil {
			return nil, fmt.Errorf("migrate shard %d: %w", i, err)
		}
		st := postgres.New(pool)
		if len(pools) > 1 {
			st = st.WithShard(i)
		}
		if err := st.ClaimShard(ctx, i, len(pools)); err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
		applied, err := st.AppliedMigrations(ctx)
		if err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
		if i == 0 {
			first = applied
		} else if !slices.Equal(first, applied) {
			return nil, fmt.Errorf("shard %d ran migrations %v but shard 0 ran %v; a newer broker may have stopped part way: start one to finish them", i, applied, first)
		}
		stores[i] = st
	}
	return stores, nil
}

// poolSum reports every shard's pool as one, for the hopper_db_pool_* series.
type poolSum []*pgxpool.Pool

func (p poolSum) each(f func(*pgxpool.Stat) int64) int64 {
	var n int64
	for _, pool := range p {
		n += f(pool.Stat())
	}
	return n
}
func (p poolSum) MaxConns() int32 {
	return int32(p.each(func(s *pgxpool.Stat) int64 { return int64(s.MaxConns()) }))
}
func (p poolSum) AcquiredConns() int32 {
	return int32(p.each(func(s *pgxpool.Stat) int64 { return int64(s.AcquiredConns()) }))
}
func (p poolSum) IdleConns() int32 {
	return int32(p.each(func(s *pgxpool.Stat) int64 { return int64(s.IdleConns()) }))
}
func (p poolSum) ConstructingConns() int32 {
	return int32(p.each(func(s *pgxpool.Stat) int64 { return int64(s.ConstructingConns()) }))
}
func (p poolSum) AcquireCount() int64 {
	return p.each(func(s *pgxpool.Stat) int64 { return s.AcquireCount() })
}
func (p poolSum) EmptyAcquireCount() int64 {
	return p.each(func(s *pgxpool.Stat) int64 { return s.EmptyAcquireCount() })
}
func (p poolSum) EmptyAcquireWaitTime() time.Duration {
	return time.Duration(p.each(func(s *pgxpool.Stat) int64 { return int64(s.EmptyAcquireWaitTime()) }))
}

var _ metrics.PoolStat = poolSum(nil)
