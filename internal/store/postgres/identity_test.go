package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func clearIdentity(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(context.Background(), "delete from shard_identity")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "delete from shard_identity") })
}

func TestClaimShardRecordsThenHolds(t *testing.T) {
	s := newStore(t)
	clearIdentity(t)
	ctx := ctxT(t)
	require.NoError(t, s.ClaimShard(ctx, 1, 4))
	require.NoError(t, s.ClaimShard(ctx, 1, 4), "the same claim again is fine")

	var mismatch *ShardMismatchError
	require.ErrorAs(t, s.ClaimShard(ctx, 2, 4), &mismatch, "DSNs listed in another order")
	require.Equal(t, ShardMismatchError{Shard: 2, Shards: 4, HeldShard: 1, HeldShards: 4}, *mismatch)
	require.ErrorAs(t, s.ClaimShard(ctx, 1, 8), &mismatch, "another shard count")
	require.ErrorAs(t, s.ClaimShard(ctx, 0, 1), &mismatch, "one shard of four reused alone")
}

func TestClaimShardRejectsNonsense(t *testing.T) {
	s := newStore(t)
	clearIdentity(t)
	for _, c := range [][2]int{{-1, 2}, {2, 2}, {0, 0}, {0, 257}} {
		require.Error(t, s.ClaimShard(ctxT(t), c[0], c[1]), "%v", c)
	}
}

// Two brokers starting at once both succeed: the second's insert waits for
// the first's commit, and its separate select then sees the row.
func TestClaimShardConcurrently(t *testing.T) {
	s := newStore(t)
	clearIdentity(t)
	errs := make(chan error, 20)
	for range 20 {
		go func() { errs <- s.ClaimShard(ctxT(t), 0, 2) }()
	}
	for range 20 {
		require.NoError(t, <-errs)
	}
}

func TestAppliedMigrationsListsEveryFile(t *testing.T) {
	s := newStore(t)
	got, err := s.AppliedMigrations(ctxT(t))
	require.NoError(t, err)
	require.Equal(t, []string{
		"migrations/001_init.sql", "migrations/002_retention_index.sql", "migrations/003_shard_identity.sql",
	}, got)
}
