package sharded

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

func fill(t *testing.T, pgs []*postgres.Store, perShard int) {
	t.Helper()
	for _, p := range pgs {
		for range perShard {
			_, _, err := p.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q"})
			require.NoError(t, err)
		}
	}
}

// The credit invariant (docs/design/credit-anchored-freshness.md): never more
// than n rows, however many shards it visits.
func TestLeaseNeverReturnsMoreThanN(t *testing.T) {
	s, pgs := newSharded(t)
	fill(t, pgs, 5)
	got, err := s.Lease(ctxT(t), []string{"q"}, 12, time.Minute, "w")
	require.NoError(t, err)
	require.Len(t, got, 12)
	got, err = s.Lease(ctxT(t), []string{"q"}, 100, time.Minute, "w")
	require.NoError(t, err)
	require.Len(t, got, 3, "what is left, from whichever shards hold it")
	got, err = s.Lease(ctxT(t), []string{"q"}, 100, time.Minute, "w")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestLeaseRotatesItsFirstShard(t *testing.T) {
	s, pgs := newSharded(t)
	fill(t, pgs, 50)
	firsts := map[int]bool{}
	for range testShards {
		got, err := s.Lease(ctxT(t), []string{"q"}, 10, time.Minute, "w")
		require.NoError(t, err)
		require.Len(t, got, 10)
		shard := shardOf(t, got[0].ID)
		for _, j := range got {
			require.Equal(t, shard, shardOf(t, j.ID), "a full batch from one shard")
		}
		firsts[shard] = true
	}
	require.Len(t, firsts, testShards, "each shard went first once")
}

func TestLeaseServesFromTheShardsThatAnswer(t *testing.T) {
	s, pgs := withDead(t, 1)
	fill(t, []*postgres.Store{pgs[0], pgs[2]}, 4)
	start := time.Now()
	var total int
	for range 6 {
		got, err := s.Lease(ctxT(t), []string{"q"}, 2, time.Minute, "w")
		require.NoError(t, err)
		total += len(got)
	}
	require.Equal(t, 8, total)
	require.Less(t, time.Since(start), 4*time.Second)
}

func TestLeaseFailsWhenNoShardAnswers(t *testing.T) {
	_, pgs := withDead(t, 0)
	_, pgs2 := withDead(t, 1)
	_, pgs3 := withDead(t, 2)
	s, err := New([]Shard{pgs[0], pgs2[1], pgs3[2]}, nil)
	require.NoError(t, err)
	_, err = s.Lease(ctxT(t), []string{"q"}, 5, time.Minute, "w")
	require.ErrorIs(t, err, store.ErrUnavailable)
}
