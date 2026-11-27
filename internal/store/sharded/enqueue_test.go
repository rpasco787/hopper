package sharded

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestKeyedEnqueueLandsOnItsKeysShardAndDeduplicates(t *testing.T) {
	s, _ := newSharded(t)
	ctx := ctxT(t)
	for i := range 300 {
		key := fmt.Sprintf("key-%d", i)
		job, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
		require.NoError(t, err)
		require.True(t, inserted)
		require.Equal(t, store.KeyShard("q", key, testShards), shardOf(t, job.ID))
		again, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
		require.NoError(t, err)
		require.False(t, inserted)
		require.Equal(t, job.ID, again.ID)
	}
	require.Equal(t, 300, rowsOn(t, 0)+rowsOn(t, 1)+rowsOn(t, 2))
}

func TestUnkeyedEnqueueRotates(t *testing.T) {
	s, _ := newSharded(t)
	for range 300 {
		_, _, err := s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q"})
		require.NoError(t, err)
	}
	for shard := range testShards {
		require.Equal(t, 100, rowsOn(t, shard), "shard %d", shard)
	}
}

// TestUnkeyedEnqueueRotatesBetweenOtherCalls: a broker announces after each
// enqueue and polls leases between them. Those calls must not move the
// enqueue rotation, or every unkeyed job could land on one shard: with N
// shards and N calls per enqueue, each enqueue would start where the last did.
func TestUnkeyedEnqueueRotatesBetweenOtherCalls(t *testing.T) {
	s, _ := newSharded(t)
	for range 300 {
		_, _, err := s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q"})
		require.NoError(t, err)
		require.NoError(t, s.Announce(ctxT(t), []string{"q"}))
		_, err = s.Lease(ctxT(t), []string{"absent"}, 1, time.Minute, "w")
		require.NoError(t, err)
	}
	for shard := range testShards {
		require.Equal(t, 100, rowsOn(t, shard), "shard %d", shard)
	}
}

func TestUnkeyedEnqueueGoesAroundADownShard(t *testing.T) {
	s, _ := withDead(t, 1)
	start := time.Now()
	for range 100 {
		job, _, err := s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q"})
		require.NoError(t, err)
		require.NotEqual(t, 1, shardOf(t, job.ID))
	}
	require.Less(t, time.Since(start), 5*time.Second, "the dead shard is tried about once a second, not on every call")
}

func TestKeyedEnqueueToADownShardFails(t *testing.T) {
	s, _ := withDead(t, 1)
	key := keyFor("q", 1)
	_, _, err := s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.ErrorIs(t, err, store.ErrUnavailable)
	other := keyFor("q", 2)
	_, _, err = s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q", IdempotencyKey: &other})
	require.NoError(t, err)
}

// maybeSent fails Enqueue the way a connection lost mid-statement does.
type maybeSent struct{ Shard }

func (maybeSent) Enqueue(context.Context, store.EnqueueParams) (store.Job, bool, error) {
	return store.Job{}, false, fmt.Errorf("store: enqueue: %w: lost mid-statement", store.ErrUnavailable)
}

func TestUnkeyedEnqueueDoesNotMoveAMaybeSentFailure(t *testing.T) {
	_, pgs := newSharded(t)
	s, err := New([]Shard{maybeSent{pgs[0]}, pgs[1], pgs[2]}, nil)
	require.NoError(t, err)
	_, _, err = s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q"}) // the rotation starts at shard 0
	require.ErrorIs(t, err, store.ErrUnavailable)
	require.Zero(t, rowsOn(t, 1)+rowsOn(t, 2), "the insert may have committed on shard 0; trying elsewhere could make a second job")
}

func TestEnqueueManyKeepsOrderAndRoutesEachItem(t *testing.T) {
	s, _ := newSharded(t)
	var ps []store.EnqueueParams
	for i := range 200 {
		p := store.EnqueueParams{Queue: "q"}
		if i%2 == 0 {
			key := fmt.Sprintf("b-%d", i)
			p.IdempotencyKey = &key
		}
		ps = append(ps, p)
	}
	results, err := s.EnqueueMany(ctxT(t), ps)
	require.NoError(t, err)
	require.Len(t, results, 200)
	for i, r := range results {
		require.True(t, r.Done)
		require.True(t, r.Inserted)
		if ps[i].IdempotencyKey != nil {
			require.Equal(t, *ps[i].IdempotencyKey, *r.Job.IdempotencyKey, "result %d is item %d's", i, i)
			require.Equal(t, store.KeyShard("q", *ps[i].IdempotencyKey, testShards), shardOf(t, r.Job.ID))
		}
	}
	for shard := range testShards {
		require.Greater(t, rowsOn(t, shard), 40, "shard %d", shard)
	}
}

func TestEnqueueManyReportsWhatCommittedOnFailure(t *testing.T) {
	s, _ := withDead(t, 1)
	k0, k1, k2 := keyFor("q", 0), keyFor("q", 1), keyFor("q", 2)
	results, err := s.EnqueueMany(ctxT(t), []store.EnqueueParams{
		{Queue: "q", IdempotencyKey: &k0}, {Queue: "q", IdempotencyKey: &k1}, {Queue: "q", IdempotencyKey: &k2},
	})
	require.ErrorIs(t, err, store.ErrUnavailable)
	require.True(t, results[0].Done)
	require.False(t, results[1].Done)
	require.True(t, results[2].Done, "not a prefix: shard 2's item committed after shard 1's failed")
	require.True(t, errors.Is(err, store.ErrUnavailable))
}
