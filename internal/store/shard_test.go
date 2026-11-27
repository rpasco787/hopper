package store

import (
	"errors"
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The routing function is part of every keyed job's address. These values
// are frozen: if this test fails, the change moves existing keys to other
// shards, which is resharding (docs/design/sharding.md).
func TestKeyShardIsFrozen(t *testing.T) {
	cases := []struct {
		queue, key string
		sum        uint64
		s2, s4, s7 int
	}{
		{"sleep", "k1", 0x0ba2034acab7ce48, 0, 0, 5},
		{"sleep", "k2", 0x0ba2064acab7d361, 1, 1, 0},
		{"cron", "nightly@2026-12-01T00:00:00Z", 0xfec63b4d824c49f8, 0, 0, 5},
		{"a", "bc", 0xab40f6820d40b523, 1, 3, 0},
		{"ab", "c", 0xfd61c083ef200867, 1, 3, 5},
	}
	for _, c := range cases {
		h := fnv.New64a()
		h.Write([]byte(c.queue))
		h.Write([]byte{0})
		h.Write([]byte(c.key))
		require.Equal(t, c.sum, h.Sum64(), "%q %q", c.queue, c.key)
		require.Equal(t, c.s2, KeyShard(c.queue, c.key, 2))
		require.Equal(t, c.s4, KeyShard(c.queue, c.key, 4))
		require.Equal(t, c.s7, KeyShard(c.queue, c.key, 7))
	}
	// The separator is why ("a","bc") and ("ab","c") hash differently.
	require.Equal(t, 0, KeyShard("anything", "at all", 1))
}

func TestKeyShardSpreadsKeys(t *testing.T) {
	counts := make([]int, 4)
	for i := range 100_000 {
		counts[KeyShard("sleep", fmt.Sprintf("run-%d", i), 4)]++
	}
	for i, n := range counts {
		require.InDelta(t, 25_000, n, 1_000, "shard %d", i)
	}
}

func TestStampAndShardOf(t *testing.T) {
	for shard := range MaxShards {
		id := StampShard(uuid.Must(uuid.NewV7()), shard)
		require.Equal(t, byte(7), id[6]>>4, "still version 7")
		require.Equal(t, byte(0b10), id[8]>>6, "still the RFC variant")
		got, err := ShardOf(id, MaxShards)
		require.NoError(t, err)
		require.Equal(t, shard, got)
	}
	id := StampShard(uuid.Must(uuid.NewV7()), 9)
	_, err := ShardOf(id, 4)
	require.True(t, errors.Is(err, ErrNotFound), "a byte past N names no shard")
	got, err := ShardOf(id, 1)
	require.NoError(t, err)
	require.Equal(t, 0, got, "one shard never reads the byte")
}

// Stamping keeps a process's IDs in creation order: they already differ in
// bytes 0-7 (time and google/uuid's monotonic sequence).
func TestStampKeepsOrder(t *testing.T) {
	prev := StampShard(uuid.Must(uuid.NewV7()), 255)
	for i := range 10_000 {
		next := StampShard(uuid.Must(uuid.NewV7()), i%4)
		require.Negative(t, compareIDs(prev, next))
		prev = next
	}
}

func compareIDs(a, b uuid.UUID) int {
	for i := range a {
		if a[i] != b[i] {
			return int(a[i]) - int(b[i])
		}
	}
	return 0
}
