package store

import (
	"context"
	"errors"
	"hash/fnv"

	"github.com/google/uuid"
)

// MaxShards is the most databases one deployment can be split across. A
// job's shard is one byte of its ID, so 256 is the limit
// (docs/design/sharding.md).
const MaxShards = 256

// shardByte is the byte of a job ID that names its shard. In google/uuid's
// UUIDv7, bytes 0-5 are the millisecond timestamp, bytes 6-7 the version and
// a 12-bit sequence kept monotonic per process, and byte 8 the variant plus
// six random bits. Byte 9 is the first wholly random byte, so writing it
// costs 8 of 62 random bits and changes no ordering between a process's IDs.
const shardByte = 9

// StampShard returns id with its shard byte set to shard. postgres.Store
// stamps every ID it generates once it is given a shard (WithShard).
func StampShard(id uuid.UUID, shard int) uuid.UUID {
	id[shardByte] = byte(shard)
	return id
}

// ShardOf names the shard that holds the job with this id, in a deployment
// split across shards databases. With one shard it is 0 and the ID is not
// read: an unsharded deployment never stamps its IDs. A byte that names no
// shard of this deployment is ErrNotFound, since no shard can hold the job.
func ShardOf(id uuid.UUID, shards int) (int, error) {
	if shards <= 1 {
		return 0, nil
	}
	i := int(id[shardByte])
	if i >= shards {
		return 0, ErrNotFound
	}
	return i, nil
}

// KeyShard is the shard a keyed job lives on: FNV-1a 64 of the queue, a zero
// byte and the key, modulo shards. A key therefore always meets its earlier
// row, and the unique index deduplicates as it does on one database
// (docs/semantics.md §8). PostgreSQL text can't hold a NUL byte, so the
// separator is unambiguous. It is fixed for good: changing it moves keys
// between shards, which is resharding. TestKeyShardIsFrozen holds it.
func KeyShard(queue, key string, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(queue))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(shards))
}

// ErrNotSent marks an ErrUnavailable failure that certainly happened before
// anything reached the database: no connection could be had, or the driver
// says nothing was sent. The call cannot have taken effect, so it is safe to
// make again anywhere. The sharded store moves an unkeyed Enqueue to another
// shard only on this error; a failure that may have reached the database goes
// to the caller as it is, because the insert may have committed.
var ErrNotSent = errors.New("store: nothing was sent")

// ShardRouter is implemented by stores split across several databases. The
// broker uses it to keep one confirmation batcher per shard, so a shard that
// has stopped answering holds up no other shard's confirmations.
type ShardRouter interface {
	// Shards is how many databases the store is split across.
	Shards() int
	// ShardOf names the shard holding the job with this id, or ErrNotFound
	// when the id names none.
	ShardOf(id uuid.UUID) (int, error)
}

// EnqueueResult is one item's outcome from EnqueueMany.
type EnqueueResult struct {
	Job      Job
	Inserted bool
	// Done reports that this item committed (inserted or deduplicated).
	Done bool
}

// BatchEnqueuer is implemented by stores that can enqueue a batch faster
// than one Enqueue at a time, as the sharded store does by running each
// shard's items concurrently. The broker's EnqueueBatch uses it when present.
type BatchEnqueuer interface {
	// EnqueueMany enqueues each item as Enqueue would and returns one result
	// per input, in order. It is not atomic: on error, the items whose Done
	// is set committed and stay committed, and they need not be a prefix.
	// The error names the first failed item in input order.
	EnqueueMany(ctx context.Context, ps []EnqueueParams) ([]EnqueueResult, error)
}
