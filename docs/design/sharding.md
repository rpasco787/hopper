# Sharding: one hopper over N PostgreSQL databases

Phase 1 found the ceiling: one PostgreSQL's commit flush and shared rows
(docs/scaling.md). This splits jobs across N independent databases. Each
shard is a whole hopper database with the same schema; nothing replicates
between shards, and no transaction spans two.

## Invariants

1. Every job lives on exactly one shard for its whole life, and its ID says
   which: byte 9 of the UUIDv7 is the shard index (store.ShardOf).
2. A keyed job's shard is fnv1a64(queue, 0x00, key) mod N (store.KeyShard),
   so a key always meets its earlier row and the unique index deduplicates.
   Queue names can't contain a NUL byte (PostgreSQL text), so the separator
   is unambiguous.
3. Every per-row guarantee in docs/semantics.md holds within a shard because
   every statement still runs in one shard's store. This layer chooses a
   shard; it never splits a statement.
4. A lease asks each shard only for what it still lacks, so the total never
   exceeds the credit asked for (credit-anchored-freshness.md).
5. An unkeyed enqueue moves to another shard only after a failure that
   certainly happened before anything was sent (store.ErrNotSent). A
   maybe-sent failure goes to the client as UNAVAILABLE, so sharding adds no
   duplicates.
6. Each database records which shard it is (shard_identity). A broker whose
   DSN order or count disagrees refuses to start, and so does one whose
   shards ran different migrations.

## Routing

| Call | Goes to |
|---|---|
| Enqueue with a key | KeyShard |
| Enqueue without a key | next healthy shard in rotation; the next one on ErrNotSent |
| EnqueueBatch | items grouped as above; groups run concurrently, each in order |
| Ack, Nack, Heartbeat, Cancel, GetJob | ShardOf(id); out of range is NotFound |
| Lease | rotating start over healthy shards, asking each for n - got |
| ListJobs, ListDead | every shard, merged; one cursor (the order is total and global) |
| Redrive, Purge | grouped by ShardOf |
| Sweeps, depth sampling | per shard, independently (cmd/broker) |
| Cron, leader lock, Now | shard 0 |
| Announce | one healthy shard in rotation; every broker listens on all |

## Failure isolation

A shard is "down" for 1 s after any ErrUnavailable from it, and a prober on
its own session (not the pool, so a busy pool can't fake an outage) marks it
up or down every 500 ms. Health steers only choices: the unkeyed rotation,
the shards a lease visits, the announcement target. Calls that must go to one
shard go there and fail UNAVAILABLE if it is down. With one shard down:
keyed enqueues for it fail, unkeyed ones go elsewhere, leasing continues on
the rest, jobs on the down shard wait (not lost), and lists fail UNAVAILABLE
rather than return a page with a hole in it. The broker keeps one
confirmation batcher per shard, so a dead shard's flights hold no other
shard's acks.

## What it gives up

- Priority and run_at order within a shard only (§9).
- EnqueueBatch can leave any subset committed (§8); it already could leave a
  prefix.
- An idle stream polls every shard: N queries per poll interval.
- Cron and leader election on shard 0, which stays a single point of
  failure for cron only.
- Resharding. N is fixed for a deployment's life; shard_identity enforces it.
  Moving from one database to N, or N to M, is a migration project.

## Alternatives considered

- **A shard field on the wire** (the improvement plan's first idea). Every
  caller holding a job would have to carry it: both SDKs' confirmations,
  Cancel and GetJob from producers and the CLI, and operator tools holding a
  bare ID. The ID already travels everywhere and had random bits to spare.
- **A lookup table (id → shard).** Needs a database of its own: a new single
  point of failure and a write per job.
- **Hashing the job ID instead of stamping it.** Unkeyed jobs could then never
  avoid a down shard: the ID would choose the shard, not health.
- **Rendezvous or consistent hashing for keys.** Only useful for resharding,
  which is a non-goal; plain mod N is simpler to state and to test.
- **Partitioning one database (declarative partitions).** Same commit flush
  and WAL; it doesn't move the ceiling.
