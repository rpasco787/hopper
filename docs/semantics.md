# hopper — delivery semantics

This document is the contract between hopper and the code that uses it. If the
implementation and this document disagree, one of them has a bug; fix whichever
is wrong and keep this file authoritative. The "verified behavior" table at the
end is filled in from the test suites, the chaos suite (`test/chaos`) and the
benchmark harness (`scripts/bench.sh`), not from intent.

## 1. Guarantees in one paragraph

hopper delivers every enqueued job **at least once**. A job is never lost once
`Enqueue` has returned, and it is never delivered to two workers at the same
time, but it *can* be delivered more than once in sequence: whenever a worker
holds a lease and does not ack before the lease expires, the job is redelivered.
Handlers must therefore be idempotent. hopper provides no ordering guarantee
across jobs and no exactly-once execution; the latter is a property your handler
must supply, using the stable job ID (or a stable business ID in the payload) as
its dedup key. `job.Attempt` is delivery history, not an idempotency key.

**Where "never lost" comes from.** `Enqueue` returns once PostgreSQL has
committed the row, so the promise lasts as long as the commit does. A single
PostgreSQL keeps it while its disk survives. A replicated one keeps it across
a failover only when every commit waits for a synchronous standby before it
returns, and the failover promotes the standby that received the most WAL.
The chart's `postgres.mode=cloudnativepg` does both (`synchronousStandbys: 1`).
Under asynchronous replication a failover can lose commits that were
acknowledged: enqueued jobs vanish, and acked jobs run again (§11, item 6).
Synchronous replication still lets a commit whose wait for the standby was
cancelled become visible on the primary alone; since bf90316 no
caller is told it succeeded until a standby has it (§11, item 7). Measured
in §15.

## 2. Job states

```
                 run_at > now                        run_at <= now
  Enqueue ────────────────────► scheduled ─────────────────────────┐
     │                              ▲            (promoter tick)   │
     │ run_at <= now                │                              ▼
     └──────────────────────────────┼─────────────────────────► available
                                    │                              │
             Nack / lease expired,  │                              │ Lease
             attempts < max_attempts│                              ▼
                                    └────────────────────────── leased
                                                                   │
                        ┌──────────────────┬───────────────────────┤
                        │ Ack              │ Nack / lease expired, │ Cancel
                        ▼                  │ attempts >= max       ▼
                    completed              ▼                   cancelled
                                         dead ── Redrive ──► available
```

| State | Meaning | Leasable? | Terminal? |
|---|---|---|---|
| `scheduled` | Not yet due. Either a delayed enqueue or a retry waiting out its backoff. `run_at` says when. | no | no |
| `available` | Due and waiting for a worker. | **yes** | no |
| `leased` | Held by exactly one worker until `lease_expires`. | no | no |
| `completed` | Acked by the worker that held the lease. | no | yes |
| `cancelled` | Cancelled by a client before completion. | no | yes |
| `dead` | Exhausted `max_attempts`. Kept for inspection; only `Redrive` moves it. | no | yes, until redriven |

There is no `failed` state. A failed attempt with retries remaining is
`scheduled` with a future `run_at`; the failure is recorded in `attempts` and
`last_error`.

Invariants the store enforces or relies on:

- `state = 'leased'` ⇒ `lease_id` and `lease_expires` are non-null (CHECK constraint).
- `state = 'available'` ⇒ `run_at` was `<= now()` when the row entered that state.
- A row is in exactly one state; transitions happen in single `UPDATE`s guarded
  by a `WHERE state = ...` clause, so a lost race updates zero rows and the
  caller is told.

## 3. Field semantics

| Column | Semantics |
|---|---|
| `id` | Client-generated UUIDv7 (time-ordered, so the primary-key B-tree appends rather than fragments). `gen_random_uuid()` default is a fallback only. |
| `queue` | Handler name. Workers subscribe to a set of queues. There is no separate "job type" field. |
| `payload` | Opaque to hopper. Stored as `jsonb`, so it must be valid JSON; hopper never reads its contents. |
| `priority` | Higher leases first within a queue. Best-effort; see §9. |
| `run_at` | Earliest time the job may be leased. Set by `Enqueue` (delay) and by retry backoff. |
| `attempts` | Number of leases ever granted. Incremented **at lease time**, so a worker that crashes still consumes an attempt. Exposed to handlers as `job.Attempt` (1 on the first delivery). |
| `max_attempts` | Dead-letter threshold. A delivery that fails when `attempts >= max_attempts` sends the job to `dead`. |
| `lease_id` | Token identifying one delivery. Generated per row at lease time, **overwritten by the next lease, never cleared**. Every Ack/Nack/Heartbeat must present it. |
| `leased_by`, `leased_at` | Which worker holds/held the lease and since when. Observability only; never used for authorization. |
| `lease_expires` | The visibility timeout. Past this instant the reaper may take the job back. |
| `idempotency_key` | Optional. Unique per `(queue, key)` among rows that exist. See §7. |
| `last_error` | Message from the most recent `Nack`, or `"lease expired"` if the reaper took the job. |
| `created_at`, `updated_at` | `updated_at` is set explicitly in every `UPDATE`; there is no trigger. |
| `finished_at` | Set when the job enters `completed`, `cancelled`, or `dead`. Null otherwise. |

## 4. Lease (visibility timeout)

A lease is exclusive possession of a job for a bounded time.

```sql
with next as (
  select c.id
  from unnest($1::text[]) as q(queue),
  lateral (
    select id, priority, run_at from jobs
    where jobs.queue = q.queue and state = 'available'
    order by priority desc, run_at
    limit $2
    for update skip locked
  ) c
  order by c.priority desc, c.run_at
  limit $2
)
update jobs j
set state = 'leased', lease_id = gen_random_uuid(), leased_by = $3, leased_at = now(),
    lease_expires = now() + $4::interval, attempts = attempts + 1, updated_at = now()
from next where j.id = next.id
returning j.*;
```

- **Exclusivity.** `FOR UPDATE SKIP LOCKED` means concurrent `Lease` calls lock
  disjoint rows; a row another transaction has locked is skipped rather than
  waited on. Two workers can never receive the same `(id, lease_id)`.
- **Cost.** Each subscribed queue, with repeats collapsed, is read through
  `jobs_lease_idx` in order and stops after n unlocked rows. The outer sort sees
  at most n rows per queue, so its input is bounded by n × queues, whatever the
  backlog. The index walk also steps over rows other lessors hold and over
  entries for rows leased since the last vacuum. Its work therefore grows with
  concurrent lessors and vacuum lag, but not with the number of available jobs.
  - Until Session 11 the statement was `queue = any($1) … order by … limit n`.
    PostgreSQL answered it by reading and sorting every available row in the
    subscribed queues: 9.2 ms per lease draining 40,000 jobs on one broker and
    18.0 ms on two, about 80% of all database time, and a two-broker drain
    slower than one broker's.
  - A queue's subquery can lock up to n rows the outer limit drops. They are
    released at commit, and other lessors skip them meanwhile, as SKIP LOCKED
    already allows.
- **Duration.** `lease_expires = now() + lease duration`. The default is set on
  the broker; a worker may request a different duration per stream. On the
  wire an absent or zero duration selects that default. Any other value must be
  a valid protobuf `Duration` that converts to a Go `time.Duration` exactly (no
  saturation) and be positive, or the call fails with `INVALID_ARGUMENT` before
  it reaches the store. No operational maximum is imposed.
- **Flow control.** A worker holds one `LeaseJobs` stream and spends *credit*:
  its first message declares how many delivery slots are free, and every later
  message returns credit only after a delivery has stopped using its slot. A
  token covers the start (an ownership check, for a delivery the start rule of
  the Extension bullet below does not vouch for), handler execution, and
  Ack/Nack confirmation; handler return alone does not free it. The broker never leases
  more rows than the outstanding credit, and never more than
  `-max-lease-batch` at once. This is why the stream is bidirectional: a
  server-stream carries no capacity-return signal, and the broker cannot infer
  one from `Ack`, because brokers are stateless and a worker's `Ack` may land on
  a different replica. Credits are cooperative free-slot declarations, not
  receipt acknowledgments. Stream teardown reclaims only unresolved stream
  credit. On reconnect the worker advertises its currently free slots; closing
  a stream does not revoke leases or capacity held by deliveries already in
  progress. A redundant subscription is rejected. A broker ends a stream with
  `UNAVAILABLE` when it reaches `-lease-stream-max-age` (±10%), and only
  between batches, so every row it leased has been sent. The worker reconnects
  as it does after a drain. `-max-connection-age` never cuts a stream: its
  grace is unlimited, so an aged connection closes only once its lease stream
  has ended.
- **The credit invariant.** On a lease stream, the broker leases a row only
  against credit it has already received on that stream and not yet spent.
  Before each store call it asks for at most the credit received so far minus
  the rows already sent, and it sends no row beyond what that call asked for.
  It never leases ahead of credit, never holds leased rows for credit still to
  come, and never carries credit from one stream to another. Consequence: the
  k-th delivery on a stream, counting from the first, was leased by a store call
  that started after the broker had received k units of that stream's credit,
  counting the subscription's credit first. It is enforced in
  `internal/broker/lease.go`: the call asks for `min(credit, -max-lease-batch)`,
  credit falls only after a row is sent, and a store that returns more rows
  than asked ends the stream `INTERNAL` without sending any of them. It is
  pinned by `broker: TestLeaseStreamNeverExceedsCredit`,
  `TestLeaseStreamLeasesOnlyAgainstCreditItHasReceived` and
  `TestLeaseStreamSendsNoRowsBeyondWhatItAskedFor`. The worker's start rule
  (Extension, below) rests on it, so any change to the lease path, including a
  lease coalescer (H1), must keep it. The design is
  [design/credit-anchored-freshness.md](design/credit-anchored-freshness.md).
- **Extension.** `Heartbeat(id, lease_id, dur)` (store method: `ExtendLease`) sets
  `lease_expires = now() + dur` **only if** `state = 'leased' and lease_id = $2`.
  Each delivery carries valid `leased_at` and `lease_expires` timestamps; their
  difference is the broker-granted duration G. As an explicit SDK policy, the
  worker starts a handler under **the start rule**, then renews at one third of
  G. Every Heartbeat sends that original grant explicitly; it does not
  reselect a broker default that may have changed since delivery. A successful
  RPC creates a conservative local deadline from the monotonic request-start
  instant with scheduling slack; that estimate is neither a new store guard nor
  permission to revive an already expired local lease. If ownership is
  definitively rejected, the job was cancelled, or the local deadline expires,
  the SDK cancels the handler's `context`. A handler should stop, but Go
  context cancellation cannot forcibly stop code that ignores it. The start
  rule is:

  - **Stamp.** Each stream keeps, oldest first, the instant each unit of credit
    was handed to the transport: one monotonic reading taken just before the
    `Send` of the subscription or credit message that carries it. Units
    reserved but never sent (the drain path) carry no instant.
  - **Match.** Each arriving delivery is matched to the oldest unit not yet
    matched on its stream. A delivery with no unit left is an over-delivery:
    the stream ends and the job is neither started nor counted.
  - **Decide.** Let s be the matched unit's send instant, r the instant the
    delivery was read off the stream, and G its grant. If s exists and
    0 ≤ r − s < G/10, the handler starts at once, with local deadline
    s + G − min(0.5 s, G/10).
  - **Otherwise check.** The SDK sends the bounded ownership Heartbeat before
    the handler, as before.
  - **Clocks.** Every instant is the worker's own monotonic clock. No instant
    from the database is compared with one from the worker.
  - **Reconnect.** A new stream starts with no units. Its subscription's credit
    is stamped at that subscription's `Send`. Units left on an ended stream are
    forgotten when its credit is reclaimed.

  It is safe because of the credit invariant: delivery k was leased after the
  broker held unit k, so its lease runs at least G from s_k, and s_k + G −
  margin is at least the margin before the reaper may take the row, whatever
  the delivery's age. This assumes the database clock advances at the real rate
  over G, the same assumption every renewal makes, and that nothing but `Ack`,
  `Nack`, `Cancel` and the reaper writes a leased row. The G/10 threshold only
  decides whether skipping is worth it: at r − s < G/10 the deadline is at
  least 0.8 G after receipt, so the renewals at G/3 and 2G/3 both fall before
  it, and a stale delivery, whose credit sat on an idle stream, still takes the
  check, which also buys a fresh grant. A delivery that skips the check learns
  that its job was cancelled (§10), or under asynchronous replication that a
  failover lost its lease (§11 item 6), at its first renewal, G/3 after start,
  or at its `Ack` or `Nack`, not before the handler starts. The design, with
  the rejected alternatives and what the Java SDK copies, is
  [design/credit-anchored-freshness.md](design/credit-anchored-freshness.md).
  The Java SDK (`java/hopper-worker`) applies the same rule: its stamp is a
  `System.nanoTime()` reading taken just before `onNext` of each subscription
  or credit message ([design/java-worker-sdk.md](design/java-worker-sdk.md)).

  History and cost. Until 2026-11-23 the SDK took that bounded Heartbeat before
  every handler. Measured on 2026-11-19 (H4 in [scaling.md](scaling.md)), the
  check was 27.7% of statement time at one broker and 49.6% at four in a
  drain, and a benchmark-only switch that skipped it unconditionally raised
  one-broker throughput 1.36× and cut commits per job from 0.44 to 0.28 (0.93
  to 0.52 at four brokers). Skipping unconditionally was unsafe, so the switch
  was retired once the start rule did the same safely. The rule, measured on
  2026-11-23 in the same drain against `770fdb5` (5 runs each, interleaved, 8
  workers, `PIN=1`, 300-byte payloads): Heartbeat calls per job fell from 1.000
  to 0.000 at both broker counts; commits per job from 0.45 to 0.29 at one
  broker and from 0.92 to 0.51 at four; jobs/s rose from 6,099 (5,717–6,879) to
  8,937 (8,543–10,143), 1.47×, at one broker and from 4,060 (3,956–4,297) to
  6,031 (5,109–7,515), 1.49×, at four, the ranges apart in each case
  ([bench/1b/h4-credit/](bench/1b/h4-credit/)).
- **Expiry.** The reaper runs on a ticker and applies the retry policy (§6) to
  every row with `state = 'leased' and lease_expires < now()`. Redelivery is
  therefore not instantaneous: worst-case latency is lease duration + reaper
  interval + backoff.
- **Due-ness is a state, not a filter.** The lease query does not check
  `run_at`. The store method is `PromoteDue` and the ticker is
  `sweep.Promoter`; together they move `scheduled → available` when
  `run_at <= now()`. Consequences: the `available` partial index contains only
  leasable rows; `queue_depth{state="available"}` (the KEDA signal) means
  "leasable right now"; and delayed or retried jobs fire up to one promoter
  interval (default 1s) after `run_at`.

## 5. Ack and Nack

Both take `(id, lease_id)` and both are guarded:

```sql
update jobs set state = 'completed', finished_at = now(), updated_at = now()
where id = $1 and lease_id = $2 and state = 'leased';
```

Brokers apply concurrent `Ack`s, and concurrent `Heartbeat`s, one statement
per batch (`-confirm-batch`, default 64; `1` turns batching off): the same guard
for every `(id, lease_id)` pair, and one commit for all of them. Each call is
answered only after the statement that applied it commits, with exactly the
result listed below for its own pair. A pair the batch couldn't lock, or whose guard
failed, is applied on its own by the same call once the batch has committed,
and classified as above. Other calls in the batch don't wait for it. `Nack` is
not batched.

For `Ack`, if zero rows are updated the store looks at the row and returns one
of:

| Situation | Result |
|---|---|
| Row is `completed` **and** `lease_id = $2` | `OK` (idempotent re-ack; the first ack's response was lost), answered once a synchronous standby has the completion (§11 item 7) |
| Row is `leased` with a different `lease_id` | `ErrLeaseLost` — the lease expired and someone else holds the job |
| Row is `scheduled` / `available` / `dead` | `ErrLeaseLost` — the reaper took it back |
| Row is `cancelled` | `ErrCancelled` |
| No row | `ErrNotFound` |

The idempotent re-ack case is why `lease_id` is never cleared on completion.
`Nack` is guarded by the same `(id, lease_id)` ownership token and applies the
retry policy. A retry of a Nack succeeds idempotently only while that same-token
row is still `scheduled` or `dead`. Like a re-ack, it is answered once a
synchronous standby has the retry. Once the promoter changes `scheduled` to
`available`, a repeated Nack returns `ErrLeaseLost`/`ABORTED` even though no new
worker has necessarily leased it. A stale worker can never ack, nack, or extend
a job it no longer holds; this is the property the "stale-lease ack rejected"
test in `store` pins down.

Over gRPC these become status codes:

| Store result | gRPC code | What the worker should do |
|---|---|---|
| `OK` | `OK` | Continue. |
| `ErrLeaseLost` | `ABORTED` | Stop working on the job. Someone else may have it. |
| `ErrCancelled` | `FAILED_PRECONDITION` | Stop; the job was called off. |
| `ErrTerminal` | `FAILED_PRECONDITION` | The message names the state. |
| `ErrNotFound` | `NOT_FOUND` | The job is gone; retention may have deleted it. |
| RPC context canceled / expired | `CANCELLED` / `DEADLINE_EXCEEDED` | Respect the caller's cancellation/deadline. |
| broker checks reject request | `INVALID_ARGUMENT` | Correct the request. |
| message or credit limit exceeded | `RESOURCE_EXHAUSTED` | Reduce size/credit; a response failure does not imply rollback. |
| broker draining | `UNAVAILABLE` | Reconnect with backoff; preserve existing handler capacity. |
| `ErrUnavailable` (database unreachable or connection lost) | `UNAVAILABLE` | Retry with backoff. The call may have taken effect: `Ack`, `Nack` and `Heartbeat` are idempotent under their lease token, and `Enqueue` is idempotent only with an idempotency key. The worker SDK retries it within the lease. |
| other store error | `INTERNAL` | Detail is logged. Do not retry blindly: invalid database input is permanent, and mutations may already have committed. |

`ErrCancelled` is deliberately **not** `CANCELLED`: that code means the caller
cancelled the RPC, and conflating the two would make a job outcome
indistinguishable from a context deadline to any generic retry interceptor.

Classifying a store failure as `ErrUnavailable` is the store's job, because it
knows the driver. Any failure to connect counts (refused, DNS, a server that is
starting up or shutting down, even bad credentials: nothing was sent), as does
a connection-class or shutdown error from the server (SQLSTATE class `08`,
`57P01`–`57P03`, `53300`) and a network error or unexpected EOF mid-call. The
caller's own `context.Canceled` or `DeadlineExceeded` does not count, and
neither does any other server error (constraints, invalid input,
serialization). Those stay `INTERNAL` and are not retried.

`Cancel` is idempotent at the gRPC boundary although `Cancel` in the store is
not: cancelling an already-cancelled job returns OK with `already_cancelled`, once a synchronous standby has the
cancellation, because a retried `Cancel` must not look like a failure. `completed` and `dead`
still fail.

## 6. Retries and backoff

Applied identically by `Nack` and by the reaper on lease expiry:

```
if attempts >= max_attempts:
    state = dead, finished_at = now(), last_error = err
else:
    delay  = min(cap, base * 2^(attempts-1)) * U(0.75, 1.25)
    state  = scheduled, run_at = now() + delay, last_error = err
```

Defaults: `base = 1s`, `cap = 5m`, `max_attempts = 5`. With these, a job that
fails every time is attempted at roughly t=0, +1s, +2s, +4s, +8s and then dies.
`attempts` is counted at lease time (§3), so the exponent on the first retry
is 0.

Lease expiry uses backoff rather than immediate redelivery deliberately: a
payload that crashes or hangs every worker that touches it would otherwise
cycle through the fleet as fast as leases expire.

## 7. Dead-letter queue

- A job becomes `dead` when a delivery fails and `attempts >= max_attempts`.
  Nothing automatic ever moves it out.
- `ListDead(queue, limit, cursor)` pages through dead jobs by `finished_at`.
- `Redrive(ids)` sets `state = 'available', attempts = 0, last_error = null,
  finished_at = null, run_at = now()`. The job gets a fresh set of
  `max_attempts`. Its `id` and `idempotency_key` are unchanged.
- `Purge(ids)` deletes the rows. This also releases their idempotency keys.

## 8. Idempotency keys

`Enqueue` with an `idempotency_key` is safe to retry: a second call with the
same `(queue, key)` returns the existing job's id and inserts nothing.

- Enforced by the partial unique index on `(queue, idempotency_key)`.
  `Enqueue` does `insert ... on conflict (queue, idempotency_key) where
  idempotency_key is not null do nothing returning id`, then falls back to a
  `select … for key share` when the insert returned
  nothing. The lock is there so that the answer's own commit waits for a
  synchronous standby (§11 item 7). Under 1000 concurrent enqueues
  with 10 distinct keys, exactly 10 rows exist. The fallback has to be a
  second statement: one statement sees one snapshot, taken before the
  concurrent inserter committed, so a CTE that tried to do both would find
  nothing to fall back to.
- Keys are scoped per queue. The same key in two queues is two jobs.
- The duplicate's `payload`, `priority`, and `run_at` are ignored; the first
  writer wins and the caller is not told the payload differed.
- **Protection lasts exactly as long as the row exists.** A key on a
  `completed` job still dedups until retention (§10) deletes the row; after
  that the same key creates a new job. A key on a `dead` job dedups until the
  job is purged. If you need permanent dedup, keep the key in your own store.
- Cron uses `idempotency_key = name || '@' || next_run_at`, so a leader
  failover re-firing a tick deduplicates instead of inserting twice — within
  the schedule's queue, and while the first row exists (§13).
- **`EnqueueBatch` is not atomic.** The broker checks every item first —
  required fields, JSON *syntax*, priority and attempt ranges, and protobuf
  timestamp validity — and a failure there writes nothing. PostgreSQL then
  applies its own rules (valid JSON containing `\u0000`, or a number outside
  `numeric` range, is not acceptable `jsonb`), so a database rejection or
  failure can leave earlier items committed and returns a sanitized `INTERNAL`.
  Do not read the prevalidation as "no invalid input leaves writes". Retry a
  failed batch whole with stable per-item keys: each item that deduplicates
  costs one wait for a synchronous standby; the committed items
  deduplicate, but only while their rows exist (retention or purge ends it). The
  producer client does not retry for you.
- **Message ceiling.** The broker enforces a 4 MiB limit on each incoming and
  outgoing gRPC message. A response can exceed it after every store call has
  already succeeded, so `RESOURCE_EXHAUSTED` from `EnqueueBatch` does **not**
  mean nothing was written; retry with the same keys and a smaller batch. The
  same limit applies to `ListJobs` pages: request a smaller `limit`.
- Wire timestamps that are not valid protobuf `Timestamp` values are
  `INVALID_ARGUMENT`, checked before any conversion.

## 9. Ordering and fairness

- **No ordering guarantee across jobs.** Within one queue, `Lease` prefers
  higher `priority`, then earlier `run_at`, but that is the order rows are
  handed out, not the order they finish, and retries, backoff, and multiple
  workers reorder everything after that.
- **No per-key ordering** (no FIFO groups). If job B must run after job A,
  enqueue B from A's handler.
- **Priority can starve.** A steady stream of priority-10 jobs will keep
  priority-0 jobs waiting indefinitely. Use separate queues with separate
  workers if you need isolation.
- `Lease` over several queues orders across all of them
  by `priority desc, run_at`, so a busy queue can dominate a stream that
  also subscribes to quiet ones. It reads each queue through `jobs_lease_idx`
  in that order and sorts only the best n of each (§4); `ListDead` similarly uses
  `jobs_dead_idx` and incrementally sorts its `id` tiebreaker. The other hot
  sweeps use their indexes without an explicit sort.
- **Idle streams are woken, and poll as a fallback.** A stream is idle once a
  lease has returned fewer jobs than it asked for while it still has credit;
  it then waits up to `-poll-interval` (default 250ms) for a wakeup.
  - An `Enqueue` or `EnqueueBatch` that inserts an `available` job wakes one
    idle stream on that broker that serves the job's queue: one job, one
    stream.
  - If no stream on that broker is idle, the job is announced to the other
    brokers with PostgreSQL `NOTIFY` on channel `hopper_available`. Each broker
    holds one connection that `LISTEN`s, and wakes one idle stream for each
    queue announced. A stream whose lease was already running when the job
    arrived leases again before it waits, so it cannot sleep through the job.
  - Announcements made while a broker is not listening are lost, so whenever
    its listener (re)connects it wakes all of its streams.
  - A broker sends one `NOTIFY` statement at a time, carrying every queue
    still to announce, so announcements coalesce under load.
  - Returned credit wakes a stream only if it had none left.
  - Both halves are hints. Without a wakeup, a stream still leases again after
    `-poll-interval`. That bounds what a lost wakeup costs, and it is the only
    path for jobs that become available without an enqueue: promoted delays and
    retries (§4) and redrives.
  - At 20 jobs a second, pickup was 1.5 ms p50 / 3.2 ms p99 with wakeups and
    88 ms / 239 ms polling alone, on one broker. Under
    sustained load streams are rarely idle, so wakeups gain nothing there; at
    3,000 jobs a second on one broker they cost enqueue latency (p99 8.1 ms
    polling, 25.8 ms with wakeups). There is still no strict delivery bound.
- **`ListJobs` order is by ID, not by creation.** Pages are keyed on the last
  ID seen. UUIDv7 ids follow application clocks, so order approximates but does
  not equal `created_at` or commit order. A traversal is live, not a snapshot:
  rows inserted or changed behind the cursor may be missed, and a cursor whose
  row was since deleted still works. It has no dedicated index and walks the
  primary key. Session 11's load generator reads each run back through it
  once; at tens of thousands of rows the walk did not matter, so it stays
  unindexed.

## 10. Cancellation and retention

**Cancel(id)**

| Current state | Effect |
|---|---|
| `scheduled`, `available` | → `cancelled`. Never runs. |
| `leased` | → `cancelled` immediately in the store. The worker learns at its next heartbeat/ack (`ErrCancelled`) and the SDK cancels the handler context. A handler may already be running: a delivery started under the credit rule (§4) learns at its first renewal, G/3 after start, or at its `Ack`/`Nack`. Side effects already performed are **not** undone. |
| `completed`, `dead`, `cancelled` | No change; returns `ErrTerminal` with the current state. |

Store method: `Cancel`, which returns the cancelled row. `ErrTerminal` is
delivered as a `*store.TerminalStateError` carrying the state, so a caller can
use `errors.Is` for the class and `errors.As` for the detail.

**Retention.** A janitor deletes `completed` and `cancelled` rows older than
a configurable window (default 24h; store method: `DeleteFinished`, ticker:
`sweep.Janitor`). `dead` rows are kept until purged.
Retention bounds table and index size and, per §8, is also the lifetime of
idempotency protection. The broker's `-retention` window must be positive
(default 24h); a nonpositive value is rejected before migration, sweeps or
network serving start, so it can never reach the janitor.

## 11. Where duplicates come from (the honest list)

Every duplicate execution corresponds to a lease that was granted and not
acked, so `attempts` should account for every one. These are the ways a
lease is granted and not acked:

1. **Worker crashes or is `SIGKILL`ed mid-handler.** Lease expires, job is
   redelivered after backoff. Expected; this is at-least-once.
2. **Handler runs longer than the lease and heartbeats stop** (worker
   partitioned from the broker, or the heartbeat goroutine starved). The
   reaper takes the job back while the original handler is still running.
   Two executions can overlap in time. The SDK retries only transient
   `UNAVAILABLE` and `DEADLINE_EXCEEDED` transport failures while its last
   locally known lease deadline remains live. Definitive lease loss or
   cancellation and local expiry stop work. Generic `INTERNAL` failures keep
   the no-blind-retry policy because the mutation may have committed and the
   cause may be permanent. The SDK cancels the handler's context when it must
   stop, but a handler that ignores its context keeps going.
3. **The ack-loss window.** Handler finishes → worker sends `Ack` → the
   broker or Postgres is unavailable before the `UPDATE` commits. The work is
   done but the store does not know. The SDK retries `Ack`/`Nack` only for
   `UNAVAILABLE` and `DEADLINE_EXCEEDED`, and a broker answers `UNAVAILABLE`
   whenever PostgreSQL cannot be reached (§5), so both a broker outage and a
   database outage are retried. How long a worker can keep retrying follows one
   rule. For a delivery granted a lease of G (Compose: 30s), with
   I = G/3 and a safety margin of min(0.5s, G/10):

   - **The local deadline** D is the send time of the last *successful*
     ownership check or renewal, plus G, less the margin; for a fresh delivery,
     which starts without a check, it is the send time of the credit unit the
     delivery was matched to (§4). A failed call never moves it. Once D passes, the worker gives the delivery up, whatever it was
     doing.
   - **While the handler runs,** renewals are one interval I apart, counted
     from when the previous renewal *returned*, not when it was sent. A renewal
     that fails with `UNAVAILABLE` or `DEADLINE_EXCEEDED` is retried at the next
     slot, each attempt's deadline being the earlier of I and D. The handler
     keeps its lease only if some renewal is sent after the outage ends, before
     D, and succeeds.
   - **After the handler returns,** the `Ack` or `Nack` is retried with backoff
     (100ms doubling to a 5s cap, each step ×0.75–1.25) until D. It gets
     through only if an attempt lands after the outage ends and before D. The
     time it has is D minus the moment the handler returned: at most G less the
     margin, and close to nothing when the last renewal failed, because D is
     still where the last success put it.
   - **Before the handler starts,** a fresh delivery (§4) takes no check: its D
     is the matched credit's send + G − margin, at least 0.8 G after receipt. A
     stale one takes the ownership check, retried with the same backoff until
     receipt + G − margin. Running out means a redelivery, not a re-execution.

   At G = 30s (I = 10s, D = last success + 29.5s), each row holding only under
   its assumption:

   | Delivery at the outage | Assumption | Survives an outage shorter than |
   |---|---|---|
   | Handler running | every renewal returns within milliseconds, and the last one before the outage succeeded | **I = 10s**: it fails at most one slot, and the next slot, about 20s after the last success, is before D |
   | Handler running | a failing renewal takes d to return | the next slot moves out by d. If d exceeds about I − 0.5s less the last success's round trip, the next slot falls after D and the lease is lost, even to an outage shorter than I |
   | Handler returned, short job | its ownership check was its last successful call, moments before | about 29s, less up to one backoff step (≤ 6.25s) before the next attempt |
   | Handler returned, short job, a fresh delivery | no check; D is its credit's send + 29.5s and the delivery arrived less than 3s after that send | at least 26.5s after receipt (D is the credit's send + G − margin, not receipt + G − margin), less up to one backoff step |
   | Handler returned just before its next renewal | the last renewal succeeded; RPCs return within milliseconds | about 19.5s, less up to one backoff step |
   | Handler returned after a failed renewal | none | whatever is left of D, possibly nothing |

   Two things can make a short outage fatal: a renewal that is slow to fail,
   and a handler that returns after a failed renewal. The rule is pinned by
   `worker: TestAFastFailedRenewalCostsOneInterval`,
   `TestASlowFailedRenewalCanSpendTheLease`,
   `TestAConfirmationHasOnlyWhatIsLeftOfTheLease` and
   `TestALateConfirmationStillLandsBeforeTheDeadline`, which pin the check
   path with an unexported option, since a fresh delivery takes none; the start
   rule by `worker: TestFreshDeadlineCountsFromTheCreditSend`,
   `TestAFreshDeliveryStartsWithoutTheOwnershipCheck`,
   `TestAStaleDeliveryStillTakesTheCheck`,
   `TestACancelledJobThatStartedStopsAtItsFirstRenewal` and
   `TestACancelledJobThatFinishesFirstIsRefusedAtItsAck`;
   `integration: TestAckRidesOutADatabaseOutage` and
   `TestHeartbeatRidesOutADatabaseOutage`; and the chaos runs below (§15).

   Batching does not move this window. A confirmation is answered only after
   the statement that applied it committed. A batch the database cannot
   complete fails every call in it with the same status, so an outage is still
   `UNAVAILABLE` to each call, and each call is retried under its own token. A
   call whose deadline passes while it is queued is dropped before its batch
   runs. A batch's statement runs while any of its calls still waits, for at
   most 10 s, and is cancelled when the last one leaves. A call whose own
   deadline passes while its batch runs is answered `DEADLINE_EXCEEDED`, may
   still have been applied, and a retry treats that as the idempotent case.

   **A batched `Ack` could be held, not answered `UNAVAILABLE` (fixed
   2026-11-23).** A batch's flight is shared. If its statement was stuck
   obtaining a connection to a database that had just gone (a dial with no
   answer), every later `Ack` queued behind it, at the default two flights,
   until the database returned or the 10 s cap ended the statement. The held
   calls then succeeded, or failed, without the fast `UNAVAILABLE` that an
   unbatched call gets from its own failed dial. Nothing was lost or repeated,
   and no call was answered `INTERNAL`; the hold was bounded by that cap and by
   each caller's deadline. Measured on 2026-11-20 with the graceful PostgreSQL
   stop: 3 of 8 runs at a batch of 64 and two flights held the gated `Ack`s (7
   or 8 `UNAVAILABLE` answers, against 36 to 50 in the passing runs), 0 of 8 at
   a batch of 1, and 0 of 8 at a batch of 64 with 16 flights. The chaos test
   that asserts every gated `Ack` was answered `UNAVAILABLE` therefore failed
   intermittently with batching on. On 2026-11-23 at `b553da7` it failed 1 of 5
   (stopped at the first failure; 9 answers against 21 to 53): after the
   brokers' last answer 100 ms into the outage, none came for 3 s, though other
   callers' dials were failing fast, and the held `Ack`s completed about 0.1 s
   after PostgreSQL listened again, which fits a dial into a torn-down
   container network that got no reply. That is inferred from timing; the
   store tests below hold a batch on a listener that never answers and measure
   it directly (5.0 s, the connect timeout).

   **The fix.** `AckMany` and `ExtendLeaseMany` acquire their connection within
   `batchAcquireTimeout` = 1 s, a dial included. When that bound passes first,
   every call in the batch is answered `UNAVAILABLE`; nothing was sent, so the
   retry is plainly safe. A caller whose own deadline or cancellation comes
   first still gets it back as its own. Once a statement has a connection it
   keeps the 10 s cap and the last-caller cancellation, and the one-row paths
   are unchanged. The cost: a pool that stays full for over a second answers
   `UNAVAILABLE` after 1 s and the workers retry with backoff, where they used
   to wait (the replication-pause runs saw acquires wait 13.3 to 14.2 s, §15);
   confirmations are idempotent under their lease token. Pinned by
   `postgres: TestABatchWhoseDialHangsAnswersUnavailableWithinTheBound`,
   `TestABatchWithNoConnectionFreeAnswersUnavailableWithinTheBound` and
   `TestACallerDeadlineInsideTheBoundStaysTheCallers`. Measured on 2026-11-23
   at `bee4ca7`: `TestPostgresRestartLosesNoAck/graceful` at the default batch
   passed 8 of 8, with 42 to 55 `Ack`s answered `UNAVAILABLE` per run (against
   9 to 53 before), 0 `INTERNAL`, and the bound's own error in the brokers'
   logs in 7 of the 8 runs, 2 `Ack`s each
   ([bench/1b/flight-bound/](bench/1b/flight-bound/graceful-8.txt)). Design:
   [design/confirmation-batching.md](design/confirmation-batching.md).

   Measured on Compose (2026-09-30): PostgreSQL outages after which new
   sessions failed for between 2.95s and 3.41s, with six three-second handlers
   returning into the outage, re-executed nothing, by fast shutdown and by
   SIGKILL with crash recovery (eight runs: the brokers answered 34 to 51 `Ack`
   calls `UNAVAILABLE` and none `INTERNAL`). A stop of 40s (new sessions failed
   for between 40.09s and 40.4s), which outlasts any lease, re-executed 12
   handlers in each of four runs, every one logged by its worker as given up
   (`job left unconfirmed; its lease ran out…`), and lost no job. Until
   Session 10 the brokers answered these calls `INTERNAL`, which the SDK never
   retries, and the same gated outage (new sessions failed for between 3.06s
   and 3.28s) re-executed 7 handlers after a fast shutdown and 12 after a
   SIGKILL.

   There is no way to close this window entirely without a transactional
   handoff between the handler's side effects and the queue; that is what
   idempotent handlers are for.
4. **Graceful shutdown deadline exceeded.** On `SIGTERM` the worker stops
   accepting deliveries and drains acquisition, handlers, and confirmation
   under one SDK deadline. That deadline must fit inside the deployment's
   termination grace budget, after any preStop delay and with process-exit
   overhead left over. A delivery still active at the deadline is abandoned,
   its handler context is cancelled, and its lease expires normally. The SDK
   reports separate active-delivery, running-handler, acquisition, and
   confirmation counts; `cmd/worker` logs the abandonment and exits nonzero.
   Size the budget to the slowest handler, and remember that an uncooperative
   Go handler cannot be forcibly stopped.

   On Kubernetes the grace period counts from pod deletion and includes the
   preStop hook, so `terminationGracePeriodSeconds ≥ preStop +
   shutdown-timeout + exit allowance`. The Helm chart refuses to render values
   that break `preStop + budget + 5s`, and derives `HOPPER_SHUTDOWN_TIMEOUT`
   from the same value it checks. Defaults: worker 5 + 45 + 5 ≤ 60, broker
   5 + 30 + 5 ≤ 45. The preStop delay is the native `sleep` action: the image
   has no `sleep` binary, and a failed exec hook would silently skip the delay.
5. **A lease stream fails before delivery is confirmed by subsequent work.**
   The broker leases rows, but a disconnect may leave some buffered or never
   received by the worker. Send success is not an application receipt. Unacked
   leases consume attempts and are reaped under the normal retry policy; they
   may eventually become dead without handler execution. `-max-lease-batch`
   bounds one store call, not all unreceived or unacknowledged jobs on a stream.
   Do not immediately release/nack them: some may already be executing.

   This uncertainty is different from a delivery the SDK has received and
   knows it never started, such as an unregistered queue or a delivery that
   lost the acceptance race with shutdown. After the start rule (§4) or bounded
   ownership validation, the SDK Nacks that known-unstarted delivery immediately. Its lease already
   consumed an attempt, so the Nack schedules the normal retry and can move the
   row to `dead` on its final attempt.
6. **A failover under asynchronous replication.** A commit returns before any
   standby has it, so the WAL the primary had not yet streamed dies with it.
   A lost `Enqueue` is a job that no longer exists, although its producer was
   told it did. A lost `Ack` puts a completed job back to `leased`, so the
   reaper takes it back and it runs again. A lost lease makes the worker's
   confirmation `ABORTED`. On kind, a crash with replication keeping up lost
   0 acknowledged jobs in 3 runs (of 21,175). With replication paused for 3 s
   first, each run lost everything acknowledged in those 3 s, 652 to 694 jobs
   a run, and 10 to 22 acked jobs ran again (§15). Under asynchronous
   replication a lease lost in a failover before a fresh delivery's handler
   starts (§4) is learned at the first renewal or the confirmation, so that
   handler can run beside the redelivery. Use synchronous
   replication (§1).
7. **A synchronous wait that was cancelled.** PostgreSQL writes a commit,
   then waits for the standby, and the transaction stays invisible while it
   waits. If the wait is cancelled, PostgreSQL keeps the commit and makes it
   visible with only a warning, though no standby has it. hopper runs every
   statement under its caller's context, and pgx cancels a statement whose
   context ends, so when standbys fall behind for longer than a caller's
   deadline, a write that timed out leaves a row only the primary has. Until
   bf90316 the retry was answered from that row at once
   (`deduplicated`, or "already applied" for `Ack`, `Nack` and `Cancel`), and
   a crash then lost the acknowledged job: 14 of 19,478 in 3 crashes with
   replication paused 3 s. Now the read behind each of those answers takes a
   row lock, so its own commit waits for a synchronous standby, and WAL order
   means a standby that has it has the row it read
   ([design](design/cancelled-waits.md)). The same paused crashes lost 0 of
   21,848 acknowledged jobs in 3 of 3 runs, and no retry was answered
   while the standbys were paused (§15).

   The window still opens after every promotion. The new primary had no
   synchronous standby until 19.9–26.0 s after the crash, and
   14–52 waits a run were cancelled in that time, leaving rows on it
   alone. Before the fix, 14–17 retries a run were answered
   deduplicated in that window; now nothing, to producers or workers, is
   answered until the new primary has a synchronous standby
   (8 runs, §15). Producers are therefore answered again a little
   later after a crash, 19.7–25.8 s against 19.3–20.3 s before. What is
   left: reads (`GetJob`, `ListJobs`, queue depth) can show such a row before
   a standby has it, and a refusal (`ABORTED`, `FAILED_PRECONDITION`) can rest
   on one; a crash can take either back, which can repeat a job but loses
   nothing acknowledged (§12). No worker is handed such a row: a lease is a
   write, and its own commit waits.

Worker-facing error text is made valid UTF-8, stripped of NUL bytes, and bounded
to 4,096 bytes (including its truncation suffix) before logging or persistence
in `last_error`. Logs distinguish `handler finished` from `job confirmed`, so a
handler return is never presented as a committed queue outcome. Handler
registration is setup-only: call `Handle` before the single `Run`. Handlers must
deduplicate their own effects with `Job.ID` or another stable business ID.

What is **not** a source of duplicates: two workers receiving the same lease
(prevented by `SKIP LOCKED`), a broker restart (brokers are stateless; workers
reconnect and their leases keep ticking) *(verified by SIGTERM and by SIGKILL, §15)*, a PostgreSQL outage that every
in-flight delivery's lease outlasts, by the rule in item 3, or a scheduler failover (cron uses
idempotency keys and a guarded advance; §13 lists the two configuration and
retention windows in which a tick can still fire twice). Nor is a `kubectl
rollout restart` of workers, as long as the shutdown budget holds (item 4 and
§15). A rollout can still cause **redeliveries**, which consume attempts
without a second execution: a delivery received during shutdown is nacked
unstarted (item 5), and one the worker never received is reaped when its lease
expires. Nor is an autoscaler's scale-down, which deletes a worker pod exactly
as a rollout does, so the worker drains (item 4). In the verified scale cycle
scale-down found every worker idle and nothing was redelivered, because the 60s
stabilization window outlasts the drain's tail (§14, §15). A scale-down that
does catch a busy worker can still redeliver a delivery in flight (item 5).

A worker killed with SIGKILL is item 1: its interrupted handlers run again, each
under a new lease, and `attempts` accounts for every one (§15).

## 12. Non-goals

- **Exactly-once execution.** Not offered, and no queue can offer it across
  a network boundary; put idempotency in the handler.
- **Ordering** across jobs or per key.
- **Workflows, sagas, child jobs, or cross-job transactions.**
- **Multi-region or multi-Postgres.** One Postgres is the source of truth;
  brokers scale horizontally against it.
- **Payload inspection**, routing on payload contents, or per-job rate limits.
- **Unbounded retention.** Completed history is pruned; ship it to your logs
  or warehouse if you need it.
- **Authentication, authorization and transport security.** The broker is a
  trusted-network prototype: plaintext gRPC, loopback by default, no identities
  and no role separation. Anyone who can connect can enqueue, cancel, inspect
  and list every job, and can read lease tokens from `GetJob`/`ListJobs`, which
  is enough to ack or nack a job they do not hold. Binding a non-loopback
  `-listen` address opts into that trust model (the broker logs a warning); use
  it for controlled local or container demos only. TLS and RBAC are future work.
- **Per-schedule concurrency policy.** A cron tick fires whether or not the
  previous tick's job has finished; overlap is the handler's business.
- **Missed-tick backfill.** An overdue schedule fires once and skips ahead
  (§13).
- **A schedule management API.** Schedules are declared in `-cron-file`;
  there is no RPC for them yet.
- **Queue-independent tick identity (future work).** Closing the queue-change
  gap in §13 needs deduplication keyed on `(schedule, tick)` regardless of
  destination queue: a durable cron-occurrence table with its own uniqueness
  constraint, written before or with the enqueue. That is a new migration and
  table, which Session 6 excluded by scope — not because the gap is imaginary.
- **Tracing.** No spans or trace context yet; `job_id` and `lease_id` in the
  logs are the correlation mechanism (§14).
- **Per-job metric labels.** By design: job, lease and worker ids belong in
  logs, and a label per id would make series unbounded (§14).
- **Alerting rules.** None are shipped; the dashboard is for watching, not
  paging.
- **Counters as an exact ledger.** Transition counters can undercount in four
  narrow windows (§14). Reconcile against the database, which is the record.
- **A production database in the Helm chart.** The bundled PostgreSQL is one
  replica with no replication and no backups, a PVC that outlives
  `helm uninstall`, and a default development password.
  `postgres.mode=cloudnativepg` adds replication and failover through the
  CloudNativePG operator 1.30.2, still with no backups, no connection
  pooler, no monitoring and `sslmode=disable`.
- **A leader whose database session goes silent.** A scheduler leader's
  `Check` has no deadline of its own. A session that stops answering without
  being reset (a partition, not a crash) keeps the role, and the server keeps
  its lock, until TCP gives up, so cron pauses meanwhile. Nothing fires twice.
  Phase 5 measures it.
- **Reads and refusals that see a cancelled wait (§11, item 7).** A row whose
  wait for the standby was cancelled is visible on the primary at once.
  hopper no longer reports work as done on the strength of one, but
  `GetJob`, `ListJobs` and the depth metric can show it, and a worker can be
  refused because of one, until a standby has it or a crash takes it back.
  Closing that needs reads that wait for a standby, on every read; not
  designed. hopper also still cancels waits whose caller left: never
  cancelling a commit was rejected, because a standby outage would then fill
  every broker's pool ([design](design/cancelled-waits.md)).
- **Password rotation.** `postgres.password` is bootstrap-only: PostgreSQL
  reads it only when it initializes an empty volume. Changing it later updates
  the Secret but not the database, so brokers fail to authenticate on their
  next start. Changing it means deleting the PVC and its data.
- **Worker readiness that tracks an active lease stream.** Workers have no
  readiness probe, so "ready" means the container started. A rolling update
  keeps the worker pod count, not leasing capacity.
- **TLS, NetworkPolicy and Ingress for the chart.**
- **Broker readiness that tracks PostgreSQL health**, deliberately. During a
  database outage every broker would go unready and workers would see
  connection-refused instead of a broker that can say `UNAVAILABLE`. Liveness
  never depends on the database, which would turn an outage into a restart
  storm. Brokers do answer `UNAVAILABLE` during a database outage (§5), so a
  worker rides it out within its lease (§11.3).
- **Even lease-stream placement.** Unary calls are balanced per request by
  client-side round robin, and lease streams move as they age out, but each
  reopened stream lands on whichever broker is next in that worker's rotation,
  so placement is random across workers and an even split is only
  statistical. Exact balance would need a worker to hold one stream per
  broker and divide its credit among them, which changes the flow-control
  accounting in §4.
- **Scaling workers to zero.** `minReplicas` is at least 1. Scale-to-zero
  needs KEDA's activation threshold and accepts a cold start for the first
  job, and neither is designed here.
- **Choosing which workers to remove.** The replica set deletes pods without
  knowing which are busy; the drain (§11.4) is what makes that safe.
- **Autoscaling brokers, or several worker pools.** The chart scales one
  worker Deployment serving a fixed list of queues; brokers run a fixed count.
- **Production monitoring in the kind add-ons.** One Prometheus with a day of
  retention, no Alertmanager, no persistence.
- **Confirmation costs that batching left.** Confirmation batching is on by
  default (§5), but the ownership and confirmation path still costs:
  - `Nack` is not batched; each one is its own RPC and committed statement.
  - A locked or missed pair falls back to a one-row statement. In the H4 drain
    at four brokers, 9.4% of Heartbeat statements were one-row fallbacks and
    the median Heartbeat batch held 3.5 calls (10.4 and 3.8% at one broker).
  - Leasing is still one query per stream. A lease coalescer was not built: H1
    did not meet its gate at one broker (lease latency 1.25× from 4 to 16
    streams), though jobs/s fell by half as the same 128 slots were split over
    more streams, because each stream's query returns fewer rows.
  - The ownership `Heartbeat` before every handler was the largest remaining
    per-job cost (§4 gives the numbers). Since 2026-11-23 a fresh delivery
    skips it under the credit rule (§4).

## 13. Cron schedules and leader election

**What a schedule is.** One row of `cron_schedules`: a name, a destination
queue, a payload, priority and attempt limit, and an expression evaluated in
the row's IANA timezone (default UTC). The expression is a standard five-field
cron line, a descriptor such as `@hourly`, or `@every <duration>`. `@every`
works in whole seconds: sub-second parts are truncated, with a one-second
floor, so `@every 1500ms` ticks every second. The broker embeds `tzdata`
because its distroless runtime image has no zoneinfo database.

**Exactly one active scheduler.** Every broker runs the scheduler loop, but it
does work only while it holds a session-scoped advisory lock taken with
`pg_try_advisory_lock` on a dedicated connection held out of the pool for the
whole leadership term. Losers retry every `-scheduler-interval` (default 5s).
The lock is released by an explicit unlock or by the session ending, so a
leader that crashes, is `SIGKILL`ed, or loses its connection loses the role,
and another broker takes it on its next retry. Leadership costs the holder one
connection from its pool. The lock is per database: every hopper process
pointed at one database contends for one role. Setting `-scheduler=false`
takes a broker out of the contest.

**Why not Raft.** Mutual exclusion here needs no replicated log. PostgreSQL is
already the single source of truth, already the thing whose failure stops the
queue, and already holds the state a consensus group would be protecting. Raft
would add a second failure domain to protect data that lives in the first.

**The lock is not a fencing token.** A leader that is paused — a GC stall, a
suspended VM, a network path TCP has not yet declared dead — can keep acting
after PostgreSQL has handed the lock to another broker. Nothing about advisory
locks prevents that. Two properties of the data model make it harmless:

- Each tick is enqueued with `idempotency_key = <name>@<tick>`, where `<tick>`
  is the stored `next_run_at` formatted with exactly six fractional digits
  (§8). The key is a pure function of the schedule and the tick, so a paused
  leader and a new leader firing the same tick produce **one job**.
- The advance that follows is a compare-and-swap: `next_run_at` moves only if
  it still equals the tick that was fired. A displaced leader's advance names
  a tick the new leader has already moved past, matches no row, and is logged
  as `scheduler stale advance` rather than rewinding the schedule.

**The dedup key is scoped to a queue.** §8's unique index is
`(queue, idempotency_key)`, so `name@tick` identifies a tick *within a queue*.
Every claim in this section that a tick is deduplicated assumes the schedule's
queue did not change.

**Leadership is checked between passes, not between enqueues.** A term runs
check → pass → wait. A pass reads at most one batch (100) of due schedules
and never loops to drain a larger backlog. A leader that loses the lock
mid-pass therefore finishes that pass — at most one batch of schedules, two
statements each — and stops at the next check. The window is deliberate: a
per-enqueue check would narrow it without closing it, since the lock can move
in the gap between any check and the statement after it. Safety across the
window is the idempotency key and the guarded advance, exactly as for a paused
leader. An ambiguous check failure (a timeout) is treated as lost leadership,
and so is any store error: the term ends, the lock is released, and the role
goes back up for contention.

**Fire, then advance.** Per due schedule, the scheduler enqueues the tick and
then advances `next_run_at`, as two statements rather than one transaction. A
crash between them leaves the tick enqueued and the schedule un-advanced; the
next leader re-fires the same tick, the key deduplicates it, and the advance
succeeds. A failover therefore costs at most a repeated enqueue that inserts
nothing. `run_at` is the tick itself, which is never in the future, so the job
is written as `available` and waits for no promoter pass.

**The two honest gaps.** Both sit inside that crash window, and both work by
making the re-fire miss the row that should have deduplicated it:

- *Retention.* If the deduplicating row is deleted by retention (§10) or by
  `Purge` before the re-fire, the tick fires twice. Cron periods are normally
  much shorter than the retention window, but the window exists.
- *A queue change.* Because dedup is per queue, a schedule whose `queue` is
  changed while a tick is in flight can produce two jobs for one occurrence:
  one on the old queue, one on the new. A schedule's queue is part of its dedup
  identity. Changing it starts a new delivery lineage, the same way renaming a
  schedule does, so make that change between ticks. This is a property of a
  deliberate configuration change, not an ambient failure mode; §12 records
  what would close it.

**Catch-up: one fire, then skip.** An overdue schedule fires once, for its
stored tick, and `next_run_at` advances to the first occurrence strictly after
the database's current time. Downtime does not backfill, so a failover slower
than the schedule's period legitimately leaves a hole in the sequence of fired
ticks. What an ungraceful failover guarantees is that the tick *pending at the
moment of death* is fired, once — not that every wall-clock occurrence in
between is. A computed next occurrence that does not move strictly past the
fired tick marks the schedule as bad (below) rather than re-firing it forever.

Because the next occurrence is computed from the time of the pass, not from the
fired tick, an `@every` schedule is anchored to when the previous tick was
processed (floored to the second), not to a fixed lattice. Its phase can shift
after a late pass or a failover. Use a five-field expression when ticks must
land on particular wall-clock instants.

**Latency.** A tick fires within roughly one `-scheduler-interval` of its
instant, plus up to one more interval when leadership has just moved.

**Clocks.** Due-ness, the next occurrence, and a newly declared schedule's
first occurrence all come from the database's clock. `DueSchedules` returns
the `now()` that judged its rows, and the next occurrence is computed from that
value. Reconciliation runs before anything is due, so it reads the database
clock through the store's `Now` and computes each new schedule's first tick
from it. A broker with a skewed clock cannot manufacture ticks, from the moment
a schedule is inserted.

**Bad schedules.** A row whose expression or timezone does not parse — which
means it was edited outside the cron file — is skipped, and the rest of the
pass continues. It is logged once per leadership term per
`(name, expression, timezone)`, not on every pass.

**DST.** The instants below are observed behavior of `robfig/cron` v3.0.1,
pinned by `scheduler: TestNextAcrossDaylightSavingTransitions` for
`30 2 * * *` in `America/New_York`. They are not a guarantee this project
makes, and a parser upgrade that changes them is a reason to re-read this
section.

- *Skipped hour.* On 2023-03-12 the clocks jump from 02:00 to 03:00, so 02:30
  does not exist. The parser does not shift it to another instant that day;
  the day is skipped, and the next occurrence is 2023-03-13 02:30 EDT
  (06:30 UTC).
- *Repeated hour.* On 2023-11-05 the clocks fall back from 02:00 to 01:00, so
  the repeated hour is 01:00–01:59. 02:30 lies outside it and fires once, at
  2023-11-05 02:30 EST (07:30 UTC), followed by a normal occurrence 24 hours
  later. An occurrence *inside* the repeated hour (for example `30 1 * * *`)
  has not been measured; do not assume it fires once or twice.

**Management.** Schedules come from the JSON file named by `-cron-file`. It is
validated in full at process start, so a malformed file stops the broker
before it listens. The file is applied by the leader at the start of every
term and is authoritative: its schedules are upserted and every other name is
deleted. An upsert keeps the stored `next_run_at` unless the expression or
timezone changed, so a redeploy neither skips a pending tick nor re-fires a
past one. During a rolling deploy that changes the file, whichever broker
leads applies its own copy, and leadership may move. Without a file, the table
is left unmanaged: the scheduler fires whatever rows exist and prunes nothing.
There is no management RPC; that is future work.

## 14. Observability

Brokers and workers expose Prometheus metrics on their own HTTP listener, and
both log JSON to stdout. Metrics describe load; they are not an audit trail.
The database is the record of what happened to a job.

**Broker metrics** (`cmd/broker`). Every hopper metric is prefixed `hopper_`
so it cannot collide with another exporter's in a shared Prometheus.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hopper_queue_depth` | gauge | `queue`, `state` | Rows in `scheduled`, `available`, `leased` and `dead` at this broker's last successful sample |
| `hopper_queue_depth_last_sample_timestamp_seconds` | gauge | — | Unix time of this broker's last successful depth sample |
| `hopper_jobs_enqueued_total` | counter | `queue` | Rows inserted by `Enqueue`; deduplicated calls excluded |
| `hopper_jobs_completed_total` | counter | `queue` | Leases acked to `completed` |
| `hopper_jobs_failed_total` | counter | `queue` | **Failed attempts**, not failed jobs: Nacks applied, whether rescheduled or dead-lettered. A job that fails three times counts 3 |
| `hopper_jobs_dead_total` | counter | `queue` | Transitions into `dead`, by Nack or lease expiry |
| `hopper_lease_expirations_total` | counter | `queue` | Expired leases the reaper took back |
| `hopper_lease_streams_open` | gauge | — | Lease streams this broker is serving now |
| `hopper_job_wait_seconds` | histogram | `queue` | First lease minus due time (below) |
| `hopper_job_run_seconds` | histogram | `queue` | Ack minus lease, completed deliveries only |
| `hopper_db_pool_max_connections` | gauge | — | `-db-max-conns` |
| `hopper_db_pool_connections` | gauge | `state` | Connections `acquired` (including those held for the scheduler role and the wakeup listener), `idle` or `constructing` |
| `hopper_db_pool_acquires_total` | counter | — | Connections taken from the pool |
| `hopper_db_pool_empty_acquires_total` | counter | — | Acquires that found no idle connection and waited |
| `hopper_db_pool_empty_acquire_wait_seconds_total` | counter | — | How long those acquires waited |
| `grpc_server_*` | histogram, counters | `grpc_type`, `grpc_service`, `grpc_method`, `grpc_code` | The standard gRPC interceptor's metrics. Every lease stream that reaches `-lease-stream-max-age` counts as `LeaseJobs` `Unavailable`, so an error-rate alert should exclude `grpc_method="LeaseJobs"` |

**Worker metrics** (`cmd/worker`).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hopper_worker_inflight_jobs` | gauge | — | Handlers executing right now |
| `hopper_worker_handler_seconds` | histogram | `queue`, `status` | Handler wall time; `status` is `ok`, `error` or `panic` |

**The pool.** `-db-max-conns` (`HOPPER_DB_MAX_CONNS`, default 16) bounds each
broker's PostgreSQL connections. The scheduler role (§13) and the wakeup
listener (§9) each hold one for as long as they run, so the flag must be at
least 2, or 3 with `-scheduler` on, which leaves queries at least one; the
broker refuses a smaller value at startup. Brokers × `-db-max-conns` must fit
the server's `max_connections` (100 by default), with room for anything else
that connects; Compose and the chart run two brokers, 32 connections at the
default. The size is set only by the flag; a DSN with `pool_max_conns` is
refused. The pool metrics are read from pgxpool's own counters at scrape time.
Waits that grow with load mean the broker is short of connections, not CPU.

Both processes also export the client library's `go_*` and `process_*`
collectors. Nothing registers globally: each process builds one private
registry.

**The counting rule.** The PostgreSQL store reports each transition to an
observer **at most once, after the store call has seen definite commit
success, and before the call returns**. So:

- A retried Ack or Nack that finds its own earlier transition already applied
  reports nothing; the transition was counted when it happened. A
  deduplicated `Enqueue` reports nothing.
- A gRPC response lost after the store succeeded neither loses nor duplicates
  a count. The transition was counted before the response left, and the
  client's retry takes the already-applied path.
- **Counters never overcount. They undercount only when the store did not
  see commit success for a transition that did commit**, which happens in
  exactly four ways: the commit result was ambiguous (the connection dropped
  after `COMMIT` was sent); the context was cancelled after the database
  committed but before the driver read the result; the process died between
  commit and the observer call; or the observer panicked.
- Counters are per process. Each transition is committed by one process, so
  **sum counters across broker replicas**. A restart resets a process's
  counters, and `rate()` handles resets.

**They are not a ledger; the database is.** Reconcile against SQL, never
against counters.

The store is the only place that counts because it is the only place that
knows the facts. An RPC handler cannot tell a first Ack from a retried one,
does not know the job's queue or whether a Nack dead-lettered, and never sees
the reaper's or the scheduler's transitions.

**Cardinality.** `queue` is an arbitrary client string, and the API accepts any
non-empty queue name. Labelling every distinct value would let any client
create unbounded series. So the `queue` label comes from an allowlist,
`-metrics-queues` (`HOPPER_METRICS_QUEUES`), a comma-separated list of at most
100 names. **Every other queue is reported as `queue="_other"`**, in counters,
histograms and depth, where the unlisted queues' counts are summed. `_other` is
reserved. The label set is fixed at startup, and every series is pre-created at
zero, so a counter's first increment is visible to `rate()`. **Every broker
must use the same list**; otherwise one broker folds a queue into `_other`
while another labels it, and the replicated depth comparison below compares
different things. This bounds metrics, not the database: API admission is
unchanged. The worker needs no flag, because it labels only the queues it
registered handlers for. No metric is ever labelled with a job id, lease id,
worker id or error string. Those belong in logs, and per-worker breakdown
comes from Prometheus's `instance` label.

**Depth.** Every broker refreshes `hopper_queue_depth` every 5s from one
statement, four `count(*) … group by queue` branches joined with `union all`,
one per counted state. Each branch's predicate matches a partial index
(`jobs_lease_idx`, `jobs_promote_idx`, `jobs_reap_idx`, `jobs_dead_idx`), so its
cost follows the rows in that state rather than the table. One statement means
one snapshot, so a job moving mid-sample is counted exactly once. `completed`
and `cancelled` are not counted: they are most of the table, they are bounded by
retention rather than load, and nothing scales on them. `dead` is counted
because DLQ size is worth watching.

- **A drained queue reads 0; it does not disappear.** All four states are
  emitted for every labelled queue, zero-filled.
- **Scrapes see whole samples.** A sample replaces an immutable snapshot in one
  step, so a scrape never sees half an update.
- **A failed or invalid sample keeps the last snapshot.** It logs a warning and
  does not advance `hopper_queue_depth_last_sample_timestamp_seconds`. An
  invalid sample has an empty queue, an unknown state, a negative count or a
  duplicate row. Zeroing depth on a database error would tell an autoscaler the
  backlog vanished mid-outage. Instead, the broker exports the stale value with
  its age, and the reader decides.

**Read depth by dropping brokers whose sample is older than 15s, then `max by
(queue, state)`, never `sum`.** Every broker samples the same database, so two
healthy brokers each export the full depth, and `sum` would count every job
once per broker. `max` alone is not safe either: a broker whose sampling failed
right after it saw a backlog keeps exporting that backlog, and `max` would let
it outvote healthy brokers that saw the drain. The query is:

```promql
max by (queue, state) (
  hopper_queue_depth
  and on (job, instance)
  (time() - hopper_queue_depth_last_sample_timestamp_seconds < 15)
)
```

Total across queues with `sum(max by (queue) (…))`. The 15 is three sample
intervals (`metrics.DepthFreshness`). Watch the worst broker's sample age,
`time() - min(hopper_queue_depth_last_sample_timestamp_seconds)`, because `max`
would show the freshest broker and hide a stale one.

**When every broker is stale, the query returns nothing, not 0.** That is
accurate: no one knows the depth. An autoscaler must treat it as an error, not
as an empty queue. For KEDA's Prometheus scaler, that means
`ignoreNullValues: false`, so an empty result is an error rather than zero
depth; see **Autoscaling** below for what the error does.

**Depth cost.** N brokers each run the grouped count every 5s, and each branch
reads every row in its state, so the cost grows with the non-terminal backlog.
It has been measured only up to a 10k burst. At 9,717 available jobs on the
Compose stack (2026-09-22), all four branches used their partial index with a
bitmap scan and the statement took 2.5 ms from 248 shared-buffer hits. Right
after the burst the planner's row estimate was 26 against 9,717 actual, because
autovacuum had not analyzed the table yet; the plan did not change. Larger
scales are unmeasured.

```
Bitmap Index Scan on jobs_lease_idx    (actual rows=9717)  -> GroupAggregate
Bitmap Index Scan on jobs_promote_idx  (actual rows=0)
Bitmap Index Scan on jobs_reap_idx     (actual rows=65)
Bitmap Index Scan on jobs_dead_idx     (actual rows=0)
Planning Time: 0.698 ms   Execution Time: 2.502 ms   Buffers: shared hit=248
```

**Wait is lateness past due time, not time since enqueue.**
`hopper_job_wait_seconds` observes `leased_at − run_at` when a job is leased
with `attempts = 1`. It replaced the spec's enqueue-to-first-lease definition
on 2026-09-22.

- For an immediate job, `run_at` is the enqueue instant, so the two definitions
  agree.
- For a delayed job, it measures how late the job was, not the delay it asked
  for. That includes up to one promoter interval, and for a cron job it
  includes the scheduler's own lateness, because `run_at` is the tick.
- Retries are excluded: their wait is backoff the policy chose, not congestion.
- `Redrive` resets `attempts` and `run_at`, so a redriven job's next delivery
  is a fresh wait measured from the redrive.
- Both instants come from the database clock, so broker clock skew cannot
  distort it.

**Histogram resolution.** Job histograms use buckets from 5ms to 600s. From 10s
to 180s adjacent buckets differ by at most 1.5× (10, 15, 20, 30, 45, 60, 90,
120, 180); above that they are 300 and 600. When all of a rate window's
observations fall in one bucket, `histogram_quantile` reports near that
bucket's upper edge. So a p99 can overstate the true value by up to the bucket
ratio: about 1.5× below 180s, and up to 2× above it. In the 10k burst the
true wait p99, from SQL, was 91.5s; the dashboard's p99 read 119.7s over the
final 20 seconds of the drain, because 91.5 falls in (90, 120]. Use the
histogram for shape and trend, and SQL for an exact figure.

**Run is lease to ack, on the database clock.** `hopper_job_run_seconds`
observes `finished_at − leased_at` when an Ack completes a job. It is observed
for completions only: a failing handler's time belongs to
`hopper_worker_handler_seconds{status="error"}`, and mixing it in would let a
slow-failing dependency move p99 without any job finishing. It is deliberately
not the handler's time. It includes lease validation, confirmation and the
network, so the gap between broker run time and worker handler time is SDK and
transport overhead.

**gRPC.** The standard `go-grpc-middleware` Prometheus interceptor wraps every
RPC, with every method's series pre-created at zero. `LeaseJobs` is a stream
that lives as long as its worker, so its handling time is stream lifetime, not
latency. Latency queries filter `grpc_type="unary"`.

**Worker.** In-flight means handlers executing: from the moment a handler is
accepted to run until it returns. Capacity held for validation or confirmation
is not counted. A delivery that never starts, because no handler is registered
for its queue or it lost the race with shutdown, is never observed: no user
code ran. A handler that never returns stays in flight until the process exits.
The SDK emits these events through `worker.Observer` and imports no metrics
library; `cmd/worker` plugs in the Prometheus implementation. The observer runs
synchronously on the delivery's goroutine, so its latency is the delivery's:
time spent in `HandlerFinished` delays the Ack or Nack and uses lease time.

**Logs.** Both processes log JSON to stdout. `lease_id` joins a broker's lines
to a worker's for the same delivery. Every line about a job falls in one of two
scopes:

- **Delivery-scoped: carries `job_id` and `lease_id`.** Broker: `job leased`,
  `job acked`, `job nacked`, `lease send failed`, and `rpc failed` /
  `rpc rejected` for `Ack`, `Nack` and `Heartbeat`. Worker: `job started`,
  `handler finished`, `job confirmed`, `nacking an unstarted job`,
  `discarding a malformed delivery` (with the raw, unparseable `lease_id`), and
  the confirmation, heartbeat and lease-expiry lines.
- **Job-scoped: carries `job_id`, and no lease exists or the RPC takes none.**
  Broker: `job enqueued` (one per inserted or deduplicated job, including each
  entry of an `EnqueueBatch`), and `rpc failed` / `rpc rejected` for `Cancel`
  and `GetJob`. Scheduler: `scheduler tick fired`.

A rejected `EnqueueBatch` entry has no job. It logs `enqueue batch rejected`
with its `index`, and belongs to neither scope. `rpc failed` is an internal
error, logged at error level. `rpc unavailable` (warn) is a store call that could not reach the
database; the caller got `UNAVAILABLE`, and `err` has the driver's detail.
`wakeup listener stopped` (warn): the broker's `LISTEN` session ended, usually
with the database, and its lease streams poll until it reconnects.
`debug endpoint listening` (warn): logged when `-debug-listen` is set. Every other rejection, such as a stale-lease
Ack, is `rpc rejected` at debug level with its `code`. The broker's
per-delivery lines (`job enqueued`, `job leased`, `job acked`, `job nacked`)
are also at debug level, so they cost nothing by default and make the two logs
joinable when switched on. The worker's `handler finished` carries
`handler_ms`, the same duration the histogram observes, alongside
`duration_ms`, which counts from receipt of the delivery. Payloads are never
logged, and a lease id appears only under the `lease_id` key. The Java example
worker (`java/hopper-example-worker`) writes the same messages with the same
fields; `test/accounting` judges its logs unchanged (§15).

**Metrics endpoints.**

| Binary | Flag | Env | Default |
|---|---|---|---|
| `cmd/broker` | `-metrics-listen` | `HOPPER_METRICS_LISTEN` | `127.0.0.1:9091` |
| `cmd/worker` | `-metrics-listen` | `HOPPER_METRICS_LISTEN` | empty (disabled) |

The broker runs one per host and an autoscaler depends on it, so it serves
metrics by default, on loopback. Several workers per host is normal and a fixed
default port would make the second one fail to bind, so the worker is off
unless asked. An empty value disables the endpoint. A broker with metrics off
builds no registry and runs no depth sampler. Each endpoint serves `GET
/metrics` only, and logs `metrics listening` with its bound `addr`.

**When enabled, the endpoint is mandatory for the life of the process.** Both
listeners are bound before anything serves, and a bind failure stops the
process before it starts. If the metrics server fails at runtime, the broker
drains gRPC and exits non-zero, and the worker drains its handlers and exits
non-zero. A process that kept serving without metrics would leave an autoscaler
blind. At shutdown the metrics server stops after gRPC has drained, so it stays
scrapeable during the drain. It gets its own 2s budget, then a hard close.

**Trust model.** The metrics endpoints are plaintext and unauthenticated, like
the API (§12). They expose allowlisted queue names, counts and timings. They
never expose payloads, job ids or lease tokens.

**Profiling endpoint.** `cmd/broker -debug-listen` (`HOPPER_DEBUG_LISTEN`,
default empty, which is off) serves Go's runtime profiles under
`/debug/pprof/`: the index, heap, goroutine and the other named profiles, plus
`profile`, `trace` and `symbol`. It is for profiling under load
(`scripts/bench.sh` with `PROFILE=1`). It never serves
`/debug/pprof/cmdline`, because a `-dsn` flag carries the database password.
Profiles still reveal code paths and allocation sites, so bind it to loopback
or a private network. It has no write timeout, so a profile streams as long as
asked. A bind failure stops the broker at startup. A failure later is logged,
and the broker keeps serving.

**Autoscaling reads depth the same way.** With `worker.autoscaling.enabled`,
the chart renders a KEDA `ScaledObject` for the worker Deployment. It asks for
one worker per 50 of this query (`AverageValue`), between 1 and 20. For
release `hopper` in namespace `hopper` the query is:

```promql
sum(max by (queue) (
  hopper_queue_depth{namespace="hopper", job="hopper-broker", state="available",
                     queue=~"sleep|fail_n_times|cpu_burn"}
  and on (job, instance)
  (time() - hopper_queue_depth_last_sample_timestamp_seconds{namespace="hopper", job="hopper-broker"} < 15)
))
```

- **Fresh-only, `max by (queue)`, then one `sum`**, as above. KEDA's
  Prometheus scaler rejects a result with more than one series.
- **Scoped to this release's brokers.** The Prometheus Operator labels a
  ServiceMonitor's targets with the Service's `namespace` and `job`. Without
  the scope, another release scraped by the same Prometheus would add its
  backlog, and while every one of this release's brokers is stale, another
  release's fresh broker would keep the result non-empty and hide the outage.
- **Only the queues the workers serve.** A backlog no worker can take,
  including everything folded into `_other`, would otherwise hold every worker
  at the maximum. The list is not a value: the example worker registers a
  fixed set of handlers, and the chart keeps a copy that a test checks against
  `cmd/worker`. The chart refuses to render autoscaling when
  `broker.metricsQueues` omits a served queue, because that queue's depth would
  be reported as `_other`.
- **Available jobs only.** Leased jobs already have a worker, and scheduled
  jobs are not due.
- **Scraped every 5s.** A sample can be one scrape interval plus one 5s sample
  interval old at query time, and the query drops a broker whose sample is 15s
  old. kube-prometheus-stack's default 30s interval would drop healthy brokers
  most of the time, so the ServiceMonitor and PodMonitor fix `interval: 5s`.

The spec's form without the three matchers reads 6,099 instead of 400 on the
promtool series, and 999 instead of empty when this release's brokers are all
stale.

**Unknown depth is an error, and an error never removes workers.** With
`ignoreNullValues: "false"`, KEDA's scaler reports an empty result as an error.
Without a fallback, KEDA then gives the HPA no metric, and the HPA keeps its
current count; that alone prevents a scale to the minimum mid-outage. The
fallback (`failureThreshold: 3`, `replicas: 4`,
`behavior: currentReplicasIfHigher`) makes the blind period a policy: after
three consecutive failures KEDA reports enough to keep the current count, or to
raise it to 4 if it is lower. So the scaler never removes a worker while it
cannot see the backlog, and a backlog building where it cannot see it still has
four workers. The chart requires `minReplicas ≤ fallbackReplicas ≤
maxReplicas`. On a fresh install the depth is unknown until Prometheus first
scrapes the brokers, so the fallback can briefly raise the workers to 4. §15
records the chain, observed with the database refusing every broker.

**Scale-down waits for 60s of low backlog.** KEDA's `cooldownPeriod` applies
only when scaling to zero, so the chart sets none. The 60s is the HPA's
scale-down stabilization window
(`worker.autoscaling.scaleDownStabilizationSeconds`): the HPA removes workers
only when every recommendation in the last 60s was below the current count.
Scale-up keeps the HPA defaults. The replica set cannot know which workers are
busy, and the drain (§11, item 4) is what makes deleting any of them safe. In
the verified runs the window also outlasted the drain's tail. The last 950
available jobs, the backlog below which 20 workers are no longer wanted, drain
in about 12s at the measured 78 jobs a second, and the HPA removed the first
worker 38s after the available depth read 0, so every removed worker was idle
at SIGTERM (19 removed, 0 busy; §15). A shorter window or longer jobs would
let scale-down catch busy workers. They would drain as in a rollout, but a
delivery in flight at SIGTERM can still be redelivered (§11, item 5).

**The HPA owns the worker count.** The HPA writes `spec.replicas` through the
Deployment's scale subresource, which makes it the field's owner under
server-side apply, so while autoscaling is enabled the chart renders no
`replicas` for the workers and `worker.replicas` is ignored. Helm upgrades
then leave the count alone. Switching autoscaling off needs
`helm upgrade --force-conflicts` (`scripts/kind-up.sh --set worker.replicas=N
--force-conflicts`), because Helm must take the field back. Helm applies the
Deployment before it deletes the ScaledObject, so if the HPA's 15s sync falls
between the two, the count ends at the HPA's value: check the count after the
upgrade, and repeat it once if it is wrong. The second run has no HPA to race.

## 15. Verified behavior

Only completed, dated runs appear here. Each row names the test or scoped demo
that produced the evidence; prospective Session 7–11 checks stay in the plan,
not in this table.

| Claim | Test | Runs | Result |
|---|---|---|---|
| Concurrent `Lease` calls never return the same job | `store: TestLeaseDisjoint` (50 goroutines × 4 over 200 jobs, `-race`) | 2 (`-count=2`, 2026-09-18) | pass |
| Stale-lease `Ack`/`Nack`/`Heartbeat` rejected | `store: TestStaleLeaseRejected` (all three paths) | 2 (`-count=2`, 2026-09-20) | pass |
| Lease expiry → redelivered exactly once more, `attempts` incremented | `store: TestLeaseExpiryRedelivers` (real 200ms lease, reaper, promoter stand-in) | 2 (`-count=2`, 2026-09-20) | pass |
| `max_attempts` failures → `dead`; `Redrive` → `available`, `attempts = 0` | `store: TestDeadLetter` | 2 (`-count=2`, 2026-09-20) | pass |
| 1000 concurrent `Enqueue` with 10 keys → 10 rows | `store: TestIdempotencyStorm` (10 keys × 100 callers released together) | 3 (`-count=3`, 2026-09-20) | pass |
| Priority-10 job leased before 100 priority-0 jobs | `store: TestPriority` (urgent job enqueued last, so FIFO would rank it 101st) | 2 (`-count=2`, 2026-09-20) | pass |
| Delayed job is leasable only after the promoter runs | `store: TestPromoteDueMakesDelayedJobsLeasable` | 2 (`-count=2`, 2026-09-20) | pass |
| `Cancel` of a leased job → holder gets `ErrCancelled` at its next call | `store: TestCancelLeasedJobIsLearnedByTheWorker` | 2 (`-count=2`, 2026-09-20) | pass |
| Retention deletes finished rows and releases their idempotency keys | `store: TestRetentionEndsIdempotencyProtection` | 2 (`-count=2`, 2026-09-20) | pass |
| Enqueue → lease stream → ack over gRPC leaves the job `completed` | `integration: TestEnqueueLeaseAck` | 2 (`-count=2`, 2026-09-20) | pass |
| Duplicate `Enqueue` over gRPC returns the first job with `deduplicated` | `integration: TestIdempotentEnqueueOverGRPC` | 2 (`-count=2`, 2026-09-20) | pass |
| Stale-lease `Ack` over gRPC is `ABORTED` | `integration: TestStaleAckOverGRPC` | 2 (`-count=2`, 2026-09-20) | pass |
| After a real reap and re-grant, the old lease token is `ABORTED` and the new one acks | `integration: TestStaleTokenAfterReapAndRelease` | 2 (`-count=2`, 2026-09-20) | pass |
| A cancelled leased job tells its worker `FAILED_PRECONDITION`; a retried `Cancel` succeeds | `integration: TestCancelledLeasedJobTellsTheWorkerToStop` | 2 (`-count=2`, 2026-09-20) | pass |
| A batch the database rejects part way leaves the committed prefix and a sanitized `INTERNAL` | `integration: TestBatchDatabaseRejectionKeepsPrefix` | 2 (`-count=2`, 2026-09-20) | pass |
| Retrying a failed batch with the same keys inserts nothing twice | `integration: TestBatchRetryAfterTransientErrorDoesNotDuplicate` | 2 (`-count=2`, 2026-09-20) | pass |
| A lease stream never hands out more jobs than the worker's credit | `broker: TestLeaseStreamNeverExceedsCredit` | 3 (`-count=3`, 2026-09-20) | pass |
| Busy streams stop on drain or half-close | `broker: TestLeaseStreamBusyTermination` | 3 (`-count=3`, 2026-09-20) | pass |
| A blocked `Send` does not stop drain or half-close from ending the handler | `broker: TestLeaseStreamBlockedSendTerminates` | 3 (`-count=3`, 2026-09-20) | pass |
| Broker shutdown ends lease streams so `GracefulStop` terminates | `broker: TestServerShutdownEndsLeaseStreams` | 2 (`-count=2`, 2026-09-20) | pass |
| Broker process runs maintenance and exits on SIGTERM with active streams | `integration: TestBrokerLifecycle` | 2 (`-count=2`, 2026-09-20) | pass |
| `ListJobs` visits every row once in ID order across mixed UUIDv4/v7 ids | `store: TestListJobsPagesInIDOrder` | 2 (`-count=2`, 2026-09-20) | pass |
| Real worker process receives `SIGTERM` mid-handler, exits zero, and commits that job once while leaving waiting work untouched | `integration: TestWorkerGracefulShutdownCompletesTheRunningJob` (`-race`) | 2 (`-count=2`, 2026-09-21) | pass |
| Real worker process exceeding its shutdown deadline exits nonzero, does not falsely complete the job, and the reaper schedules its retry | `integration: TestWorkerShutdownDeadlineReportsAbandonment` (`-race`) | 2 (`-count=2`, 2026-09-21) | pass |
| Drain cannot finish before stream intake, including a buffered delivery received during teardown | `worker: TestDrainWaitsForDeliveryReceivedBeforeIntakeFinishes` (`-race`) | 20 focused + 2 full-suite (2026-09-22) | pass |
| Shutdown force-cancellation observes the original absolute deadline even if lifecycle logging blocks | `worker: TestShutdownDeadlineIsEnforcedWhileStartLogIsBlocked` (`-race`) | 20 focused + 2 full-suite (2026-09-22) | pass |
| Every Heartbeat pins the delivery's original grant, and renewal-observed expiry cancels the handler | `worker: TestExecutionPinsHeartbeatsToTheDeliveredGrant`, `TestEveryRenewalPinsTheOriginalBrokerGrant`, `TestRenewalCancelsHandlerWhenItObservesAnExpiredLease` (`-race`) | 2 full-suite each; latter two also 20 focused (2026-09-22) | pass |
| The sanitized, bounded handler error logged by the SDK is the same value persisted by Nack | `worker: TestAFailingHandlerIsNackedWithASanitizedError` (`-race`) | 20 focused + 2 full-suite (2026-09-22) | pass |
| Three Compose workers complete 1,000 scoped jobs with no retries | `hopper-session5` scoped Compose demo (`s5-bulk-*`, SQL state check) | 1 (2026-09-21) | pass: 1,000 completed, 0 retried, 8s enqueue-to-completion |
| Compose worker stop accounts for every observed handler and every extra attempt | `hopper-session5` scoped Compose demo (`s5-shutdown-*`, JSON logs + SQL state checks) | 1 (2026-09-21) | pass: 12/12 started handlers confirmed; one captured unreceived lease explains the only second attempt; 300 completed, 0 dead |
| One scheduler is active across three instances; an ungraceful kill hands the role to a *different* instance within one scheduler interval (50ms, plus a stated 150ms election allowance), the tick pending at the kill is fired, and no occurrence fires twice | `integration: TestSchedulerFailover` (20 rounds per run, backend terminated, leader identified by `application_name`, `-race`) | 20 (`-count=20`, 400 rounds, 2026-09-22) | pass: takeover 6–74ms (median 39ms); no occurrence skipped; ~20s per 20-round run |
| A newly declared schedule's first tick is computed from the database clock, not the broker's | `scheduler: TestReconcileIgnoresTheProcessClock` (`-race`) | 5 (`-count=5`, 2026-09-22) | pass |
| A tick already fired by a previous leader deduplicates rather than double-firing | `scheduler: TestDuplicateTickIsNotReFired` (`-race`) | 5 (`-count=5`, 2026-09-22) | pass |
| A stale advance from a displaced leader matches no row | `store: TestAdvanceScheduleIsGuarded` (`-race`) | 2 (`-count=2`, 2026-09-22) | pass |
| Releasing leadership never returns a locked connection to the pool | `store: TestReleaseDoesNotPoisonThePool` (`-race`) | 2 (`-count=2`, 2026-09-22) | pass |
| Three real broker processes: one fires; `SIGKILL` of the leader, twice in succession, hands the role to another broker within one interval each time; every tick pending at a kill fires and no occurrence fires twice | `integration: TestBrokerProcessCronFailsOverOnKill` (three `-race` broker binaries, 50ms `-scheduler-interval`) | 20 (`-count=20`, 40 kills, 2026-09-22) | pass: takeover 7–50ms (median 28ms) |
| Two Compose brokers elect one scheduler; stop and `docker kill` of the leader both hand the role over, and cron jobs (including an `America/New_York` schedule in the distroless image) complete | `hopper-session6` Compose demo (`broker` + `broker-standby`, JSON logs + SQL state checks) | 1 (2026-09-22) | pass: graceful takeover in ~4s; after `docker kill` the pending tick fired ~2s late, then skipped ahead; 10/10 `sleep` jobs and the 18:30 UTC `cpu_burn` tick completed |
| Counters match the database for completions, failed attempts, dead-letters, lease expirations and dedup; wait counts first leases only; an unlisted queue counts under `_other` | `integration: TestMetricsMatchTheDatabase` (in-process broker + SDK worker, `-race`) | 2 (`-count=2`, 2026-09-22) | pass |
| A retried Ack or Nack is not counted twice; a lost response neither loses nor duplicates a count; a failed store call reports nothing | `store: TestObserverSeesCompletionOnce`, `TestObserverSeesNackOutcome`, `TestObserverCountsOnceAcrossALostResponse`, `TestObserverSilentOnStoreFailure` (`-race`) | 2 (`-count=2`, 2026-09-22) | pass |
| Unlisted queues fold into `_other`, and the label set stays fixed | `metrics: TestBrokerFoldsUnlistedQueues` (`-race`); `integration: TestMetricsMatchTheDatabase` | 3 and 2 (`-count=3`/`-count=2`, 2026-09-22) | pass |
| A broker that cannot reach the database keeps its last depth snapshot and a frozen sample timestamp and keeps serving; the dashboard query excludes it; all-stale is empty, not 0 | `integration: TestStaleBrokerKeepsItsLastSnapshot` (two broker binaries, one behind a severed TCP proxy); `promtool test rules deploy/prometheus/queries_test.yml` (`prom/prometheus:v3.14.0`) | 2 and 1 (2026-09-22) | pass |
| Two broker replicas report identical depth, so depth is read with `max by`; allowlisted counters exist at 0 before any traffic | `integration: TestBrokerReplicasReportTheSameDepth` (two broker binaries) | 2 (`-count=2`, 2026-09-22) | pass |
| A drained queue reads 0; a scrape never sees a half-updated snapshot | `metrics: TestDrainedQueueReadsZero`, `TestScrapeNeverSeesAHalfUpdate` (`-race`) | 3 (`-count=3`, 2026-09-22) | pass |
| Delivery-scoped lines carry both ids, job-scoped lines carry `job_id`, no job-bearing line escapes both scopes; broker and worker lines join on `lease_id`; no payload, and lease ids only under `lease_id` | `integration: TestJobLogsCarryIDsAndCorrelate` (`-race`) | 2 (`-count=2`, 2026-09-22) | pass |
| The depth query can use the four partial indexes; at peak burst backlog it does | `store: TestHotQueriesUseTheirIndexes/depth`; `EXPLAIN (ANALYZE, BUFFERS)` at 9,717 available on `hopper-session7` | 2 and 1 (2026-09-22) | pass: bitmap scans on `jobs_lease_idx`, `jobs_promote_idx`, `jobs_reap_idx`, `jobs_dead_idx`; 2.5 ms, 248 shared hits; estimate 26 against 9,717 actual (stale statistics) |
| A 10k burst drains on the dashboard; the completion counter delta equals the SQL count | `hopper-session7` Compose demo (scheduler off, 3 workers × 4, 100 ms `sleep` jobs, run `s7-1790136511`) | 1 (2026-09-22) | pass: peak fresh depth 9,747; drained in 95 s; peak 107 jobs/s; SQL completed 10,000 = counter delta 10,000. Wait p99 read 119.7 s at the end of the drain against a true p99 of 91.5 s (bucket resolution, §14) |
| Every shutdown budget fits its grace period, and the chart refuses values that do not | `chart: TestShutdownBudgetsFitTheirGracePeriods`, `TestChartRejectsGracePeriodsTooShortForTheirDrain` (`-race`, Helm 4.3.0) | 3 (`-count=3`, 2026-09-23) | pass: broker 5 + 30 + 5 ≤ 45, worker 5 + 45 + 5 ≤ 60; grace 39 (broker) and 54 (worker) refused, a raised budget or preStop refused too |
| PostgreSQL, broker, worker and test pods meet the restricted Pod Security Standard, enforced by the namespace on kind | `chart: TestPodsMeetTheRestrictedStandard` (4 pod specs); kind 1.35.8 namespaces `hopper` and `hopper-plain` labeled `enforce=restricted` | 3 (`-count=3`) + every kind install (2026-09-23) | pass: all pods admitted and Running, including PostgreSQL as uid 70; no `FailedCreate` events |
| The PostgreSQL password is only in the Secret; pgx completes the passwordless DSN from `PGPASSWORD` | `chart: TestPasswordLivesOnlyInTheSecret`, `TestPGPasswordCompletesThePasswordlessDSN` | 3 (`-count=3`, 2026-09-23) | pass; brokers on kind authenticate with the passwordless DSN |
| `deploy/k8s/hopper.yaml` matches the chart, and applied alone it runs a working queue | `chart: TestRenderedManifestsAreCurrent` (fails after a one-line edit, passes after re-render); `hopper-plain` namespace + `hopperctl smoke` | 3 (`-count=3`) and 1 (2026-09-23) | pass: 9 objects applied into a fresh restricted namespace; smoke job completed (13 s, the worker still connecting) |
| `helm install` on a fresh kind cluster gives a working queue | `helm test` (`hopperctl smoke`) after `kind delete cluster` + `scripts/kind-up.sh --set worker.replicas=3` | 2 passes + 1 negative (2026-09-23) | pass: bring-up from nothing in 65 s, job completed after 274 ms, no `FailedCreate` or `FailedPreStopHook` events. With `worker.replicas=0`, `helm test` failed: `did not complete within 1m0s (last state JOB_STATE_AVAILABLE)`; that job completed once workers returned |
| Execution accounting reports duplicates, incomplete capture, unfinished jobs, unclean or idle shutdowns, and nacks the rows cannot explain; it counts redeliveries per lost lease | `accounting: Test*` (13 tests, `-race`) | 3 (`-count=3`, 2026-09-23) | pass |
| `kubectl rollout restart` of three workers mid-run: every job's handler started exactly once; every replaced pod had handlers running at SIGTERM and stopped cleanly | `k8s: TestRolloutRestartDuplicatesNothing` (1,200 × 3 s `sleep`, 3 × 8 concurrency, restart after 50 completions) | 6 (`-count=3` twice, the second on a cluster rebuilt from nothing, 2026-09-23) | pass, each run: 1,200/1,200 completed, 0 duplicate starts; every replaced pod `running_handlers=8`, clean stop; 0 redelivered jobs, 0 redeliveries (0 unstarted nacks, 0 never received); ~148 s per run |
| The rollout proof fails when the drain budget is too short | the proof with `worker.shutdownTimeoutSeconds=1`, `preStopSleepSeconds=0`, grace 6 s | 1 (2026-09-23) | fails as intended: 2 of 3 replaced pods reached the shutdown deadline with 8 running handlers and exited with an error; 16 jobs' handlers started twice. The third pod's batch had started 2.1 s before SIGTERM and finished inside the 1 s budget |
| Accounting judges a run whose shutdowns all found workers idle, and still reports every other problem | `accounting: TestExecutionProblemsAllowIdleShutdowns`, `TestExecutionProblemsKeepEveryOtherCheck` (`-race`) | 3 (`-count=3`) + 2 full-suite (2026-09-27) | pass |
| The broker ServiceMonitor and worker PodMonitor scrape every 5s, inside the depth freshness window; they and the ScaledObject are off by default, so the plain manifests are unchanged | `chart: TestMonitorsScrapeEveryFiveSeconds`, `TestMonitoringAndAutoscalingAreOffByDefault`, `TestRenderedManifestsAreCurrent` (`-race`, Helm 4.3.0) | 3 (`-count=3`, 2026-09-27) | pass: 5 s scrape + 5 s sample < 15 s; `deploy/k8s/hopper.yaml` byte-identical to Session 8 |
| The worker ScaledObject scales on this release's fresh available depth for the queues its workers serve, with `ignoreNullValues: false`, a `currentReplicasIfHigher` fallback of 4 and a 60s scale-down window; the Deployment leaves `replicas` to the HPA; the chart's queue list matches `cmd/worker`; unsafe settings refuse to render | `chart: TestScaledObjectScalesWorkersOnFreshDepth`, `TestAutoscalerCountsTheWorkersQueues`, `TestChartRejectsUnsafeAutoscaling` (`-race`) | 3 (`-count=3`, 2026-09-27) | pass: 11 unsafe settings refused; reordering the chart's queue list fails the queue test |
| The scaler's query counts each job once across brokers, ignores a stale broker, another release, leased jobs and unserved queues, and is empty, not 0, when every broker is stale | `promtool test rules deploy/prometheus/queries_test.yml` (3 groups, `prom/prometheus:v3.14.0`); `chart: TestPromtoolTestsTheRenderedQuery` | 2 and 3 (2026-09-27) | pass: 400, 0 and empty. The spec's unscoped query fails two groups, reading 6,099 instead of 400 and 999 instead of empty |
| A scale cycle on kind: 20,000 × 2 s `sleep` jobs take the workers 1 → 20 → 1; every job's handler started once and every job completed on its first lease; every removed worker stopped cleanly | `k8s: TestAutoscalingRedeliversNothing` (KEDA 2.20.2, kube-prometheus-stack 91.7.0, cluster rebuilt from nothing; run `s9-1790489039308325000`) | 1 (2026-09-27) | pass: 20,000/20,000 completed, 0 duplicate starts; 20 running at +40 s (1 → 5 → 10 → 20); drained at +4m40s, about 78 jobs/s at 20 workers; the first worker removed 38 s after the available depth read 0; back to 1 at +5m46s; 19 removed, each `running_handlers=0` with a clean stop; 0 redelivered jobs, 0 redeliveries (0 unstarted nacks, 0 unexplained); cluster memory peaked at 2.63 GiB of 3.83 GiB, no evictions |
| When the database refuses every broker, depth becomes unknown (not 0), KEDA reports the fallback and holds idle workers at 4; allowed again, they return to 1 and a job completes | `k8s: TestUnknownDepthHoldsTheFallback` (`ALTER DATABASE hopper ALLOW_CONNECTIONS false` + `pg_terminate_backend`) | 1 (2026-09-27) | pass: depth unknown after 12 s, with both brokers alive, unrestarted and frozen (logging `queue depth sample failed … not currently accepting connections`); the fallback held 4 workers after 50 s; allowed again, both brokers fresh after 14 s and back to 1 idle worker after 1m4s; `datallowconn` restored |
| Accounting tells a repeat a nack, the worker's own loss record or an earlier kill explains from one nothing explains, and catches a lease with two handlers, more leases than attempts, an acked lease followed by another, and a kill that came after the job ran again | `accounting: TestAKillExplainsARepeat` and the rest of the per-lease tests (`-race`) | 3 (`-count=3`, 2026-09-30) | pass |
| Connection-class failures are marked unavailable, rejected statements are not, and every store error goes through the classifier; a terminated session's call does not take effect and its retry succeeds | `postgres: TestUnavailableClassifiesConnectionFailures`, `TestEveryStoreErrorGoesThroughWrap`, `TestADroppedConnectionIsUnavailable`, `TestARejectedStatementIsNotUnavailable` (`-race`) | 5 (`-count=5`, 2026-09-30) | pass |
| A broker answers an unreachable database `UNAVAILABLE` and logs it at warn | `broker: TestToStatus`, `TestFailureLogsCarryJobAndLease` (`-race`) | 3 (`-count=3`, 2026-09-30) | pass |
| An Ack and a renewal that meet a database outage are retried and the job completes on its first lease, its handler never cancelled | `integration: TestAckRidesOutADatabaseOutage`, `TestHeartbeatRidesOutADatabaseOutage` (`-race`) | 5 (`-count=5`, 2026-09-30) | pass |
| The local-deadline rule: a renewal that fails at once costs one interval, one slow to fail can spend the lease, and a confirmation has only what the last successful call left it | `worker: TestAFastFailedRenewalCostsOneInterval`, `TestASlowFailedRenewalCanSpendTheLease`, `TestAConfirmationHasOnlyWhatIsLeftOfTheLease`, `TestALateConfirmationStillLandsBeforeTheDeadline` (`-race`) | 3 (`-count=3`, 2026-09-30) | pass |
| An undisturbed Compose run completes every job on its first lease | `chaos: TestUndisturbedRunIsClean` (1,000 jobs) | 3 (2026-09-30) | pass: 1,000 completed, 0 repeated |
| Workers killed with SIGKILL mid-run lose no job, and every repeated handler start is one a kill interrupted before the job ran again | `chaos: TestKilledWorkersLoseNothing` (10,000 × 100 ms) | 3 (2026-09-30) | pass: 10 kills each; 39, 33 and 40 handlers interrupted; every repeat caused by a kill; 0, 7 and 0 redeliveries never received; 138–140 s |
| A broker restarted by SIGTERM or SIGKILL mid-run loses no job and starts no handler twice; every worker reconnects and handlers running through it are confirmed | `chaos: TestBrokerRestartLosesNothing/graceful`, `/crash` (3,000 jobs) | 3 each (2026-09-30) | pass: 3,000 completed, 0 repeated starts, 35–36 s each |
| **Before the fix,** a PostgreSQL outage re-executed handlers: brokers answered the gated Acks `INTERNAL` and workers gave them up | `chaos: TestPostgresRestartLosesNoAck` at `5d0a3bf` (the store change without the broker's) | 1 per subtest (2026-09-30) | fails as measured: new sessions failed for between 3.06s and 3.27s, 7 handlers started twice (graceful); between 3.07s and 3.28s, 12 (SIGKILL); Acks answered `INTERNAL` (7 and 12), every repeat `lost` |
| A PostgreSQL outage (fast shutdown, or SIGKILL and crash recovery) that every delivery's lease outlasts loses no ack and starts no handler twice; brokers never restart, every gated Ack met it and was answered `UNAVAILABLE`, and nothing was answered `INTERNAL` | `chaos: TestPostgresRestartLosesNoAck/graceful`, `/crash` (3,000 jobs + 6 gated 3 s jobs) | 4 each (2026-09-30) | pass: new sessions failed for between 2.95s and 3.41s; brokers logged store failures for 3.06–3.15s; 34–51 Acks answered `UNAVAILABLE`; 0 repeated starts |
| An outage longer than the lease re-executes handlers, each one its worker logged giving up, and loses no job | `chaos: TestAnOutageLongerThanTheLeaseReexecutes` (40 s) | 4 (2026-09-30) | pass: new sessions failed for between 40.09s and 40.4s; 12 handlers repeated, all `lost` (`its lease ran out`), the six gated jobs among them |
| Sixteen retrying producers through a broker crash and a PostgreSQL restart, with new keys arriving throughout, get exactly one row per key counted in SQL, and every answer names it | `chaos: TestEnqueueStormDeduplicates` | 4 (2026-09-30) | pass: about 1,000 keys and rows each; 16,399–17,168 calls, 142–179 retried; 6–14 keys in doubt; fails when retries use fresh keys |
| The chaos suite passes three times in a row | `go test -tags chaos -count=3 ./test/chaos` | 1 invocation (2026-09-30) | pass: 1,206 s |
| A lease walks `jobs_lease_idx` and its candidate scans produce at most n rows per subscribed queue whatever the backlog, plus only the rows another lessor holds, none of which it leases; a repeated queue still yields n distinct jobs; the batch is the best n across queues | `postgres: TestLeaseCostDoesNotGrowWithTheBacklog`, `TestLeaseCostSkipsLockedCandidates`, `TestLeaseWithARepeatedQueueLeasesDistinctJobs`, `TestLeaseOrdersAcrossQueues` (`-race`) | 3 (`-count=3`, 2026-10-13) | pass; the old statement read 6,000 rows for 10; candidate scans 6,000 rows and 80 shared blocks before, 21 rows and 8 blocks after (24 and 8 with five rows locked) |
| Announcements reach every listener; a listening session never returns to the pool; a terminated one is reported unavailable | `postgres: TestAnnounceReachesEveryListener`, `TestListenEndsWithItsContextAndKeepsNoSession`, `TestListenReportsATerminatedSession` (`-race`) | 3 (`-count=3`, 2026-10-13) | pass |
| An enqueue wakes one idle stream, not every one, and a burst reaches distinct streams; a job enqueued while a lease runs is not slept through; a short batch and returned credit cost no extra lease; only a job no local stream took is announced; delayed and deduplicated jobs wake nothing; announcements coalesce while one is in flight; the listener wakes every stream when it reconnects | `broker: TestAnEnqueueWakesOneIdleStreamNotEveryOne` and 17 more in `wake_test.go` (`-race`) | 3 (`-count=3`, 2026-10-13) | pass |
| An enqueue on one broker process wakes an idle stream on another within seconds, both polling hourly; each exports its pool | `integration: TestAnEnqueueOnOneBrokerWakesAStreamOnAnother` (`-race`) | 3 (`-count=3`, 2026-10-13) | pass |
| The load generator offers exactly its jobs and rate, charges late calls from their schedule, stops on the first error, and reads back only its own jobs | `loadgen: TestProduceOffersTheRate` and 19 more (`-race`) | 3 (`-count=3`, 2026-10-13) | pass |
| The profiling endpoint serves profiles and never the command line | `broker: TestDebugServerServesProfilesButNotTheCommandLine` (`-race`) | 3 (`-count=3`, 2026-10-13) | pass |
| Drain, steady and idle numbers before and after the lease rewrite, the pool size and the wakeups | `scripts/bench.sh` | 70 runs (66 matrix, 4 diagnostic; 9 rerun after the host slept), 2026-10-13 | drain 2,131 → 6,191 jobs/s on one broker, 1,851 → 5,001 on two |
| The chaos suite passes with Session 11's lease and wakeups | `go test -tags chaos -count=1 ./test/chaos` at `df3cde1` | 1 invocation (2026-10-13) | pass: 389 s; undisturbed run, killed workers, broker restart (graceful, crash), PostgreSQL restart (graceful, crash), outage longer than the lease, enqueue storm |
| Unary calls, and reopened lease streams, reach every resolved broker | `client: TestDialSpreadsCallsAcrossEveryResolvedBroker` (calls), `worker: TestReconnectingStreamsSpreadAcrossBrokers` (streams) | 3 (client, `-count=3`) and 2 (worker, `-count=2`), 2026-11-12 | pass |
| A silent broker is noticed in about 15s | `client: TestSilentBrokerIsNoticedInAboutFifteenSeconds` (a proxy stops forwarding; no FIN or RST) | 1 (2026-11-12) | pass; noticed after 15.1 s |
| A connection age makes clients find an added broker | `client: TestConnectionAgeFindsANewBroker` (with a control that never finds it) | 3 (`-count=3`, 2026-11-12) | pass |
| A lease stream ages out only between batches, and a connection age never cuts a held batch | `broker: TestLeaseStreamAgesOutOnlyBetweenBatches`, `TestConnectionAgeNeverCutsAHeldBatch` | 3 (`-count=3`, 2026-11-12) | pass; each fails when broken on purpose (age read inside a batch: 1 of 3 rows sent; a 200ms grace: the transport cut with 3 rows leased and unsent) |
| A client deadline cancels the broker's query | `integration: TestClientDeadlineCancelsTheQuery` (table lock; backend gone after the 500ms deadline) | 3 (`-count=3`, 2026-11-12) | pass |
| Lease-stream age-outs strand no lease | `chaos: TestLeaseStreamAgeOutsStrandNothing` (3 workers, 2 brokers, 5s ages, 6,000 jobs) | 3 (2026-11-12) | pass; 33, 33, 33 age-outs; 0 extra leases |
| Streams spread back after a broker returns, losing nothing | `chaos: TestStreamsSpreadBackAfterABrokerReturns` (8 workers, 2 brokers, 20s ages, 40,000 jobs) | 3 (2026-11-12) | pass; back in 9, 4, 20 s (3/5, 2/6, 3/5 streams); 0 lost, 0 repeated starts; run 3 redelivered 1 job whose lease the stopped broker's drain took and never sent (§11), reaped 30 s later |
| `Ack` and `Heartbeat` find their row by primary key even when statistics say nothing is leased | `store: TestPerJobUpdatesUseThePrimaryKeyUnderStaleStatistics` (20,000 rows analyzed idle, 128 leased; custom and generic plans) | 2 (`-count=2`, 2026-11-16) | pass; fails on `e61ae1c`'s guard, which plans `jobs_reap_idx` in all four cases |
| A PostgreSQL outage still loses no `Ack` or `Heartbeat`, and stale acks and cancellations still behave, with the H3a guard | `integration: TestAckRidesOutADatabaseOutage`, `TestHeartbeatRidesOutADatabaseOutage`, `TestMetricsMatchTheDatabase`, `TestStaleAckOverGRPC`, `TestCancelledLeasedJobTellsTheWorkerToStop` (`-race`) at `654c140` | 5 (`-count=5`, 2026-11-15) | pass |
| An undisturbed run, a broker restart and a PostgreSQL restart lose nothing and repeat no handler with the H3a guard | `chaos: TestUndisturbedRunIsClean`, `TestBrokerRestartLosesNothing` (graceful, crash), `TestPostgresRestartLosesNoAck` (graceful, crash) at `654c140` | 1 (2026-11-15) | pass: 162 s; 0 repeated starts, 0 redelivered, 0 lost leases in each; PostgreSQL outages answered `UNAVAILABLE` (45 and 35 Acks), never `INTERNAL` |
| Batch confirmations apply held leases, skip locked rows, never deadlock on overlapping batches, honor individual extension durations, and retain primary-key plans under stale statistics | `postgres: TestAckManyAppliesHeldLeasesAndMarksTheRest`, `TestAckManyNeverWaitsForALockedRow`, `TestOverlappingBatchesNeverDeadlock`, `TestExtendLeaseManyMovesEachLeaseByItsOwnDuration`, `TestBatchConfirmationsUseThePrimaryKeyUnderStaleStatistics` (`-race`, included in the full suite) | 2 (`-count=2`, 2026-11-16) | pass; prior Task 8 full-suite run, not rerun at close-out |
| Confirmation batchers start lone calls immediately, bound size/flights, preserve per-caller results, cancel abandoned statements and join running flights | Eleven tests in `internal/broker/batch_test.go` (`-race`, package/full suite) | 2 (`-count=2`, 2026-11-16) | pass; close-out's additional `-count=20` run was skipped at the user's request |
| One locked row delays only its caller, and shutdown waits for an abandoned confirmation statement to finish | `broker: TestALockedRowDelaysOnlyItsOwnCaller`, `TestShutdownWaitsForAnAbandonedConfirmationStatement` (`-race`) | 2 (`-count=2`, 2026-11-16) | pass |
| The integration suite runs with batching enabled and no store wrapper bypassing it | `HOPPER_TEST_CONFIRM_BATCH=64 go test -race -count=2 ./test/integration` | 2 (2026-11-16) | pass; six wrappers forward batch methods; both broker constructors honor the suite switch |
| Batching preserves renewal outage recovery, lease-loss cancellation, rejection of stale buffered deliveries and confirmation-token reuse | `integration: TestHeartbeatRidesOutADatabaseOutage`, `TestWorkerLeaseLossCancelsHandler`, `TestWorkerInitialValidationRejectsDelayedStaleDelivery`, `TestWorkerConfirmationAmbiguityReusesLeaseToken`, with `HOPPER_TEST_CONFIRM_BATCH=64` (`-race`) | 3 (`-count=3`, 2026-11-16) | pass; fault wrappers assert positive batch-call counters |
| A lone batched Ack's deadline cancels its PostgreSQL statement while the table lock is still held | `integration: TestClientDeadlineCancelsABatchedAck`, alongside `TestClientDeadlineCancelsTheQuery` (`-race`) | 3 (`-count=3`, 2026-11-16) | pass; disabling last-caller cancellation with a Go overlay made the new test fail because the backend remained waiting; normal source passed |
| Database outages, metrics, stale Acks and cancellation retain their behavior with batching enabled | `integration: TestAckRidesOutADatabaseOutage`, `TestHeartbeatRidesOutADatabaseOutage`, `TestMetricsMatchTheDatabase`, `TestStaleAckOverGRPC`, `TestCancelledLeasedJobTellsTheWorkerToStop`, with `HOPPER_TEST_CONFIRM_BATCH=64` (`-race`) | 5 (`-count=5`, 2026-11-16) | pass |
| Batching survives an undisturbed run and broker/PostgreSQL graceful and crash restarts without losing jobs or repeating handler starts | `chaos: TestUndisturbedRunIsClean`, `TestBrokerRestartLosesNothing`, `TestPostgresRestartLosesNoAck`, with `HOPPER_CHAOS_CONFIRM_BATCH=64` | 1 invocation (2026-11-16) | pass; 0 repeated starts in all five cases; graceful broker restart redelivered 6 jobs whose leases were never received, with no repeated handler starts; all 10 broker logs announced batching |
| The benchmark-only switch starts handlers without the ownership Heartbeat | `worker: TestTheBenchmarkSwitchStartsHandlersWithoutTheOwnershipCheck`, `cmd/worker: TestConfigReadsTheBenchmarkOwnershipSwitch` (`-race`) | 1 (2026-11-19) | pass; no Heartbeat calls reached the fake broker. Both tests were removed with the switch on 2026-11-23 (see the rows below) |
| The race suite passes with batching on by default, and the integration suite passes with batching off | `go test -race -count=2 ./...`; `HOPPER_TEST_CONFIRM_BATCH=1 go test -race -count=1 ./test/integration` (2026-11-20) | 2; 1 | pass in every package both ways; no timestamp gaps |
| With batching on by default (all broker logs announced it), an undisturbed run, a broker restart and a stopped worker lose nothing, and every repeat is attributed | `chaos: TestUndisturbedRunIsClean`, `TestBrokerRestartLosesNothing` (graceful, crash), `TestEnqueueStormDeduplicates`, `TestLeaseStreamAgeOutsStrandNothing`, `TestStreamsSpreadBackAfterABrokerReturns`, `TestKilledWorkersLoseNothing`, `TestAnOutageLongerThanTheLeaseReexecutes` (full suite, `-count=2`, 2026-11-20, 02:37–02:57 UTC) | 2 each | pass in both passes: 0 repeated starts and 0 redelivered jobs undisturbed, after broker restarts and in the age-out and rebalance runs (streams 3/5 and 2/6, 8 s after the broker returned); the storm left 1,000 rows for 1,000 keys (16 and 13 answers in doubt); 10 and 9 worker kills repeated 36 and 32 handler starts, all `killed`; the 40 s outage repeated 12 starts in each pass, all `lost` |
| A PostgreSQL restart with batching on by default loses no job, repeats no handler, and answers nothing `INTERNAL` | `chaos: TestPostgresRestartLosesNoAck/graceful`, `/crash` (full suite, `-count=2`, 2026-11-20) | 2 each | crash: pass twice (46 and 18 `Ack` plus 15 `Heartbeat` answered `UNAVAILABLE`); graceful: **1 of 2 failed**, 1 passed (39 `UNAVAILABLE`). The failure was the test's check that every gated `Ack` was answered `UNAVAILABLE`: 3,006 of 3,006 jobs completed, 0 repeated starts, 0 `INTERNAL`, but only 10 `Ack` answers, all at the moment of shutdown (§11.3) |
| That failure is the batcher's shared flight, not the database or the test | `chaos: TestPostgresRestartLosesNoAck/graceful` with `HOPPER_CHAOS_CONFIRM_BATCH` and flights varied (2026-11-20) | 8 per setting | batch 64, 2 flights (default): 3 failed, 5 passed; batch 1: 0 failed; batch 64 with 16 flights: 0 failed (41 to 49 `UNAVAILABLE` answers, or 15 plus 17 `Heartbeat`). With the earlier full-suite run, the default setting failed 4 of 10 |
| A read-only server's refusals are unavailable, and a write to one takes no effect | `postgres: TestUnavailableClassifiesConnectionFailures`, `TestAReadOnlyServerIsUnavailable` (`-race`) | 1 full suite (2026-11-22) | pass |
| A connection attempt with no deadline of its own ends at 5 s when the DSN sets none; the DSN's bound wins | `broker: TestPoolConfigBoundsConnectionAttempts`, `TestASilentServerEndsAConnectionAttemptWithNoDeadline` (`-race`) | 1 full suite (2026-11-22) | pass: ended after 5.01 s (the test's own run time, from a rerun with `-v`) |
| The wakeup listener logs its reconnect at info, with its downtime; the first connect is not a reconnect | `broker: TestTheListenerLogsItsReconnectAtInfo` (`-race`) | 1 full suite (2026-11-22) | pass |
| Accounting finds acknowledged jobs missing from the rows, by first answer, and names a repeat that followed an acked lease | `accounting: TestEveryAcknowledgedJobPresentIsDurable`, `TestAMissingAcknowledgedJobIsLostAtItsFirstAnswer`, `TestAnAckedLeaseThatRanAgainIsALostAck` (`-race`) | 1 full suite (2026-11-22) | pass |
| `postgres.mode=cloudnativepg` replaces the bundled database with a three-instance Cluster replicating synchronously to any one standby; brokers use its `-rw` Service; unsafe replication refuses to render; the default renders unchanged | `chart: TestCloudNativePGReplacesTheBundledDatabase`, `TestAsynchronousReplicationIsExplicit`, `TestChartRejectsUnsafeReplication`, `TestTheFailoverProofsValuesRender`, `TestRenderedManifestsAreCurrent` | 1 full suite (2026-11-22) | pass |
| The chaos suite passes with the failover fixes | `go test -tags chaos -count=1 ./test/chaos` at `7da5f07` | 1 invocation (2026-11-22) | pass: 613.9 s; PostgreSQL restarts answered `UNAVAILABLE` (44 Acks in the graceful case, 52 in the crash case), never `INTERNAL` |
| A pod delete stops PostgreSQL with a shutdown; SIGKILL through the runtime is a crash | `docs/failover/kill-modes.txt` (CloudNativePG 1.30.2, kind) | 1 each (2026-11-22) | delete: 1 `received smart shutdown request` line; crash: no shutdown line |
| A primary crash under synchronous replication loses no acknowledged job; every repeat is explained and none follows an ack; brokers answer `UNAVAILABLE`, never `INTERNAL`; every wakeup listener reconnects; a new scheduler term fires the pending tick once | `k8s: TestPrimaryFailoverLosesNothing` (200 enqueues/s, 50 ms jobs, 3 workers, 2 brokers, 3 instances, `ANY 1`) at `12d598e` | 5 (`-count=5`, 2026-11-22) | pass: 31,355 acknowledged, 0 lost; promoted after 17.8–18.2 s, routed after 17.9–18.6 s; producers answered again after 19.3–20.3 s (median 19.5); 149 retries found their job; no repeats, 0 attempt problems, `INTERNAL` empty |
| The same crash with replication paused 3 s first, before the cancelled-wait fix | `k8s: TestPrimaryFailoverLosesNothing`, `HOPPER_FAILOVER_LAG=3s` at `12d598e` | 3 (2026-11-22) | fails as measured, 2 of 3 runs: 0, 5 and 9 lost a run (14 of 19,478), all answered only as deduplicated; 0 new jobs acknowledged during the pause; 12, 12 and 13 cancelled waits logged, all `INSERT` |
| A primary crash under asynchronous replication | `k8s: TestAsynchronousFailoverCountsWhatItLoses` at `12d598e` | 3 (2026-11-22) | 0 lost of 21,175 acknowledged; recovered after 19.1–19.8 s |
| The same with replication paused 3 s first | `k8s: TestAsynchronousFailoverCountsWhatItLoses`, `HOPPER_FAILOVER_LAG=3s` at `ab24dec` | 3 (2026-11-22) | 689, 652 and 694 lost a run (of 657, 633 and 661 acknowledged during the pause); answered from −3.3 to +0.2 s around the crash; 10, 22 and 11 acked deliveries ran again |
| PostgreSQL waits for a synchronous standby at the commit of a write and of a row lock, not of a plain read, a deduplicating insert or a transaction ID alone; a cancelled wait keeps the commit | `postgres: TestWhatASynchronousCommitWaitsFor` (a standby named `ghost` that never connects, PostgreSQL 16) | 1 full suite + 5 focused (2026-11-23) | pass |
| A retried `Enqueue`, `Ack`, `Nack` and `Cancel` is not answered until a standby has the work it found; refusals are answered at once; a lease of a row no standby has waits too | `postgres: TestARetriedEnqueueWaitsForTheStandby`, `TestARetriedAckWaitsForTheStandby`, `TestARetriedNackWaitsForTheStandby`, `TestARetriedCancelWaitsForTheStandby`, `TestRefusalsDoNotWaitForTheStandby`, `TestALeaseOfARowNoStandbyHasWaitsForTheStandby` (`-race`) | 1 full suite + 5 focused (2026-11-23) | pass; the first four failed before their fix ("answered … about a row no standby has") |
| The chaos suite passes with the fix | `go test -tags chaos -count=1 ./test/chaos` at `1f128c5` | 1 invocation (2026-11-23) | pass but for the known batcher-flight flake: 614.2 s; every test passed except `TestPostgresRestartLosesNoAck/graceful`, which failed the check that every gated `Ack` was answered `UNAVAILABLE` (8 answers; 3,006 of 3,006 completed, 0 repeated starts), as in the 2026-11-20 rows above, and passed when rerun alone (47 `UNAVAILABLE`); crash: 18 `Ack` answered `UNAVAILABLE`; storm: 1,000 keys, 17,043 calls (156 retried), 28,153 answers, 6 in doubt, 1,000 rows; `INTERNAL` empty throughout |
| The fix doesn't change the drain | `scripts/bench.sh drain`, `a3fcbda` against `1f128c5`, 1 broker, 8 workers, `PIN=1`, 300-byte payloads | 5 each, interleaved (2026-11-23) | no measurable effect: 5,685 jobs/s (4,577–6,244) after against 5,602 (4,692–6,399) before, 1.015×; 0.44 commits per job against 0.45; 0 locked reads |
| Before the fix, under the new checks | `k8s: TestPrimaryFailoverLosesNothing`, `HOPPER_FAILOVER_LAG=3s`, release at `a3fcbda`, harness at `de94830` | 3 (2026-11-23) | fails as measured, 3 of 3 runs: 12, 9 and 9 retries answered deduplicated during the pause, and the same 12, 9 and 9 lost (30 of 19,767); 16, 17 and 14 answered, all deduplicated, before the new primary had a synchronous standby, each run's first answer before its first standby; the producer's broker's pool had every connection acquired in 100% of samples during the pause, its acquires waiting 13.3–14.2 s, the other broker's in none: the pool hypothesis is supported |
| A primary crash with replication paused 3 s loses no acknowledged job; nothing is answered during the pause or before the new primary has a synchronous standby | `k8s: TestPrimaryFailoverLosesNothing`, `HOPPER_FAILOVER_LAG=3s` at `de94830` | 3 (2026-11-23) | pass 3 of 3: 21,848 acknowledged, 0 lost; 0 answered during the pause; 0 answered or confirmed without a standby; old primary cancelled 16, 14 and 13 waits |
| A primary crash under synchronous replication, with the window check | `k8s: TestPrimaryFailoverLosesNothing` at `de94830` | 5 (`-count=5`, 2026-11-23) | pass 5 of 5: 35,420 acknowledged, 0 lost; first synchronous standby after 19.9–26.0 s; answered again after 19.7–25.8 s (median 24.4) against 19.3–20.3 (19.5) before; 0 answered or confirmed without a standby; the new primary cancelled 14–46 waits |
| The broker leases only against credit it has received, and sends no row beyond what the store was asked for | `broker: TestLeaseStreamNeverExceedsCredit`, `TestLeaseStreamLeasesOnlyAgainstCreditItHasReceived`, `TestLeaseStreamSendsNoRowsBeyondWhatItAskedFor`, at `70c376e` | 3 (`-count=3`, 2026-11-23) and the package under `-race` | pass. Before the guard the first passed (the invariant was already enforced at the lease call) and the third failed, the broker sending a row from a batch one larger than asked; a store that over-returns now ends the stream `INTERNAL` and sends none of that batch |
| A fresh delivery starts without the ownership check; a stale one takes it; credit is matched oldest first and a reconnect resets it; a job cancelled before its handler starts stops at its first renewal, or is refused at its `Ack` | `worker: TestCreditIsMatchedToDeliveriesOldestFirst`, `TestReconcileForgetsTheSendInstants`, `TestAReconnectResetsTheCreditAccounting`, `TestFreshDeadlineCountsFromTheCreditSend`, `TestAFreshDeliveryStartsWithoutTheOwnershipCheck`, `TestAStaleDeliveryStillTakesTheCheck`, `TestACancelledJobThatStartedStopsAtItsFirstRenewal`, `TestACancelledJobThatFinishesFirstIsRefusedAtItsAck`, at `0735f49` and `170c385` | 3 and 5 (`-count=3`, `-count=5`, 2026-11-23) | pass, no flake. With only the deadline function written the start tests failed as predicted (a Heartbeat where none was expected; the first-renewal test timing out after 10 s); the stale test passed on both |
| The tests that pin the ownership check still do, through an unexported option | `worker`: seven tests (`TestASuccessfulHandlerIsAcked`, `TestExecutionPinsHeartbeatsToTheDeliveredGrant`, `TestAnUnregisteredQueueIsNackedUnstarted`, `TestADeliveryThatLosesToShutdownIsNackedUnstarted`, `TestObserverNeverSeesUnstartedJobs`, `TestForcedShutdownCountsBlockedAcquisition`, `TestForcedShutdownCountsBlockedUnstartedNack`) with `checkEveryDelivery: true`, no assertion changed; `integration: TestWorkerInitialValidationRejectsDelayedStaleDelivery` with a real 300 ms delay in place of an edited `lease_expires` | 3 (`./worker -count=3`) and 5 (the integration test), 2026-11-23 | pass. The integration test failed unchanged: the delivery was 3.3 ms old, so the rule skipped the check and the handler ran; at 104 ms the same, at 406 ms it passed (the threshold, G/10 = 200 ms, is what separates them). Its setup had simulated age by rewriting `lease_expires`, which the rule assumes nothing does |
| The race suite passes after the start rule | `go test -race -count=1 ./...` at `170c385`, and again at `8d26a73` after the switch was retired | 1 invocation each (2026-11-23) | pass: 15 of 15 packages each |
| The chaos suite passes with the start rule | `go test -tags chaos -count=1 ./test/chaos` at `170c385` | 1 invocation (2026-11-23) | pass but for the known batcher-flight flake: 604.2 s; every test passed except `TestPostgresRestartLosesNoAck/graceful` (8 `Ack`s answered `UNAVAILABLE`, 3,006 of 3,006 completed, 0 repeats), which passed alone on its one allowed rerun (48). 0 repeated starts in the undisturbed run, the broker restarts, the age-outs (32 streams aged out) and the rebalance; the 40 s outage repeated 12, all `lost`; the worker kills repeated 36, all `killed` (9 kills); `INTERNAL` empty throughout |
| The drain with the start rule against the check on every delivery | `scripts/bench.sh drain`, `170c385` against `770fdb5`, 1 and 4 brokers, 8 workers, `PIN=1`, 300-byte payloads | 5 each, interleaved (2026-11-23) | 1 broker: **helps**, 8,937 jobs/s (8,543–10,143) against 6,099 (5,717–6,879), 1.465×, commits per job 0.29 against 0.45; 4 brokers: **helps**, 6,031 (5,109–7,515) against 4,060 (3,956–4,297), 1.485×, 0.51 against 0.92. Heartbeat calls per job 0.000 against 1.000 at both; no failed or rejected run ([bench/1b/h4-credit/](bench/1b/h4-credit/)) |
| The benchmark switch is retired without loss | `grep -rn 'SkipOwnershipCheck\|SKIP_OWNERSHIP'` outside `docs/` and `gen/`; `go test -race -count=1 ./...` at `8d26a73` | 1 each (2026-11-23) | no match; 15 of 15 packages pass. The two tests of the removed option went with it |
| A batch waits at most 1 s for a connection and then answers `UNAVAILABLE`; a caller's own deadline stays its own | `postgres: TestABatchWhoseDialHangsAnswersUnavailableWithinTheBound`, `TestABatchWithNoConnectionFreeAnswersUnavailableWithinTheBound`, `TestACallerDeadlineInsideTheBoundStaysTheCallers` (each for `AckMany` and `ExtendLeaseMany`), at `bee4ca7` | 1 before the fix and 3 after (`-count=3`, 2026-11-23); with the broker and integration packages once | before: the dial test failed on time for both (5.005 s, 5.004 s; a hung dial was already `UNAVAILABLE`), the no-connection test failed on `ErrUnavailable` (it waited 5 s and got the caller's deadline), the caller test passed; after: all pass, and `TestEveryStoreErrorGoesThroughWrap`, `TestClientDeadlineCancelsABatchedAck` and `TestBatchRetryAfterTransientErrorDoesNotDuplicate` pass unchanged |
| A graceful PostgreSQL stop answers the gated `Ack`s `UNAVAILABLE` at the default batch | `chaos: TestPostgresRestartLosesNoAck/graceful`, batch 64, two flights, at `bee4ca7`; before, `b553da7` | **8 of 8** after (`-count=8`); 1 of 5 failed before (`-failfast`, 2026-11-23) | pass 8 of 8: 42, 44, 49, 46, 55, 44, 42 and 45 `Ack`s answered `UNAVAILABLE`, 0 `INTERNAL`, 3,006 of 3,006 completed, 0 repeats each; the bound's error was in the brokers' logs in 7 runs, 2 `Ack`s each. Before: 51, 21, 42 and 53 answered in the four passes and 9 in the failure ([bench/1b/flight-bound/graceful-8.txt](bench/1b/flight-bound/graceful-8.txt)) |
| The race suite and the chaos suite pass with the bound | `go test -race -count=1 ./...` and `go test -tags chaos -count=1 ./test/chaos` at `bee4ca7` | 1 invocation each (2026-11-23) | 15 of 15 packages pass; chaos passes in full, `graceful` included: 614.1 s. PostgreSQL restarts answered 44 `Ack`s `UNAVAILABLE` (graceful) and 51 (crash), 0 `INTERNAL`, 0 repeats; the 40 s outage repeated 12, all `lost`; the worker kills repeated 40, all `killed` (10 kills); 33 streams aged out with 0 repeats; the broker restarts repeated 0 (the SIGKILL one redelivered 3 jobs a stream had sent but no worker received, §11 item 5); the undisturbed run and the rebalance repeated 0; `INTERNAL` empty throughout |
| The Java worker SDK applies the start rule: the instant is read before `onNext` and accounted before it returns; credit is matched oldest first, refused beyond what is outstanding, and reset on reconnect; G/10 − 1 ns is fresh, and G/10, a negative age and a never-sent unit are not; a fresh deadline is `deadlineFrom(s, G)`; a cancelled job that started stops at its first renewal, and one that finished first is refused at its `Ack` | `hopper-worker: StartRuleTest` (3), `CreditLedgerTest` (8), `CreditStampTest` (3), `StartRuleSessionTest` (5), `FreshnessTest` (4), at `4e13996` | 4 (the suite once and three `--rerun-tasks`, 2026-11-23) | pass, 23 of 23 each. Mutants killed across the worker suite: the stamp after `onNext`, the ledger lock held across it, newest-first matching, an over-delivery started or counted, a reconnect keeping the old units, the deadline from receipt, G/10 inclusive, a negative age allowed, an Ack after a cancelled renewal. Without its `sent` check `StartRule` is caught only by `StartRuleTest`, since no session test can deliver against an unsent unit ([design/java-worker-sdk.md](design/java-worker-sdk.md), Results) |
| The Java worker SDK passes its suite | `hopper-worker`, 187 tests from 181 methods: 102 port 94 of the Go SDK's 110 tests, 79 are Java-only; 13 more Go tests (the fake's) are ported in `hopper-proto`, 3 are not ported; at `4e13996` | 4 (once and three `--rerun-tasks`, 2026-11-23) | pass, 187 of 187 each, no flake. One flake found earlier in a test's own wait (the fake counts a call before its hook runs) was fixed in the test before these runs |
| The Java client, and against the Compose stack | `hopper-client`, 38 tests, at `4e13996`; `ComposeClientSmokeTest` (5) through `scripts/java-smoke.sh client` (Task 9); `ComposeWorkerSmokeTest` (2) through `worker-smoke` | the suite with the last `--rerun-tasks` build; the smoke 1, the worker smoke 2 (2026-11-23) | pass: 38 of 38; 5 of 5; 2 of 2 both times |
| The example worker keeps `cmd/worker`'s configuration and exit codes, and prints only JSON | `hopper-example-worker`, 45 tests (10 ports of `cmd/worker/main_test.go`, 35 Java-only, among them `MainProcessTest`, a child JVM sent a real SIGTERM); then the image `hopper-example-worker:dev`, at `e39bd1c` | 4 (once and three `--rerun-tasks`); the image's probes once (2026-11-23) | pass, 45 of 45 each. Mutants killed: no halt (the JVM exits 143), `halt(0)` after abandonment, no JUL bridge, the default uncaught-exception printer, the environment overriding a given flag, concurrency 0 accepted, a `sleep` deaf to interruption. The flushes before `halt` are an equivalent mutant: every line is already flushed. Image: user `65532:65532`; `-concurrency 0` exits 1 with `worker: concurrency must be positive`; `-help` exits 0; SIGTERM exits 0 with 19 lines, all JSON; the worker id ends `-1`, so the JVM is PID 1 |
| The Java worker's undisturbed log passes `test/accounting` unchanged | `accounting: TestJavaWorkerLog` (build tag `javalog`), `undisturbed`, 240 jobs over the three queues, Compose `hopperjava` (one broker, 30 s lease), concurrency 4, image at `e39bd1c` | 1 (2026-11-23) | pass: 240 of 240 completed, 1 process, 20 redelivered and 20 restarted (the `fail_n_times` retries, each repeat explained as nacked), no shutdown, 0 non-JSON lines ([bench/java-smoke/20261123/](bench/java-smoke/20261123/)) |
| A SIGTERM mid-run drains cleanly: exit 0, and the log passes accounting as a graceful shutdown | `TestJavaWorkerLog`, `graceful`, 400 `sleep` jobs of 250 ms, `docker stop` after 20 starts, then a restart; image at `e39bd1c` | 1 (2026-11-23) | pass: exit 0; 400 of 400 completed over 2 processes, 0 redelivered; `shutdown started` carried `running_handlers` 4, and the process logged `worker stopped` |
| A job cancelled while its handler runs is stopped at its first renewal, G/3 after the start, and not confirmed | `TestJavaWorkerStopsACancelledJobAtItsFirstRenewal`, a 20 s `sleep` job cancelled through the Java client | 2 (2026-11-23): at `e39bd1c`, then at `3461424` | the first run **failed**: the client cancelled 15.7 ms after the lease, before the worker's ownership check (the delivery was stale, 5.2 s after its credit), which was refused `FAILED_PRECONDITION`; the handler never started and nothing was confirmed, as in the Go SDK, but it is not the running case. The driver now cancels 2 s after `LEASED`. The rerun passed: the delivery, stale by 4.6 s, took the check; stopped 10.05 s after the handler started (G/3 = 10 s); no `job confirmed`. The live run exercises the renewal branch; the case where a job is cancelled before a fresh delivery's handler starts is `FreshnessTest`'s, on the manual clock |
| A drain that outlasts its budget exits 1 and reports what it abandoned | `scripts/java-smoke.sh abandon`, two 20 s jobs, a 2 s shutdown budget; image at `e39bd1c` | 1 (2026-11-23) | pass: exit 1; one `shutdown deadline reached` line with `active_deliveries` 2, `running_handlers` 2, `acquiring` 0, `confirming` 0; stderr `worker: worker: shutdown deadline reached with 2 active deliveries (2 running handlers, 0 acquiring a lease, 0 awaiting confirmation)`; both jobs completed after the restart |
| The Java log check can fail | `TestJavaWorkerLog` on damaged copies of the runs' logs | 1 each (2026-11-23) | the first `job started` deleted: fails, `completed, but no worker logged its start`; one non-JSON line added: fails, `every line the Java worker printed must be a JSON record`; a nack's `job confirmed` deleted: fails, no nack explains the job running again. An ack's `job confirmed` deleted: **passes**, because accounting proves a completed job's Ack from its row, for the Go worker too |

The Phase 2 rows are tabulated per run in [failover/summary.md](failover/summary.md).
After a promotion the new primary has no synchronous standby for a few seconds,
and 0 to 43 `INSERT` and `UPDATE` waits were cancelled in that window in ten
unpaused promotions ([failover/post-promotion-waits.txt](failover/post-promotion-waits.txt),
§11 item 7). Since the cancelled-wait fix, nothing is answered in that window ([failover/cancelled-wait/summary.md](failover/cancelled-wait/summary.md)). Three details of the runs:

- The scheduler fires an `@every 1s` schedule on its 5 s poll, so a run saw
  2 to 5 later ticks, not one a second.
- Promotion takes about 18 s, almost all of it the operator's "Waiting for the
  new primary to notice the promotion request". One paused run promoted in
  4.3 s; why was not investigated.
- A producer's retry found its job already there 16 to 43 times per
  synchronous plain crash (10 to 34 in the paused runs) and 0 times per
  asynchronous one.

Phase 1b-2 close-out (2026-11-17) reuses the completed runs above, recorded in
[the verification note](bench/1b/h2-verification.md). At the user's request,
no close-out tests, lint, formatting checks, additional focused store runs or
20-repetition batcher run were performed. The rows report actual prior counts;
they do not claim the skipped checks passed. Batching was enabled by default
in `df19e76` on 2026-11-19, after the user approved the H2 recommendation in
[scaling.md](scaling.md). The recheck was the full race suite once with
batching on (one broker logging test configured for its non-batching fake) and
the integration suite once with `HOPPER_TEST_CONFIRM_BATCH=1`. The Phase 1b-3
rows above are the later, full runs.
