# hopper — delivery semantics

This document is the contract between hopper and the code that uses it. If the
implementation and this document disagree, one of them has a bug; fix whichever
is wrong and keep this file authoritative. The "verified behavior" table at the
end is filled in from the chaos suite (`test/chaos`), not from intent.

## 1. Guarantees in one paragraph

hopper delivers every enqueued job **at least once**. A job is never lost once
`Enqueue` has returned, and it is never delivered to two workers at the same
time, but it *can* be delivered more than once in sequence: whenever a worker
holds a lease and does not ack before the lease expires, the job is redelivered.
Handlers must therefore be idempotent. hopper provides no ordering guarantee
across jobs and no exactly-once execution; the latter is a property your handler
must supply, using the job id or `job.Attempt` as its dedup key.

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
  select id from jobs
  where queue = any($1) and state = 'available'
  order by priority desc, run_at
  limit $2
  for update skip locked
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
- **Duration.** `lease_expires = now() + lease duration`. The default is set on
  the broker; a worker may request a different duration per stream.
- **Extension.** `Heartbeat(id, lease_id, dur)` sets
  `lease_expires = now() + dur` **only if** `state = 'leased' and lease_id = $2`.
  The SDK heartbeats at 1/3 of the lease duration. If a heartbeat is rejected
  (lease lost) the SDK cancels the handler's `context`; the handler should
  stop, but hopper cannot force it to.
- **Expiry.** The reaper runs on a ticker and applies the retry policy (§6) to
  every row with `state = 'leased' and lease_expires < now()`. Redelivery is
  therefore not instantaneous: worst-case latency is lease duration + reaper
  interval + backoff.
- **Due-ness is a state, not a filter.** The lease query does not check
  `run_at`. A promoter ticker moves `scheduled → available` when
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

If zero rows are updated the store looks at the row and returns one of:

| Situation | Result |
|---|---|
| Row is `completed` **and** `lease_id = $2` | `OK` (idempotent re-ack; the first ack's response was lost) |
| Row is `leased` with a different `lease_id` | `ErrLeaseLost` — the lease expired and someone else holds the job |
| Row is `scheduled` / `available` / `dead` | `ErrLeaseLost` — the reaper took it back |
| Row is `cancelled` | `ErrCancelled` |
| No row | `ErrNotFound` |

The idempotent re-ack case is why `lease_id` is never cleared on completion.
`Nack` is guarded the same way and applies the retry policy. A stale worker can
never ack, nack, or extend a job it no longer holds; this is the property the
"stale-lease ack rejected" test in `store` pins down.

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
  `select` when the insert returned nothing. Under 1000 concurrent enqueues
  with 10 distinct keys, exactly 10 rows exist.
- Keys are scoped per queue. The same key in two queues is two jobs.
- The duplicate's `payload`, `priority`, and `run_at` are ignored; the first
  writer wins and the caller is not told the payload differed.
- **Protection lasts exactly as long as the row exists.** A key on a
  `completed` job still dedups until retention (§10) deletes the row; after
  that the same key creates a new job. A key on a `dead` job dedups until the
  job is purged. If you need permanent dedup, keep the key in your own store.
- Cron uses `idempotency_key = name || '@' || next_run_at` so a leader
  failover cannot fire the same tick twice.

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
- `Lease` over several queues (`queue = any($1)`) orders across all of them
  by `priority desc, run_at`, so a busy queue can dominate a stream that
  also subscribes to quiet ones.

## 10. Cancellation and retention

**Cancel(id)**

| Current state | Effect |
|---|---|
| `scheduled`, `available` | → `cancelled`. Never runs. |
| `leased` | → `cancelled` immediately in the store. The worker learns at its next heartbeat/ack (`ErrCancelled`) and the SDK cancels the handler context. Side effects already performed are **not** undone. |
| `completed`, `dead`, `cancelled` | No change; returns `ErrTerminal` with the current state. |

**Retention.** A janitor deletes `completed` and `cancelled` rows older than
a configurable window (default 24h). `dead` rows are kept until purged.
Retention bounds table and index size and, per §8, is also the lifetime of
idempotency protection.

## 11. Where duplicates come from (the honest list)

Every duplicate execution corresponds to a lease that was granted and not
acked, so `attempts` should account for every one. These are the ways a
lease is granted and not acked:

1. **Worker crashes or is `SIGKILL`ed mid-handler.** Lease expires, job is
   redelivered after backoff. Expected; this is at-least-once.
2. **Handler runs longer than the lease and heartbeats stop** (worker
   partitioned from the broker, or the heartbeat goroutine starved). The
   reaper takes the job back while the original handler is still running.
   Two executions can overlap in time. The SDK cancels the handler's context
   when a heartbeat fails, but a handler that ignores its context keeps going.
3. **The ack-loss window.** Handler finishes → worker sends `Ack` → the
   broker or Postgres is unavailable before the `UPDATE` commits. The work is
   done but the store does not know. The SDK retries `Ack` until the lease
   would have expired; if it cannot get through, the lease expires and the
   job is redelivered. From the client's point of view the job simply ran
   twice. There is no way to close this window without a transactional
   handoff between the handler's side effects and the queue; that is what
   idempotent handlers are for.
4. **Graceful shutdown deadline exceeded.** On `SIGTERM` the worker stops
   leasing and waits for in-flight handlers up to `terminationGracePeriodSeconds`
   minus the preStop sleep. A handler still running at the deadline is
   abandoned and its lease expires normally. Size the grace period to your
   slowest handler.

What is **not** a source of duplicates: two workers receiving the same lease
(prevented by `SKIP LOCKED`), a broker restart (brokers are stateless; workers
reconnect and their leases keep ticking), or a scheduler failover (cron uses
idempotency keys).

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

## 13. Verified behavior

Filled in from the chaos suite (Session 10). Each row must link to the test
that proves it; a row without a test is a claim, not a guarantee.

| Claim | Test | Runs | Result |
|---|---|---|---|
| Concurrent `Lease` calls never return the same job | `store: TestLeaseDisjoint` (50 goroutines) | | pending |
| Stale-lease `Ack`/`Nack`/`Heartbeat` rejected | `store: TestStaleLeaseRejected` | | pending |
| Lease expiry → redelivered exactly once more, `attempts` incremented | `store: TestLeaseExpiryRedelivers` | | pending |
| `max_attempts` failures → `dead`; `Redrive` → `available`, `attempts = 0` | `store: TestDeadLetter` | | pending |
| 1000 concurrent `Enqueue` with 10 keys → 10 rows | `store: TestIdempotencyStorm` | | pending |
| Priority-10 job leased before 100 priority-0 jobs | `store: TestPriority` | | pending |
| `SIGTERM` mid-job → job acked, not redelivered | `worker: TestGracefulShutdown` | | pending |
| Random worker kills during 10k jobs → every duplicate explained by `attempts` | `chaos: TestWorkerKills` | | pending |
| Broker restart mid-run → no job lost, workers reconnect | `chaos: TestBrokerRestart` | | pending |
| Postgres restart mid-run → broker recovers; ack-loss window observed and bounded | `chaos: TestPostgresRestart` | | pending |
| Duplicate enqueue storm → dedup holds | `chaos: TestEnqueueStorm` | | pending |
| Scheduler failover → no duplicate cron enqueues (20 runs) | `scheduler: TestFailover` | | pending |
| `kubectl rollout restart deployment/worker` under load → zero duplicates | manual, Session 8 | | pending |
