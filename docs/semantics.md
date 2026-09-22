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
must supply, using the stable job ID (or a stable business ID in the payload) as
its dedup key. `job.Attempt` is delivery history, not an idempotency key.

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
  the broker; a worker may request a different duration per stream. On the
  wire an absent or zero duration selects that default. Any other value must be
  a valid protobuf `Duration` that converts to a Go `time.Duration` exactly (no
  saturation) and be positive, or the call fails with `INVALID_ARGUMENT` before
  it reaches the store. No operational maximum is imposed.
- **Flow control.** A worker holds one `LeaseJobs` stream and spends *credit*:
  its first message declares how many delivery slots are free, and every later
  message returns credit only after a delivery has stopped using its slot. A
  token covers initial ownership validation, handler execution, and Ack/Nack
  confirmation; handler return alone does not free it. The broker never leases
  more rows than the outstanding credit, and never more than
  `-max-lease-batch` at once. This is why the stream is bidirectional: a
  server-stream carries no capacity-return signal, and the broker cannot infer
  one from `Ack`, because brokers are stateless and a worker's `Ack` may land on
  a different replica. Credits are cooperative free-slot declarations, not
  receipt acknowledgments. Stream teardown reclaims only unresolved stream
  credit. On reconnect the worker advertises its currently free slots; closing
  a stream does not revoke leases or capacity held by deliveries already in
  progress. A redundant subscription is rejected.
- **Extension.** `Heartbeat(id, lease_id, dur)` (store method: `ExtendLease`) sets
  `lease_expires = now() + dur` **only if** `state = 'leased' and lease_id = $2`.
  Each delivery carries valid `leased_at` and `lease_expires` timestamps; their
  difference is the broker-granted duration. As an explicit SDK policy, the
  worker performs one bounded Heartbeat before invoking user code, then renews
  at one third of the granted duration. Every Heartbeat sends that original
  grant explicitly; it does not reselect a broker default that may have changed
  since delivery. A successful RPC creates a conservative local deadline from
  the monotonic request-start instant with scheduling slack; that estimate is
  neither a new store guard nor permission to revive an already expired local
  lease. If ownership is definitively rejected, the job
  was cancelled, or the local deadline expires, the SDK cancels the handler's
  `context`. A handler should stop, but Go context cancellation cannot forcibly
  stop code that ignores it. This initial validation costs one unary RPC for
  every valid delivery.
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

For `Ack`, if zero rows are updated the store looks at the row and returns one
of:

| Situation | Result |
|---|---|
| Row is `completed` **and** `lease_id = $2` | `OK` (idempotent re-ack; the first ack's response was lost) |
| Row is `leased` with a different `lease_id` | `ErrLeaseLost` — the lease expired and someone else holds the job |
| Row is `scheduled` / `available` / `dead` | `ErrLeaseLost` — the reaper took it back |
| Row is `cancelled` | `ErrCancelled` |
| No row | `ErrNotFound` |

The idempotent re-ack case is why `lease_id` is never cleared on completion.
`Nack` is guarded by the same `(id, lease_id)` ownership token and applies the
retry policy. A retry of a Nack succeeds idempotently only while that same-token
row is still `scheduled` or `dead`. Once the promoter changes `scheduled` to
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
| other store error | `INTERNAL` | Detail is logged. Do not retry blindly: invalid database input is permanent, and mutations may already have committed. |

`ErrCancelled` is deliberately **not** `CANCELLED`: that code means the caller
cancelled the RPC, and conflating the two would make a job outcome
indistinguishable from a context deadline to any generic retry interceptor.

`Cancel` is idempotent at the gRPC boundary although `Cancel` in the store is
not: cancelling an already-cancelled job returns OK with `already_cancelled`,
because a retried `Cancel` must not look like a failure. `completed` and `dead`
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
  `select` when the insert returned nothing. Under 1000 concurrent enqueues
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
  failed batch whole with stable per-item keys: the committed items
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
- `Lease` over several queues (`queue = any($1)`) orders across all of them
  by `priority desc, run_at`, so a busy queue can dominate a stream that
  also subscribes to quiet ones. PostgreSQL uses `jobs_lease_idx` to find the
  candidates but still sorts the combined stream; `ListDead` similarly uses
  `jobs_dead_idx` and incrementally sorts its `id` tiebreaker. The other hot
  sweeps use their indexes without an explicit sort.
- **Idle queues are polled.** After an empty lease result, a stream waits up
  to `-poll-interval` (default 250ms), unless a credit update wakes it earlier.
  Normal additional polling delay is between zero and one interval, plus
  scheduling/database/network latency. There is no strict delivery bound.
  Session 11 evaluates `LISTEN/NOTIFY` and pool sizing under load.
- **`ListJobs` order is by ID, not by creation.** Pages are keyed on the last
  ID seen. UUIDv7 ids follow application clocks, so order approximates but does
  not equal `created_at` or commit order. A traversal is live, not a snapshot:
  rows inserted or changed behind the cursor may be missed, and a cursor whose
  row was since deleted still works. It has no dedicated index and walks the
  primary key; Session 11 revisits that if it matters.

## 10. Cancellation and retention

**Cancel(id)**

| Current state | Effect |
|---|---|
| `scheduled`, `available` | → `cancelled`. Never runs. |
| `leased` | → `cancelled` immediately in the store. The worker learns at its next heartbeat/ack (`ErrCancelled`) and the SDK cancels the handler context. Side effects already performed are **not** undone. |
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
   done but the store does not know. The SDK retries `Ack`/`Nack` only for the
   two transient transport codes, ending at the earlier of its conservative
   local lease deadline and forced shutdown. If it cannot get through, the
   lease expires and the job is redelivered. A handler's side effects can
   therefore duplicate after failures even when its own call returned once.
   There is no way to close this window without a transactional handoff between
   the handler's side effects and the queue; that is what idempotent handlers
   are for.
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
5. **A lease stream fails before delivery is confirmed by subsequent work.**
   The broker leases rows, but a disconnect may leave some buffered or never
   received by the worker. Send success is not an application receipt. Unacked
   leases consume attempts and are reaped under the normal retry policy; they
   may eventually become dead without handler execution. `-max-lease-batch`
   bounds one store call, not all unreceived or unacknowledged jobs on a stream.
   Do not immediately release/nack them: some may already be executing.

   This uncertainty is different from a delivery the SDK has received and
   knows it never started, such as an unregistered queue or a delivery that
   lost the acceptance race with shutdown. After bounded ownership validation,
   the SDK Nacks that known-unstarted delivery immediately. Its lease already
   consumed an attempt, so the Nack schedules the normal retry and can move the
   row to `dead` on its final attempt.

Worker-facing error text is made valid UTF-8, stripped of NUL bytes, and bounded
to 4,096 bytes (including its truncation suffix) before logging or persistence
in `last_error`. Logs distinguish `handler finished` from `job confirmed`, so a
handler return is never presented as a committed queue outcome. Handler
registration is setup-only: call `Handle` before the single `Run`. Handlers must
deduplicate their own effects with `Job.ID` or another stable business ID.

What is **not** a source of duplicates: two workers receiving the same lease
(prevented by `SKIP LOCKED`), a broker restart (brokers are stateless; workers
reconnect and their leases keep ticking), or a scheduler failover (cron uses
idempotency keys and a guarded advance; §13 lists the two configuration and
retention windows in which a tick can still fire twice).

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

## 14. Verified behavior

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
| One scheduler is active across three instances; an ungraceful kill hands the role to a *different* instance, and the tick pending at the kill is fired exactly once | `integration: TestSchedulerFailover` (20 rounds per run, backend terminated, leader identified by `application_name`, `-race`) | 2 (`-count=2`, 40 rounds, 2026-09-22) | pass: ~20s per 20-round run |
| A newly declared schedule's first tick is computed from the database clock, not the broker's | `scheduler: TestReconcileIgnoresTheProcessClock` (`-race`) | 5 (`-count=5`, 2026-09-22) | pass |
| A tick already fired by a previous leader deduplicates rather than double-firing | `scheduler: TestDuplicateTickIsNotReFired` (`-race`) | 5 (`-count=5`, 2026-09-22) | pass |
| A stale advance from a displaced leader matches no row | `store: TestAdvanceScheduleIsGuarded` (`-race`) | 2 (`-count=2`, 2026-09-22) | pass |
| Releasing leadership never returns a locked connection to the pool | `store: TestReleaseDoesNotPoisonThePool` (`-race`) | 2 (`-count=2`, 2026-09-22) | pass |
| The real broker process schedules from a file and fails over on `SIGKILL` | `integration: TestBrokerProcessCronFailsOverOnKill` (two `-race` broker binaries) | 2 (`-count=2`, 2026-09-22) | pass |
| Two Compose brokers elect one scheduler; stop and `docker kill` of the leader both hand the role over, and cron jobs (including an `America/New_York` schedule in the distroless image) complete | `hopper-session6` Compose demo (`broker` + `broker-standby`, JSON logs + SQL state checks) | 1 (2026-09-22) | pass: graceful takeover in ~4s; after `docker kill` the pending tick fired ~2s late, then skipped ahead; 10/10 `sleep` jobs and the 18:30 UTC `cpu_burn` tick completed |
