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
  storm.
- **Request-level gRPC load balancing.** The broker Service balances per
  connection, and each worker holds one long-lived connection, so after a
  broker restart workers can stay concentrated on the surviving broker.
- **Scaling workers to zero.** `minReplicas` is at least 1. Scale-to-zero
  needs KEDA's activation threshold and accepts a cold start for the first
  job, and neither is designed here.
- **Choosing which workers to remove.** The replica set deletes pods without
  knowing which are busy; the drain (§11.4) is what makes that safe.
- **Autoscaling brokers, or several worker pools.** The chart scales one
  worker Deployment serving a fixed list of queues; brokers run a fixed count.
- **Production monitoring in the kind add-ons.** One Prometheus with a day of
  retention, no Alertmanager, no persistence.

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
| `hopper_job_wait_seconds` | histogram | `queue` | First lease minus due time (below) |
| `hopper_job_run_seconds` | histogram | `queue` | Ack minus lease, completed deliveries only |
| `grpc_server_*` | histogram, counters | `grpc_type`, `grpc_service`, `grpc_method`, `grpc_code` | The standard gRPC interceptor's metrics |

**Worker metrics** (`cmd/worker`).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hopper_worker_inflight_jobs` | gauge | — | Handlers executing right now |
| `hopper_worker_handler_seconds` | histogram | `queue`, `status` | Handler wall time; `status` is `ok`, `error` or `panic` |

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
error, logged at error level. Every other rejection, such as a stale-lease
Ack, is `rpc rejected` at debug level with its `code`. The broker's
per-delivery lines (`job enqueued`, `job leased`, `job acked`, `job nacked`)
are also at debug level, so they cost nothing by default and make the two logs
joinable when switched on. The worker's `handler finished` carries
`handler_ms`, the same duration the histogram observes, alongside
`duration_ms`, which counts from receipt of the delivery. Payloads are never
logged, and a lease id appears only under the `lease_id` key.

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
