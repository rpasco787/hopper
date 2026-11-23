# Confirmation batching (group commit)

Status: approved for Phase 1b H2 (2026-11-16), revised the same day after a
plan review (fallback isolation, cancellation and shutdown, test coverage,
exact counts, plan checks, the decision rule). Not yet measured. Read this
before `internal/broker/batch.go` or `internal/store/postgres/confirm_many.go`.
Results go in `docs/scaling.md` under H2.

## Problem

Every delivery costs an ownership Heartbeat and an Ack. Each is one statement
and one commit, so a job costs 2.24 commits (`docs/scaling.md`, baseline).
With H3a fixed (`654c140`), waiting for the WAL flush (`LWLock:WALWrite`) is
78% of active-backend samples at one broker and 82% at four, where about 38
backends are active at once, most of them queued for the flush. PostgreSQL
already shares flushes: 0.43 WAL syncs per job at one broker, 0.18 at four.
Batching aims at what is left: one statement, one transaction and one commit
for many confirmations, so fewer backends queue for each flush.

## Design

- **Where.** In each broker, between the gRPC handler and the store: one
  batcher for Ack, one for Heartbeat. They never share a transaction, so the
  Heartbeat that gates a handler's start never waits behind Ack traffic.
  Nack is not batched: it applies the retry policy row by row, and it is rare.
  The worker and the protocol don't change.
- **State.** A mutex, a FIFO of waiters {caller ctx, item, result, done
  channel}, a count of running flushes, and a channel that is closed while
  that count is zero.
- **Algorithm (no timer).** A call appends its waiter. If fewer than
  `flights` (2) flushes are running, it starts one. A flush repeatedly takes
  up to `max` (64) waiters still waiting, runs one statement for them, and
  closes each waiter's channel with its result. It exits when it finds the
  queue empty, decrementing the count under the same lock. A lone call runs
  at once, with no added latency. Under load, calls queue while statements run,
  and the next statement takes them all.
- **Statement.** `UPDATE jobs … FROM (locked rows)`. The CTE joins
  `unnest($ids, $lease_ids)` to `jobs` under the guard
  `(j.state || '') = 'leased'` (the H3a form: never the partial index) and
  locks with `FOR UPDATE SKIP LOCKED`. RETURNING names the (id, lease_id)
  pairs applied. The store answers `nil` for an applied pair, and
  `store.ErrNotApplied` for any other pair (its row was locked, or its guard
  failed). It never waits for a lock and never retries anything itself.
- **Fallback, per caller.** A caller whose pair comes back `ErrNotApplied`
  runs the existing one-row Ack or ExtendLease on its own RPC goroutine,
  under its own deadline, after its batch has committed. That call waits for
  the lock and names the miss exactly as today. The other callers in the
  batch already have their answers, and no flight is held.
- **Cancellation.** A statement's context has a 10 s cap, for callers with
  no deadline. It is also cancelled, through `context.AfterFunc` on every
  caller's context, when the last caller in its batch stops waiting, whether
  by deadline or by cancellation. A statement somebody still waits for is
  never cut short by another caller leaving. A statement nobody waits for is
  cancelled at once, so a client's deadline still cancels its confirmation's
  query when it's alone in its batch.
- **Shutdown.** `Server.Shutdown`, after the gRPC server has stopped, waits
  until no confirmation statement is running, bounded by the 10 s cap. By
  then every handler has returned, so any statement still running has no
  caller left and is already being cancelled. `cmd/broker` closes the pool
  only after that.
- **Switch.** `-confirm-batch` (`HOPPER_CONFIRM_BATCH`) is **off by default**.
  A broker with it above 1 logs `confirmation batching on` at startup. If its
  store can't batch, it logs a warning instead and confirms one call at a
  time. `-confirm-flights` (default 2) exists to measure 1 against 2.

## Invariants

1. No call is answered before the statement that applied it has committed
   (`docs/semantics.md` §11.3 is unchanged).
2. Every queued waiter is taken by a running flush: the count is decremented
   only under the lock, after seeing the queue empty.
3. A batch statement never waits for a row lock. A locked row delays only its
   own caller, through that caller's own one-row call, and never another
   caller or a flight.
4. Each call's final result is what the one-row call would have returned,
   except that a statement failure fails every call in that batch.
5. At most `flights` statements of each kind per broker. Each runs while at
   least one of its callers waits, and for at most 10 s.
6. Once `Server.Shutdown` returns, no confirmation statement is running.

## Failures

- **Database unreachable:** every call in the batch gets UNAVAILABLE, and
  the SDK retries each under its own token, as today.
- **Connection lost after commit, before the reply:** UNAVAILABLE. The
  retry finds the row completed under the same lease_id, which is OK (§5).
- **Caller's deadline passes while queued:** it returns DEADLINE_EXCEEDED at
  once and is left out of the next statement.
- **Caller's deadline passes while its statement runs:** it returns
  DEADLINE_EXCEEDED. The statement runs on if another caller still waits, and
  may commit. The retry is idempotent. If it was the last caller, the
  statement is cancelled.
- **Row locked by another transaction:** skipped. Its caller waits for it on
  the one-row path, under its own deadline, after the batch has committed.
- **Broker drain:** GracefulStop waits for unary calls, which wait for their
  statement. Shutdown then waits for any statement whose callers have left.
  **Crash mid-batch:** the same as a crash mid-Ack today.

## Alternatives considered

- **A 1–2 ms linger timer:** adds latency to every call at low load, while
  natural batching gets the same grouping under load.
- **Retrying skipped pairs inside the store method:** the first draft did
  this. Every caller in the batch then waited for the slowest locked row, and
  the flight stayed occupied.
- **A statement deadline copied from the callers (the latest one):** the
  first revision did this. A copied deadline doesn't carry cancellation, so a
  statement whose callers had all left ran on, up to the cap, past shutdown.
- **Waiting for locks in id order (`ORDER BY id FOR UPDATE`):** also
  deadlock-free, since the reaper skips locked rows and Nack and Cancel each
  lock one row. But one slow holder would then delay every other caller in
  the batch. Revisit if H2 measures the fallback share as large.
- **One transaction for Ack and Heartbeat together:** fewer commits still,
  but it couples the heartbeat that gates handler starts to Ack latency.
  Revisit only if separate batchers help and the remaining commits matter.
- **A batch RPC from the worker:** a protocol change, and under round robin a
  worker's confirmations go to different brokers anyway.
- **Server settings:** `synchronous_commit=off` gives up durability (§1);
  `commit_delay` was measured and doesn't help (baseline).

## What changes for callers

- Nothing while `-confirm-batch` is off.
- When it's on, a call's statement may be shared. It runs while any caller in
  the batch waits, so one caller's deadline cancels it only when that caller
  is the last one waiting.
- Two heartbeats for one delivery in one batch apply the first duration. The
  SDK pins one grant per delivery, so this doesn't arise from it.

## How H2 is measured

In one interleaved loop on one build (`654c140` or later), 5 runs each at
1, 2 and 4 brokers, with the order of the variants rotated in each
repetition:
- batching off (control);
- batching on, `flights=2`;
- batching on, `flights=1`;
- `synchronous_commit=off` with batching off, which is the flush-wait ceiling
  on the fixed build (the baseline's was measured before H3a).

These are variants of one build, set through `BROKER_ENV`.

Each run records, from exact counts:
- the statements per job;
- the mean batch size as submitted, which is the confirmation RPCs per batch
  statement (PostgreSQL's row count is what a batch applied, not what it was
  given);
- the fallback share per kind, which is the one-row statements per
  confirmation RPC.

**Turning it on.** A batched variant qualifies if, against batching off:
- it helps at some broker count: a median at least 10% better, with the
  ranges apart;
- it is no worse at any broker count: a median at least 0.95× batching off's,
  and Ack and Heartbeat p99 at most 1.5× batching off's;
- its fallback share is at most 10% per kind at every broker count.

Both flight counts are judged against batching off directly. If both
qualify, `flights=2` stays the default, unless `flights=1` beats it under the
evidence rule. Turning batching on is the user's decision, made from these
numbers.

**Prediction:** commits per job fall from about 2.2 to about 0.3–0.5, and the
flush-wait share and active backends fall with them. If throughput doesn't
move, the conclusion is "no net gain from this implementation on this setup".
It doesn't show that commits never limited throughput. The waits and
statement times it leaves behind say where to look next, likely H4 (the
ownership heartbeat).

## A batch's wait for a connection

Status: designed 2026-11-23, before code. Evidence in docs/semantics.md §11.3
and §15.

**Problem.** A flight is shared. Its statement first needs a connection, and
after PostgreSQL stops the pool must dial. A dial to the stopped container's
address gets no answer until the connect timeout (`DefaultDBConnectTimeout`,
5 s), the same as the worker's per-attempt deadline (`confirmRPCTimeout`, 5 s).
The calls queued behind that flight end with `DEADLINE_EXCEEDED` at the worker,
or succeed once the database returns, instead of being answered `UNAVAILABLE`.
`TestPostgresRestartLosesNoAck/graceful` failed 3 of 8 at a batch of 64 with
two flights, and 0 of 8 at a batch of 1 or with 16 flights.

**Rule.** A batch statement (`AckMany`, `ExtendLeaseMany`) waits at most
`batchAcquireTimeout` = 1 s for its connection, including any dial it waits on.
- If that bound passes first, every call in the batch is answered
  `UNAVAILABLE`. Nothing was sent, so the retry is plainly safe.
- If the caller's own deadline or cancellation ends first, it is reported as
  before; it is not the database's absence.
- The statement, once it has a connection, keeps the 10 s cap and the
  cancellation when its last caller leaves.
- The one-row paths are unchanged.

**Why 1 s.** It is well inside the worker's 5 s attempt, so the answer arrives
while the caller still waits. A healthy acquire takes well under a millisecond
in a drain.

**Cost.** When the pool stays full for over a second, as during a replication
pause where acquires waited 13-14 s, batches answer `UNAVAILABLE` after 1 s and
workers retry with backoff, where before they waited. Confirmations are
idempotent under their lease token, so nothing is applied twice.

**Rejected.**
- More flights by default: they only make a hold less likely.
- A shorter connect timeout for the whole pool: it also governs the wakeup
  listener and the scheduler, and a dial still holds a flight for its length.
- A watchdog per flight: more machinery for the same bound.
