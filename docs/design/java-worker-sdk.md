# The Java worker SDK (Phase 3b)

Status: designed 2026-11-23, before any code (ground rule 1). It implements the
worker side of docs/semantics.md a second time, in Java 25 on virtual threads,
as `java/hopper-worker`, together with the example worker 3c runs,
`java/hopper-example-worker`. docs/semantics.md is the contract: where this
document disagrees with it, this document has a bug. The Go SDK (`worker/`) is
the reference implementation: where this document is silent, do what it does.
Results go in docs/semantics.md §15.

## What it must keep

- **No two holders at once, as far as a client can promise it.** A handler
  runs only while the SDK believes, conservatively and on its own monotonic
  clock, that the delivery's lease is live, and is stopped as soon as that
  belief ends.
- **The concurrency bound.** Free tokens + credit outstanding + active
  deliveries = concurrency, always. A token leaves the pool when the credit it
  stands for is sent, and the delivery that credit buys inherits it. It
  returns when that delivery's confirmation has ended, whatever the handler
  did. A handler that ignores its stop keeps its token until it returns
  (docs/semantics.md §4, Flow control).
- **The start rule**, below, exactly.
- **The outage window of §11 item 3:**
  - renewals are G/3 apart, counted from the previous renewal's return;
  - each attempt is bounded by the earlier of G/3 and the local deadline;
  - a confirmation is retried only on `UNAVAILABLE` and `DEADLINE_EXCEEDED`,
    with backoff (100 ms doubling to a 5 s nominal cap, ×0.75-1.25), until
    the local deadline or the shutdown deadline;
  - a failed call never moves the local deadline.
- **One shutdown deadline** for stream teardown, handlers and confirmations,
  with separate counts of what it abandoned (§11 item 4).
- **The log contract** that test/accounting and the chaos suite read (Logs,
  below).

## The start rule (normative)

Copied from docs/design/credit-anchored-freshness.md, which is authoritative.

- **Stamp.** Each stream keeps, oldest first, the instant each unit of credit
  was handed to the transport: one monotonic reading taken just before the
  `Send` of the subscription or credit message that carries it. Units reserved
  but never sent (the drain path) carry no instant.
- **Match.** Each arriving delivery is matched to the oldest unit not yet
  matched on its stream. A delivery with no unit left is an over-delivery: the
  stream ends and the job is neither started nor counted.
- **Decide.** Let s be the matched unit's send instant, r the instant the
  delivery was read off the stream, and G its grant. If s exists and
  0 ≤ r − s < G/10, the handler starts at once, with local deadline
  s + G − min(0.5 s, G/10). That is `deadlineFrom(s, G)`.
- **Otherwise check.** The SDK sends the bounded ownership Heartbeat before
  the handler, exactly as before.
- **Clocks.** Every instant is the worker's own monotonic clock. No instant
  from the database is compared with one from the worker.
- **Reconnect.** A new stream starts with no units. Its subscription's credit
  is stamped at that subscription's `Send`. Units left on an ended stream are
  forgotten when its credit is reclaimed.

In Java:
- **s.** `TimeSource.nanoTime()`, read immediately before `onNext` of the
  subscription or credit message. It is read inside the ledger's lock, which
  also appends the units and their count, and the lock is released before
  `onNext`. So a delivery that arrives before `onNext` returns finds its units,
  and a send that blocks never stops the reader from matching.
- **Unsent units.** Units the writer reserves on the drain path, which it
  never sends, are appended with `sent = false`. A delivery matched to one
  takes the check.
- **Match, then r.** The match happens first in the inbound `onNext`, and r is
  read immediately after it. Then the delivery's virtual thread starts.
- **Fresh.** r − s is computed by subtracting the two `long`s. A delivery is
  fresh if and only if its unit was sent, G > 0, 0 ≤ r − s, and r − s < G/10,
  with G/10 in integer nanoseconds. Its deadline is
  s + G − min(500 ms, G/10).
- **A new call** is a new session with an empty ledger, and `reconcile`
  empties the ledger when the call ends.
- **The broker's half**, leasing only against credit it has received, is the
  broker's contract (credit-anchored-freshness.md, "The broker invariant").
  The SDK's half is never to stamp late, and never to match a delivery to a
  unit younger than the one that bought it.
- **No other way to skip the check.** No public option starts a delivery
  without this rule or the check. The package-private
  `checkEveryDelivery(true)` exists only so tests can pin the check path.

## Threads

| Thread | Kind | Does | Never does |
|---|---|---|---|
| The caller's, inside `run()` | the caller's | opens sessions, waits for each to end, backs off, then drains | — |
| grpc callback executor | grpc's default | inbound `onNext`: match, parse, start the delivery's thread. `onError` and `onCompleted`: end the session | block, call the network, run user code |
| writer, one per session | virtual | waits for free tokens; stamps, reserves and sends `onNext(credit)`; sends `onCompleted()` on drain | read deliveries |
| delivery, one per delivery | virtual | the check, the handler and the confirmation; releases its token, then ends its work | outlive its token |
| renewal, one per attempt | virtual | one Heartbeat, then arms the next renewal | block the scheduler |
| scheduler | one daemon platform thread | timers: renewals, expiry, backoff wakeups and the shutdown deadline | block or make an RPC: each task finishes at once or hands off |

No lock is held across user code, an RPC, an `onNext` or any blocking call.
No grpc channel uses `directExecutor()`.

## Go to Java

| Go | Where | Java |
|---|---|---|
| `Options`, `New`, `Handle` | `worker.go:68-182` | `HopperWorker.builder(target)…handle(queue, handler)…build()`. The builder is the only place to register, so "Handle after Run" can't happen. A duplicate, empty or null registration throws at `handle(...)`, and no handlers at all throws at `build()`. |
| `Run(ctx)` | `worker.go:199-207` | `run()` blocks. `shutdown()` begins the drain from any thread. Interrupting the thread in `run()` also begins it. `run()` is single-use. |
| `forceCtx` (`context.WithoutCancel` + cancel cause) | `lifecycle.go:28-34`, `72` | A root `io.grpc.Context.CancellableContext`. Every RPC runs attached to it or to a child, so cancelling it cancels the stream, the renewals and the confirmations, as cancelling `forceCtx` does in Go. |
| handler `context.Context` and its cause | `execute.go:34`, `worker.go:59-64` | A `JobContext` with `stopReason()` (`LEASE_LOST`, `LEASE_EXPIRED`, `JOB_CANCELLED`, `SHUTTING_DOWN`), plus interruption of the handler's thread. The reason is set first, then the interrupt. |
| `capacity` (a token channel) | `stream.go:41-78` | `Capacity`: a counter under a lock with a condition. `takeOneOrWake`, `takeAll` and `release`; an over-release is logged and dropped. |
| `session.reserve`/`spend`/`reconcile`, `sends` | `stream.go:326-382` | `CreditLedger`: `reserveAndSend` (stamp and count under the lock, then send outside it), `reserveUnsent`, `spend` and `reconcile`, over a `Deque` of unit runs. An instant is a `long` from `TimeSource.nanoTime()` plus a `sent` flag, because there is no zero `time.Time`. |
| reader goroutine (`read`, `accept`) | `stream.go:230-266` | The `ClientResponseObserver.onNext` callback, on grpc's callback executor. It matches, parses and starts the delivery's virtual thread, and never blocks or calls the network. |
| writer goroutine (`write`, the one `Send` owner) | `stream.go:284-317` | One virtual thread per session. It is the only caller of the request observer's `onNext`/`onCompleted`, which are not thread-safe. |
| `dispatch` goroutine per delivery | `stream.go:271-280` | `Thread.ofVirtual().name("hopper-delivery-<n>").start(…)`. It releases the token, then ends the work, exactly in Go's order. |
| `heartbeat.monitor` goroutine | `heartbeat.go:291-316` | A task on the `TimeSource` scheduler, armed at `deadline − now` and re-armed on a moved deadline. |
| `heartbeat.renew` goroutine | `heartbeat.go:322-403` | A scheduler task every G/3, measured from the previous renewal's return. Each RPC runs on a fresh virtual thread, so the scheduler thread never blocks. |
| `heartbeat.stop` (flag, close, cancel, `wg.Wait`) | `heartbeat.go:279-286` | Set `stopping`, cancel the timers, cancel the heartbeat's child `CancellableContext`, then join the in-flight renewal thread. |
| `lifecycle`, `work`, `phase` | `lifecycle.go` | The same names and the same counters, under one `ReentrantLock`. `enforceDeadline` is a scheduler task. |
| `confirmer.confirm` | `confirm.go:90-132` | The same loop. Each attempt's deadline is `withDeadlineAfter(min(5 s, deadline − now))`. Backoff waits run on the `TimeSource`, and the forced shutdown interrupts them. |
| `backoff` | `backoff.go` | `Backoff`: 100 ms doubling, a 5 s nominal cap, ×[0.75, 1.25) jitter, and an injectable jitter source. |
| `slog` with `With("worker_id", …)` | `worker.go:146`, `execute.go:27-28` | `EventLog`, an SDK interface, with `JsonLinesLog`, its JSON-lines implementation (Decision D10). |
| `Observer` | `observer.go` | `WorkerObserver`, called synchronously on the delivery's thread. |
| `checkEveryDelivery` (unexported) | `worker.go:92-95`, `execute.go:116-121` | The package-private `Builder.checkEveryDelivery(boolean)`. |
| `w.runner` (test seam) | `worker.go:113-115` | The package-private `Builder.runner(DeliveryRunner)`. |
| `healthySession` (test seam) | `worker.go:117-120` | The package-private `Builder.healthySession(Duration)`. |

## Time

All time goes through a package-private `TimeSource`:

```java
interface TimeSource {
    long nanoTime();
    /** Runs task once, delayNanos from now. Tasks must not block. */
    Timer schedule(Runnable task, long delayNanos);
    interface Timer { void cancel(); }
}
```

`SystemTimeSource` reads `System.nanoTime()` and schedules on one
`ScheduledThreadPoolExecutor` thread (daemon, remove-on-cancel). Tests use
`ManualTimeSource`, whose clock moves only when a test calls `advance`, which
runs due tasks in deadline order, or `jump`, which doesn't, to simulate a
pause. grpc deadlines are relative: an RPC gets
`withDeadlineAfter(remaining, NANOSECONDS)`, with `remaining` computed on the
TimeSource. Monotonic instants are compared only by subtracting one from the
other.

Both SDKs' monotonic clocks are `CLOCK_MONOTONIC` on Linux, which doesn't
advance while the machine is suspended. That is an assumption both make, like
the database clock running at the real rate over G.

## Stopping a handler

A handler gets a `JobContext`: the `Job`, plus `isStopped()`, `stopReason()`
and `throwIfStopped()`. The SDK stops a handler for one of four reasons, the
counterparts of the Go SDK's context causes:

| Reason | When | Go |
|---|---|---|
| `LEASE_LOST` | a Heartbeat answered `ABORTED` or `NOT_FOUND`, or any other refusal | `ErrLeaseLost` |
| `LEASE_EXPIRED` | the local deadline passed | `ErrLeaseExpired` |
| `JOB_CANCELLED` | a Heartbeat answered `FAILED_PRECONDITION` | `ErrJobCancelled` |
| `SHUTTING_DOWN` | the shutdown deadline fired | `ErrShuttingDown` |

**The order.** Stopping sets the reason first, and the first reason wins.
Then it interrupts the handler's thread, so a handler that catches
`InterruptedException` can read why it was stopped.

**The interrupt is sent only while the handler runs**, under the
`JobContext`'s lock. When the handler returns, the SDK marks it finished
under the same lock and clears the thread's interrupt flag. The thread is the
SDK's, and a stale interrupt would cancel the confirmation that follows.
Wherever else the SDK catches `InterruptedException`, it restores the flag.

**A handler that ignores both interruption and `isStopped()` can't be
stopped:** Java has no way to kill a thread. It keeps its token until it
returns, so the worker never runs more handlers than its concurrency. Its job
may meanwhile be redelivered elsewhere (§11 item 2).

## Pauses

A long GC pause, a `SIGSTOP` or a frozen VM is the frozen-worker case. No
timer fires during it, so nothing can stop a handler mid-pause, and the next
holder may already be running (§11 item 2; Phase 5a's fencing). What the SDK
must not do is act on a lease it no longer holds once it wakes:
- **Confirmations.** Each attempt compares the clock with the local deadline
  immediately before it is sent (the Go SDK's confirm.go:100-103). A result
  whose deadline passed during the pause isn't confirmed. It logs `job left
  unconfirmed; its lease ran out and it will be redelivered`.
- **Renewals.** A renewal that returns after its lease's deadline has passed
  can't revive the lease (`Lease.extend` refuses, as heartbeat.go:83-89 does).
- **Expiry.** The expiry timer fires late, sees that the deadline has passed,
  and stops the handler.

A pause between that last check and the send itself is covered by neither
SDK. The store's `(id, lease_id)` guard then decides, and idempotent handlers
cover the rest.

## Flow control

grpc-java's request observer buffers, so `onNext` never blocks. The writer
sends one credit message per wakeup, carrying every token free at that moment,
so at most `concurrency` credit messages can ever be unread. The outbound
buffer is therefore bounded without consulting `isReady()`. Inbound messages
are requested automatically, and the reader never blocks, so they can't back
up. Credit is `uint32` on the wire, so concurrency is at most 65,536, as in
Go.

## Shutdown

- **What begins it.** `shutdown()`, an interrupt of the thread inside `run()`,
  or a permanent stream failure. The first of these records
  deadline = now + shutdownTimeout.
- **The drain.** The writer half-closes. The reader keeps accepting deliveries
  already in flight until the broker ends the stream. A delivery that loses
  the acceptance race is nacked unstarted.
- **At the deadline**, one scheduler task:
  - snapshots the counts;
  - cancels the root grpc context, which ends the stream, every renewal and
    every confirmation;
  - stops every running handler with `SHUTTING_DOWN`;
  - logs `shutdown deadline reached`.

  `run()` then throws `WorkerFailedException` with the counts: active
  deliveries, running handlers, acquiring and confirming. A permanent stream
  failure is reported in the same exception, so neither hides the other (as
  `errors.Join` does in Go).

The example worker drains on `SIGTERM` from a JVM shutdown hook:
- the hook calls `shutdown()`;
- it waits until `main` has logged `worker stopped`, or failed;
- it flushes stdout and halts with 0 (clean) or 1 (anything else).

Without the halt, a JVM ended by a signal exits 143, where the Go worker exits
0.

## Reconnect

As the Go SDK's `consume` does (worker.go:240-304), after a session ends:
- if the worker is draining, or the code is permanent (`INVALID_ARGUMENT`,
  `UNIMPLEMENTED`, `PERMISSION_DENIED` or `UNAUTHENTICATED`), it stops;
- otherwise it waits a jittered backoff, which resets when the session
  delivered anything or stayed up for 5 s;
- then it opens a new session advertising only the tokens free now.

The leases of deliveries still running are untouched.

## Logs

The example worker writes every line to stdout as one JSON object. It holds
these keys:
- `time` (RFC 3339, UTC), `level`, `msg`, then `worker_id`, then the line's
  own fields;
- on delivery-scoped lines: `job_id`, `lease_id`, `queue` and `attempt`;
- on heartbeat lines: `job_id`, `lease_id` and `queue`.

A lease id appears only under `lease_id`, and payloads are never logged.
Nothing but these lines may reach stdout or stderr: test/accounting fails a
run with any line that isn't JSON.

| `msg` (exact) | Level | Fields beyond the scope's | Emitted by | Read by |
|---|---|---|---|---|
| `worker starting` | INFO | `broker`, `concurrency`, `lease_duration`, `shutdown_timeout` | example worker (Go: `cmd/worker/main.go:186`) | accounting 24, 242-249 (starts a process) |
| `worker stopped` | INFO | — | example worker (`main.go:193`, `231`) | accounting 31, 291-296 |
| `lease stream opened` | INFO | `queues`, `credit` | SDK (`stream.go:185`) | chaos `stack_test.go:152`, `rebalance_test.go:42`; `scripts/bench.sh:390` |
| `lease stream closed` | INFO | `delivered` | SDK (`stream.go:208`) | chaos `rebalance_test.go:44` |
| `lease stream failed; reconnecting` | INFO if the session was healthy and the code is `UNAVAILABLE`, else WARN | `err` | SDK (`worker.go:276-283`) | chaos `broker_test.go:13-16` |
| `lease stream closed; reconnecting` | WARN | — | SDK (`worker.go:282`) | chaos `broker_test.go:13-16` |
| `not opening another lease stream; the worker is draining` | INFO | — | SDK (`worker.go:296`) | — |
| `shutdown started` | INFO | `reason`, `deadline`, `timeout`, `active_deliveries`, `running_handlers` (integers) | SDK (`lifecycle.go:125-130`) | accounting 29, 282-286; integration `worker_process_test.go:31-32` |
| `shutdown deadline reached` | WARN | `active_deliveries`, `running_handlers`, `acquiring`, `confirming` | SDK (`lifecycle.go:152`) | accounting 30, 287-290 |
| `job started` | INFO | — | SDK (`execute.go:75`) | accounting 25; chaos |
| `handler finished` | INFO | `err` (null on success), `handler_ms`, `duration_ms` (from receipt) | SDK (`execute.go:91-93`) | accounting 26; chaos `postgres_test.go:89` |
| `job confirmed` | INFO | `operation` (`ack`/`nack`), `outcome` | SDK (`confirm.go:154`) | accounting 27, 273-279; chaos |
| `nacking an unstarted job` | INFO | `reason` | SDK (`execute.go:133`) | accounting 28 |
| `lease not confirmed; leaving the job to be redelivered` | WARN | `err` | SDK (`execute.go:52`) | — |
| `discarding a malformed delivery` | ERROR | `err`, `job_id`, `lease_id` (raw) | SDK (`stream.go:257`) | — |
| `lease expired locally; cancelling the handler` | WARN | `deadline` | SDK (`heartbeat.go:302`, `347`) | accounting `lossMessages` |
| `heartbeat rejected; cancelling the handler` | WARN | `err` | SDK (`heartbeat.go:383`) | accounting `lossMessages` |
| `lease expired while applying a renewal; cancelling the handler` | WARN | `err` | SDK (`heartbeat.go:397`) | accounting `lossMessages` |
| `job confirmation refused; someone else may have it now` | WARN | `operation`, `outcome`, `err` | SDK (`confirm.go:156`) | accounting `lossMessages` |
| `job left unconfirmed; its lease ran out and it will be redelivered` | WARN | `operation`, `outcome`, `err` | SDK (`confirm.go:159`) | accounting `lossMessages` |
| `job left unconfirmed; the worker was shutting down` | WARN | `operation`, `outcome`, `err` | SDK (`confirm.go:162`) | accounting `lossMessages` |
| `handler returned after the shutdown deadline; not confirming` | WARN | `cause` | SDK (`execute.go:100`) | accounting `lossMessages` |
| `retrying a confirmation` | DEBUG | `operation`, `attempt`, `err` | SDK (`confirm.go:117`) | — |
| `worker: released more capacity than exists; this is an SDK bug` | ERROR | `concurrency` | SDK (`stream.go:76`) | — |

## API

```java
public final class HopperWorker {
    public static Builder builder(String target);        // dials target, owns the channel
    public static Builder builder(ManagedChannel channel); // caller-owned; never shut down
    public void run();                                     // blocks; throws WorkerFailedException
    public void shutdown();                                // idempotent, any thread
    public boolean awaitTermination(Duration timeout) throws InterruptedException;

    public static final class Builder {
        public Builder concurrency(int n);                 // 1..65536; default 8
        public Builder leaseDuration(Duration d);          // ZERO = the broker's default
        public Builder shutdownTimeout(Duration d);        // ZERO = 30 s
        public Builder workerId(String id);                // default <hostname>-<pid>
        public Builder log(EventLog log);                  // default JSON lines on System.err, INFO
        public Builder observer(WorkerObserver observer);  // default: none
        public Builder credentials(ChannelCredentials c);  // default plaintext
        public Builder handle(String queue, JobHandler handler);
        public HopperWorker build();
        // package-private seams: checkEveryDelivery(boolean), timeSource(TimeSource),
        // runner(DeliveryRunner), healthySession(Duration)
    }
}

@FunctionalInterface
public interface JobHandler { void handle(JobContext ctx) throws Exception; }

public interface JobContext {
    Job job();
    boolean isStopped();
    Optional<StopReason> stopReason();
    void throwIfStopped() throws JobStoppedException;
}

public final class Job {          // immutable; payload() returns a copy
    UUID id(); String queue(); byte[] payload(); String payloadUtf8();
    int attempt(); int maxAttempts(); Instant enqueuedAt();
}

public enum StopReason { LEASE_LOST, LEASE_EXPIRED, JOB_CANCELLED, SHUTTING_DOWN }
public final class JobStoppedException extends Exception { public StopReason reason(); }
public enum HandlerStatus { OK, ERROR, PANIC }
public interface WorkerObserver {
    void handlerStarted(String queue);
    void handlerFinished(String queue, HandlerStatus status, Duration elapsed);
}
public interface EventLog {
    enum Level { DEBUG, INFO, WARN, ERROR }
    boolean enabled(Level level);
    void log(Level level, String msg, Object... keyValues);
    EventLog with(Object... keyValues);
}
public final class JsonLinesLog implements EventLog {
    public JsonLinesLog(PrintStream out, EventLog.Level minimum);
}
public final class WorkerFailedException extends RuntimeException {
    public Optional<io.grpc.Status> permanentFailure();
    public Optional<AbandonedCounts> abandoned();
}
public record AbandonedCounts(int deliveries, int handlers, int acquiring, int confirming) {}
```

A handler that returns is acked. One that throws an `Exception` is nacked
with `toString()`, and the observer sees `ERROR`. One that throws an `Error`
is nacked with `toString()` and its stack trace, the observer sees `PANIC`,
and the worker carries on. Every message is made valid UTF-8, stripped of NUL
and bounded to 4,096 bytes, `… (truncated)` included.

## Testing

- **The fake broker.** An in-process fake (`java/hopper-proto/src/testFixtures`)
  mirrors worker/fake_test.go: scripted unary hooks, per-stream credit,
  deliveries that spend credit or don't, recorded requests, and a cleanup that
  proves every handler returned.
- **Ports of the Go tests.** Every Go worker test is ported, with the same
  name in lowerCamelCase and the same assertions. The exceptions are the
  tests of Go-only machinery, which the plan lists. A Go test that waits on
  wall time runs on `ManualTimeSource` instead.
- **Java-only tests:**
  - the stamp before `onNext`;
  - interrupt hygiene;
  - the pause;
  - `Error` against `Exception`;
  - the shutdown hook's exit codes.
- **End to end.** test/accounting/javalog_test.go checks the log contract
  against a real Compose run.

## Rejected

- **A permit acquired when a delivery arrives.** It counts the token twice:
  the token already left the pool with its credit.
- **`StructuredTaskScope`.** It is a preview API through JDK 27.
- **Async stubs for renewals.** Callbacks on grpc's executor need the same
  care as a blocking call on a virtual thread, and read worse.
- **SLF4J.** Its JSON encoders don't produce the contract's keys without
  configuration this module would then have to test. An adapter can come
  later.
- **Exposing the lease id on `JobContext`.** The Go SDK doesn't (job.go:11-16);
  Phase 5a decides for both SDKs.
- **A public option to skip the check.** The benchmark switch was retired for
  being unsafe by design.
- **Java options in proto/.** They would change the descriptor gen/ embeds;
  buf.gen.java.yaml's managed mode sets them instead.

## Results (2026-11-23)

Measured on branch `phase-3-java`; the plan is
docs/plans/2026-11-23-phase-3a-3b.md. Nothing below claims a run that wasn't
made.

**Test counts** (each module's last run; every count is `failed=0`,
`skipped=0`, and javac `-Xlint:all -Werror` reports no warning):

| Module | Tests | Of which |
|---|---|---|
| `hopper-proto` | 20 | the stubs, the channel defaults, and `FakeBrokerTest`: 13 of worker/transport_test.go's tests of the fake, ported to where the fake now lives |
| `hopper-client` | 38 | ports of client/client_test.go and the 3a note's code table, plus `compose`-tagged smoke tests run only by scripts/java-smoke.sh |
| `hopper-worker` | 187 | 181 test methods (parameterized ones run several times). 102 methods port 94 of the Go SDK's 110 tests: 89 by name in lowerCamelCase, 3 renamed for Java's trigger (`callerShutdownBeginsDrain`, `drainListenersRunOnce`, `buildRejectsConfigurationBeforeDialing`), and `TestLeaseTransitions` and `TestSanitizeError` split into their 5 subtests each. 79 methods are Java-only (the stamp before `onNext`, interrupt hygiene, pauses, `Error` against `Exception`, the manual clock). Not ported: `TestForceContextKeepsValuesButNotCancellation` (a Go context's values), `TestAnAlreadyCancelledContextDrainsBeforeNewLifecycleReturns` (covered at the worker level by `ShutdownTest.aShutdownBeforeRunOpensNoStream`), `TestJobUnmarshal` (Java's `Job` has no unmarshal) |
| `hopper-example-worker` | 45 | 10 ports of cmd/worker/main_test.go and 35 Java-only, among them `MainProcessTest`, which runs the worker as its own JVM and sends it a real SIGTERM. Not ported: the three metrics-server tests (no metrics, Decision D24) and `TestDurationOverflowFixtureFitsInt64` (Go fixture arithmetic; its literal is reused) |

The start rule's own tests are 23 of the worker's: `StartRuleTest` 3,
`CreditLedgerTest` 8, `CreditStampTest` 3, `StartRuleSessionTest` 5 and
`FreshnessTest` 4.

**Reruns.** The worker suite ran three times more with `--rerun-tasks` at
`4e13996`, 187 of 187 each; the example worker three times at `e39bd1c`, 45 of
45 each. One flake was found on the way and fixed in the test (see Findings).

**Against the Compose stack**, project `hopperjava`, PostgreSQL and one broker
with a 30 s lease, the worker at concurrency 4, through scripts/java-smoke.sh
and test/accounting/javalog_test.go. The evidence is
[docs/bench/java-smoke/20261123/](../bench/java-smoke/20261123/).

- **Undisturbed** (240 jobs over the three queues): accounting passes,
  `AttemptProblems` empty. 240 of 240 completed, one process, 20 redelivered
  and 20 restarted (the `fail_n_times` retries, each repeat explained as
  nacked), no shutdown, no non-JSON line.
- **SIGTERM** mid-run (400 `sleep` jobs, the signal after 20 starts, then a
  restart): exit 0; accounting passes with `Problems` empty. 400 of 400
  completed over two processes, 0 redelivered, and `shutdown started` carried
  `running_handlers` 4.
- **Cancel while running.** The first run failed the check. The client
  cancelled 15.7 ms after the lease, before the worker's ownership check (the
  delivery was stale, 5.2 s after its credit), so the check was refused
  `FAILED_PRECONDITION` and the handler never ran. That is the Go SDK's path
  too, but not the running case the check is about. The driver now cancels 2 s
  after `LEASED` (`3461424`). The rerun passed: the delivery, stale by 4.6 s,
  took the check, and the first renewal was refused 10.05 s after the handler
  started (G/3 = 10 s). The handler stopped 2 ms later on its interrupt, the
  job was left unconfirmed, and no `job confirmed` was logged for it.
- **Abandonment** (two 20 s jobs, a 2 s budget): exit 1 and one
  `shutdown deadline reached` line with its four counts by name. stderr held
  `worker: worker: shutdown deadline reached with 2 active deliveries (2
  running handlers, 0 acquiring a lease, 0 awaiting confirmation)`. Both jobs
  completed after the restart.
- **Negative controls**, on copies of those logs. A deleted `job started`, an
  inserted non-JSON line and a deleted nack confirmation each fail the check.
  A deleted ack confirmation passes; see Spec gaps.

**Findings.**
- **Equivalent mutants**, each explained and none killed with a contrived
  test:
  - `CreditLedger`'s `reconciled` flag: the first reconcile already zeroes
    what is outstanding (D11a).
  - `HandlerContext`'s reason-then-interrupt order: the lock, not the order,
    is the guarantee (D13d).
  - A reconnect reusing a ledger that reconcile has already emptied (D15).
  - `StartRule` without its `sent` check, through a session: no session test
    can deliver against an unsent unit without a seam (D17).
  - The example worker's flushes before `halt`: every line is already
    flushed (D19).
- **A flake in the tests, not the SDK** (D17): a wait on the fake's call
  count, which counts a call before its hook runs. Fixed by waiting on the
  hook's own record.
- **Sealed jars** (D35): every jar is sealed except `hopper-proto`'s, whose
  tests share its packages. It holds none of the package-private seams.
- **What in-process grpc-java can't show** (D5b, D5c): a failed first send
  has already sent the call's headers, and a failed `onNext` doesn't throw.
  The fake's stream-control tests assert the same intent another way.
- **Outside this work** (D0): the Go baseline's
  `internal/scheduler TestReconcileFailsWhenTheClockIsUnavailable` failed
  once before any of this. It is reported, and not touched.

## Spec gaps found writing it

These are the 3c talking points. Only the first was corrected, in
docs/semantics.md; the others need Go changes this work didn't make.

- **§11 item 5** said "After bounded ownership validation, the SDK Nacks that
  known-unstarted delivery immediately". A fresh delivery for an unregistered
  queue, or one that loses the race with shutdown, is nacked with no
  validation at all (worker/execute.go:42-71). It now reads "After the start
  rule (§4) or bounded ownership validation". Both SDKs behave this way.
  - Phase 3c: no scenario. An unregistered queue cannot occur against a real
    broker, which leases only the queues a worker subscribed to, and both workers
    subscribe exactly to their handlers. A delivery that loses the race with
    shutdown is nacked unstarted; scenario 11 reports how many it saw (0 unstarted
    nacks and 0 never received, on both SDKs, 2026-11-24).
- **Go's `shutdown deadline reached` carries no counts.** It logs
  `"counts", snap`, where `counts` has only unexported fields
  (worker/lifecycle.go:64-66, 152). Checked on 2026-11-23 with a ten-line
  program in the same shape: slog's JSON handler renders it `"counts":{}`. The
  four counts §11 item 4 promises are only in the returned error. Java logs
  them by name. The Go fix is one line, with a test in
  worker/lifecycle_test.go.
  - Phase 3c: fixed in 03b220e, the branch's first commit; the Go line now
    names the four counts with Java's keys. Scenario 12 judges both SDKs' line,
    exit code and stderr (counts 2/2/0/0, stderr identical), and its negative
    control 17 shows the old line, `counts {}`, failing.
- **§11 item 3's pinning tests.** The doc says the four window tests pin the
  check path with an unexported option. Only the first two do, by calling
  `acquire` directly. The last two run a fresh delivery, so the Heartbeat
  their hook answers first, which their comments call "the check", is the
  first renewal at G/3. Their assertions still hold; the sentence and the
  comments are stale.
  - Phase 3c: a mutant that lets a failed renewal move the deadline survived
    the old tests and died once the last two ran with the unexported
    `checkEveryDelivery` option, so the check path was restored in both SDKs'
    test setup (no assertion changed) and the semantics.md sentence says which
    tests pin it how.
- **§4 Extension** says "a successful RPC creates a conservative local deadline
  from the monotonic request-start instant". That reads as if every deadline
  came from an RPC. A fresh delivery's first deadline comes from a credit send,
  as the same paragraph's start rule says. This is wording, not behaviour.
  - Phase 3c: wording fixed in semantics.md §4.
- **3c's cancel scenario must cancel after the start.** A row is `LEASED`
  before any worker has its delivery. A client that cancels as soon as it sees
  `LEASED` can beat the start: on a stale delivery the ownership check is
  refused `FAILED_PRECONDITION` and the handler never runs. Task 20's first
  cancel run did exactly that, 15.7 ms after the lease. It is correct, and the
  same in Go, but it is a third outcome beside "stopped at its first renewal"
  and "its Ack refused". A scenario that means the running case has to wait
  for the start; one that accepts any of the three has to say so.
  - Phase 3c: scenarios 5 and 6 cancel after `job started` and scenario 7 at
    lease time, accepting the third outcome by name. On 2026-11-24 the running
    and short cases ended as designed on both SDKs; at lease time Go stopped the
    job at its first renewal and Java never started it. Both are accepted.
- **Accounting doesn't need the Ack's log line.** `Reconcile` reads
  `job confirmed` only to explain a lease that repeated. A completed job's
  final Ack is proven by its row, completed under that lease, so a log missing
  that line still passes. That holds for the Go worker too. A check of the log
  contract alone can't see a missing ack line; the rows check the Ack itself.
  - Phase 3c: `ContractProblems` checks the log apart from the rows in every
    scenario. Negative control 1 (a completed job's ack line removed) fails on
    both SDKs.
