# The Java producer client (Phase 3a)

Status: designed 2026-11-23, before any code. A short note, because the client
holds no delivery state. Like client/client.go, it is a thin wrapper over the
generated stub that keeps proto types, base64 and raw status codes out of
calling code.

## Surface

```java
public final class HopperClient implements AutoCloseable {   // thread-safe
    public static HopperClient connect(String target);       // owns its channel
    public static Builder builder(String target);            // credentials(…), callTimeout(…), build()
    public static HopperClient wrap(Channel channel);         // caller-owned; close() leaves it open
    public static HopperClient wrap(Channel channel, Duration callTimeout);
    public JobResult enqueue(NewJob job);                     // each call also takes a trailing Duration timeout
    public List<JobResult> enqueueBatch(List<NewJob> jobs);
    public CancelResult cancel(UUID id);
    public JobResult getJob(UUID id);
    public void close();
}
NewJob.builder(queue).payloadJson(String | byte[]).priority(int).runAt(Instant)
      .maxAttempts(int).idempotencyKey(String).build()
record JobResult(UUID id, JobState state, boolean deduplicated)
record CancelResult(UUID id, JobState state, boolean alreadyCancelled)
enum JobState { SCHEDULED, AVAILABLE, LEASED, COMPLETED, CANCELLED, DEAD }
```

- **Payload.** Raw JSON, sent as is. The broker checks its syntax. A null
  payload is sent empty, and the store keeps `{}`.
- **Local checks.** `queue` is required. `priority` must fit 16 bits, as
  Go's `int16` field does. `maxAttempts` must be at least 0, and 0 takes the
  broker's default. A null batch entry is rejected with its index, before any
  network call.

## Calls are blocking, and every one has a deadline

The calls are blocking, and meant for virtual threads. Each one runs under
the builder's `callTimeout` (default 30 s) or its own `Duration` argument.
Java has no `context.Context` to carry a deadline. A blocking call with none
would wait on a stuck broker until keepalive gave up. A caller that
interrupts a thread blocked in a call gets a `HopperException` with code
`CANCELLED`, and the thread's interrupt flag stays set.

## Errors

| gRPC code | Exception | `outcomeUnknown()` for enqueue, enqueueBatch, cancel |
|---|---|---|
| `INVALID_ARGUMENT` | `InvalidRequestException` | false: broker validation writes nothing (docs/semantics.md §8) |
| `NOT_FOUND` | `JobNotFoundException` | false |
| `FAILED_PRECONDITION` | `JobTerminalException` | false |
| `RESOURCE_EXHAUSTED` | `ResourceExhaustedException` | **true**: a response over 4 MiB can follow committed writes (§8) |
| `UNAVAILABLE` | `BrokerUnavailableException` | **true**: the call may have taken effect (§5) |
| `DEADLINE_EXCEEDED` | `DeadlineExceededException` | **true** |
| `UNIMPLEMENTED`, `UNAUTHENTICATED`, `PERMISSION_DENIED` | `HopperException` | false |
| anything else (`INTERNAL`, `CANCELLED`, `UNKNOWN`, …) | `HopperException` | **true** |

`getJob` is a read, so `outcomeUnknown()` is always false for it. Every
exception keeps the original `StatusRuntimeException` as its cause and
exposes `code()`.

## Retries

The client retries nothing (§8: "The producer client does not retry for
you"). The caller decides, as with the Go client:
- **An `enqueue` with an idempotency key** is safe to retry whenever
  `outcomeUnknown()` is true. Without a key it is not.
- **`enqueueBatch` is not atomic.** Retry a failed batch whole, with the same
  stable per-item keys. If message size was the problem, retry it smaller.
  Never resend an unkeyed batch blindly: rows may already be committed,
  `RESOURCE_EXHAUSTED` included (README, "Retrying a failed enqueue").
- **`cancel`** is idempotent at the broker: a repeat reports
  `alreadyCancelled`.

grpc-java's transparent retry of an RPC that never left the client stays on,
as grpc-go's does. No service config retry policy is set.

## Channel

`ChannelDefaults` (in hopper-proto, shared with the worker so the two can't
drift) sets the same defaults as client/client.go and worker/worker.go:
- `round_robin` over every address the target resolves to;
- keepalive pings every 10 s, with a 5 s timeout, without calls too (the
  broker permits pings every 5 s);
- 4 MiB inbound messages;
- plaintext unless credentials are given. A caller's credentials replace the
  plaintext default.

Targets use grpc-java's syntax. `host:port` resolves through DNS, as Go's
default does, so `brokers:9090` in Compose and the chart's headless Service
reach every broker.

## Not in 3a

`ListJobs`, `Redrive` and `ListDead`: the Go client doesn't wrap them either.
