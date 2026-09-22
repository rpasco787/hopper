# hopper

Hopper is an at-least-once PostgreSQL job queue with a gRPC broker, a Go worker
SDK, and bounded streaming flow control. PostgreSQL owns durable state and lease
transitions; stateless brokers can be replicated without moving job ownership
into process memory.

The current Session 5 build includes the broker, producer client, worker SDK,
example worker, bulk enqueue CLI, and a minimal local Compose stack. Scheduling,
metrics, image publishing, Kubernetes/Helm, KEDA, chaos automation, and load
testing remain later-session work.

## Quick start

Requirements: Docker with Compose v2. The broker is published only on
`127.0.0.1:9090`; PostgreSQL is not published to the host.

```bash
docker compose -p hopper-session5 up -d --build --scale worker=3

go run ./cmd/hopperctl enqueue \
  -queue sleep \
  -payload '{"ms":50}' \
  -n 1000 \
  -batch 100 \
  -key-prefix quickstart

docker compose -p hopper-session5 logs -f broker worker
docker compose -p hopper-session5 down -v
```

The example worker registers three queues:

- `sleep` and `cpu_burn` accept `{"ms":100}`; omitted `ms` defaults to 100.
- `fail_n_times` accepts `{"fail":2}` and fails while `Job.Attempt <= fail`.

## Worker SDK

Register handlers during setup, then call `Run` once. Handler contexts survive
the first shutdown signal so accepted work can drain, but are cancelled on
definitive lease loss, job cancellation, local lease expiry, or the forced
shutdown deadline.

```go
package main

import (
    "context"
    "errors"
    "log"
    "os/signal"
    "syscall"
    "time"

    "github.com/rpasco787/hopper/worker"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer stop()

    w := worker.New("127.0.0.1:9090", worker.Options{
        ID:              "email-worker-1",
        Concurrency:     8,
        ShutdownTimeout: 30 * time.Second,
    })
    w.Handle("send_email", func(ctx context.Context, job worker.Job) error {
        var payload struct {
            To string `json:"to"`
        }
        if err := job.Unmarshal(&payload); err != nil {
            return err
        }
        // Make the external effect idempotent with job.ID (or a stable business
        // ID), and stop promptly when ctx is cancelled.
        return sendEmailOnce(ctx, job.ID.String(), payload.To)
    })

    if err := w.Run(ctx); err != nil {
        if errors.Is(err, worker.ErrAbandoned) {
            log.Printf("shutdown abandoned work: %v", err)
        }
        log.Fatal(err)
    }
}
```

Each valid delivery spends one initial Heartbeat RPC to prove that its lease is
still owned before user code starts. The SDK then renews at one third of the
broker-granted duration, and every Heartbeat pins that original grant rather
than consulting a possibly changed broker default. A capacity token remains
occupied through validation, handler execution, and Ack/Nack confirmation.

## Bulk enqueue CLI

`hopperctl enqueue` validates the entire command before dialing and sends one
bounded batch at a time:

| Flag | Default | Meaning |
|---|---:|---|
| `-broker` | `127.0.0.1:9090` | Broker address; `HOPPER_BROKER` is used when the flag is absent. |
| `-queue` | required | Destination queue. |
| `-payload` | `{}` | JSON payload shared by every generated job. |
| `-n` | `1` | Total jobs to enqueue. |
| `-batch` | `100` | Jobs per RPC, from 1 through 1,000. |
| `-priority` | `0` | Signed 16-bit priority. |
| `-max-attempts` | `0` | Retry limit; zero selects the broker default. |
| `-key-prefix` | empty | Stable keys `<prefix>-<index>`. |
| `-timeout` | `1m` | One deadline for the whole command. |

An RPC failure can be ambiguous: writes may have committed even when no response
arrived, including `RESOURCE_EXHAUSTED` on an oversized response. Do not blindly
resend an unkeyed batch. With a stable prefix, rerun the same command and the
same indexed keys deduplicate while the original rows still exist; use a smaller
batch for future attempts if message size was the problem.

## Shutdown and failure boundaries

On `SIGTERM` or `SIGINT`, `cmd/worker` stops accepting work and uses one
configured deadline for stream teardown, handlers, and confirmations. It exits
zero only after a clean drain. If the deadline passes with active delivery work,
it logs separate delivery/handler/acquisition/confirmation counts, cancels SDK
work, and exits nonzero; unconfirmed leases follow the normal reaper and retry
policy. A Go context cannot forcibly stop handler code that ignores cancellation.

Confirmation retries are deliberately narrow. The SDK retries only transient
`UNAVAILABLE` and `DEADLINE_EXCEEDED` failures while its conservative local
lease deadline remains live. Generic `INTERNAL` errors are not retried blindly:
a database mutation may already have committed, or the failure may be permanent.
This means Hopper tolerates broker restarts and selected transient transport
failures, but Session 5 does not promise transparent recovery from every
PostgreSQL outage. Handlers must remain idempotent because side effects can
duplicate across the Ack-loss window.

For the complete ownership, retry, cancellation, and trust model, see
[docs/semantics.md](docs/semantics.md).

## Development

```bash
gofmt -l .
go vet ./...
buf lint
go test -race -count=2 ./...
docker compose -p hopper-session5 config
```

The integration suite starts PostgreSQL with Testcontainers and therefore needs
access to a running Docker daemon.
