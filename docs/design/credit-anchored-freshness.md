# Credit-anchored freshness: starting a delivery without asking the broker

Status: designed 2026-11-23, before any code (ground rule 1). It replaces the
ownership Heartbeat the Go worker SDK sent before every handler (H4 in
docs/scaling.md). Read this before `session` in worker/stream.go,
worker/freshness.go, or the lease loop in internal/broker/lease.go. Results go
in docs/semantics.md §15.

## Problem

Before every handler the SDK sent one Heartbeat to prove it still held the
lease. A delivery message can't prove that by itself: `leased_at` and
`lease_expires` give the grant G, but nothing in the message says how long the
row sat in buffers before it arrived. Comparing `leased_at` with the worker's
clock would answer that only if the two clocks agreed, and nothing makes them
agree. In a drain the check is 27.7% of statement time at one broker and 49.6%
at four; skipping it unconditionally raised one-broker throughput 1.36× and
cut commits per job from 0.44 to 0.28 (0.93 to 0.52 at four). Skipping it
unconditionally is unsafe: a delivery that waited past its lease would run
beside its next holder.

## The broker invariant

On a lease stream, the broker leases a row only against credit it has already
received on that stream and not yet spent. Before each store call it asks for
at most the credit received so far minus the rows already sent, and it sends no
row beyond what that call asked for. It never leases ahead of credit, never
holds leased rows for credit still to come, and never carries credit from one
stream to another.

Consequence: the k-th delivery on a stream, counting from the first, was leased
by a store call that started after the broker had received k units of that
stream's credit, counting the subscription's credit first.

Where it is enforced: `internal/broker/lease.go`. The call asks for
`min(credit, -max-lease-batch)`; credit falls only after a row is sent; a store
that returns more rows than asked ends the stream `INTERNAL` without sending
any of them. Tests: `TestLeaseStreamNeverExceedsCredit`,
`TestLeaseStreamLeasesOnlyAgainstCreditItHasReceived`,
`TestLeaseStreamSendsNoRowsBeyondWhatItAskedFor`. Any change to the lease path,
including a lease coalescer (H1), must keep this invariant.

## The worker's start rule

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

## Why it is safe

Let unit k be sent at s_k and delivery k be leased by the store call starting
at L.
- By the invariant, the broker had received unit k before L, so s_k ≤ L.
- The lease statement's `now()` is at or after L, so its `lease_expires` is at
  least G after s_k. This assumes the database clock advances at the real rate
  over G, the same assumption `deadlineFrom` makes for every renewal.
- So s_k + G − margin is at least the margin before the reaper may take the
  row, whatever the delivery's age.

Matching oldest first is conservative whatever order the broker spends credit
in. Units are fungible and an earlier unit is never younger, so a broker that
leases fewer rows than it could, or loses one, only makes the bound larger.

The skip's deadline is never later than the check's: the check bases its own on
its Heartbeat's send, which comes after s_k.

## The threshold: G/10

The deadline above is safe for any age. The threshold decides only whether
skipping is worth it. The check's Heartbeat extends the lease to a fresh G, and
a delivery matched to old credit, such as an idle stream's, would otherwise
start with little or no lease left.

At r − s < G/10, the deadline is at least 0.8 G after receipt (margin ≤ G/10).
Renewals run at G/3 and 2G/3 after start, so both fall before the deadline and
docs/semantics.md §11.3's "a fast failed renewal costs one interval" still
holds. At G = 30 s the threshold is 3 s; drain deliveries arrive within
milliseconds of their credit.

Busy streams, where the check cost the most, skip it. Idle streams, whose
credit waits for work, still check, and there the check is cheap.

## What a skip gives up

- **Learning before the handler starts** that the job was cancelled
  (docs/semantics.md §10), or, under asynchronous replication only, that a
  failover lost the lease (§11 item 6). Both are learned at the first renewal,
  G/3 after start, or at the `Ack` or `Nack`, which §10 already allows. Under
  synchronous replication no lease is answered before a standby has it (§11
  item 7). No other path ends a lease before `lease_expires`: only the holder's
  `Ack`/`Nack`, `Cancel` and the reaper write a leased row.
- **The fresh grant a check would have bought.** The deadline starts at least
  0.8 G after receipt instead of about G − margin after the check.

## When the broker breaks the invariant

- **More deliveries than units reserved.** The worker detects this and ends
  the stream (`errOverDelivery`), as before.
- **Leasing ahead of credit, or a lease whose `now()` predates the credit.**
  For example a long-lived transaction, or a coalescer that leases ahead of
  demand. The worker can't detect this. If the lease is older than s_k by more
  than the margin, a handler can run past its lease and beside the next holder.
  That is why the invariant is a tested broker contract, stated in
  docs/semantics.md §4, not an SDK detail.

## Wire protocol

No change. Credit and deliveries share one ordered gRPC stream, so a
delivery's position matches it to its unit. The worker knows its own send
instants, and each delivery already carries G.
- A credit sequence number, or a broker echo of the unit it spent, would add
  nothing: oldest-first matching is already conservative.
- A broker timestamp on the delivery would be a clock comparison.

## For the Java SDK (Phase 3b)

- Take `System.nanoTime()` immediately before `onNext` of each subscription or
  credit message, under the same lock as the credit count, and append it with
  its unit count.
- Match in the inbound `onNext`, oldest first, before handing the delivery to
  its virtual thread.
- Reset on every new call.
- Unsent reservations carry no instant.
- The threshold, the deadline and the fallback are exactly those above.

## Rejected

- **Skipping when the delivery arrives within a fraction of the grant**,
  measured from `leased_at`. It compares the worker's clock with the
  database's.
- **Skipping unconditionally**: the benchmark switch. Receipt is no evidence
  about the lease.
- **A protocol field** (sequence numbers, broker timestamps): see above.
