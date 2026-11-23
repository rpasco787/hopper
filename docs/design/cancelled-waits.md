# Answers a cancelled synchronous wait can't take back

Status: designed 2026-11-22, before any code (ground rule 1); measured 2026-11-23 (docs/semantics.md §15). Read this before
`Enqueue`'s `dedupSQL`, `classifyMiss` in `internal/store/postgres/ack.go`, or
`Cancel`'s classify. The evidence is in `docs/failover/summary.md` and
`docs/failover/post-promotion-waits.txt`. Results go in `docs/semantics.md` §15.

## Problem

PostgreSQL commits, then waits for the synchronous standby; the transaction
stays invisible while it waits. A cancelled wait keeps the commit and makes it
visible on the primary alone, with only a warning. pgx cancels a statement
whose context ends (`asyncClose` sends a `CancelRequest`), and the batcher
ends a statement whose last caller left. So a write that outlived its caller's
deadline leaves a row no standby has. The caller's retry finds it, and four
answers come from a plain read that waits for nothing: `deduplicated`, and
"already applied" for `Ack`, `Nack` and `Cancel`. If the primary is then lost,
the acknowledged job is gone, or the acked job runs again. On kind: 14 of
19,478 acknowledged jobs in 3 crashes with replication paused 3 s, and 0–43
cancelled waits after every plain promotion, while the new primary had no
synchronous standby.

## Invariant

An answer that reports work as done (inserted, deduplicated, acked, nacked,
cancelled) is given only after a transaction that read or wrote that work has
committed, and that commit waited for a synchronous standby wherever the
server requires one. That is enough because WAL is one ordered stream: a row
a transaction can see has its commit record in WAL before the reader's own,
and a standby that flushed the reader's commit flushed the row's.

## Mechanism

PostgreSQL waits for a standby at commit only when the transaction has a
transaction ID and wrote WAL (`RecordTransactionCommit`). A plain read has
neither, and neither has an insert that deduplicated against a committed row.
`pg_current_xact_id()` assigns an ID but writes no WAL, so its commit doesn't
wait. A row lock does both: `heap_lock_tuple` takes an ID and logs the lock.
`TestWhatASynchronousCommitWaitsFor` pins all of this.

So the read behind a "done" answer takes `FOR KEY SHARE` in its own implicit
transaction:

- `Enqueue`'s dedup lookup always does (it runs only on the dedup path).
- `classifyMiss` (`Ack`, `Nack`) reads plainly, and only when it finds the
  already-applied case reads again under the lock and answers from that.
- `Cancel`'s classify does the same when it finds `cancelled`.
- `ExtendLease` has no already-applied case and doesn't change. Refusals never
  wait: they promise nothing a crash could take back.

`FOR KEY SHARE` is the weakest row lock. Ack's and Heartbeat's updates never
wait for it; Lease and AckMany (`FOR UPDATE SKIP LOCKED`) skip the row for one
commit; Nack's `FOR UPDATE` and retention's `DELETE` wait for one commit.

## Failures

- A standby behind longer than the caller's deadline: the lock's commit is
  cancelled like any write's, and the caller gets `DEADLINE_EXCEEDED`, not an
  answer. The cancelled commit is a lock only.
- The primary crashes mid-wait: `UNAVAILABLE`; nothing was promised.
- After a promotion, before a standby streams: the retry waits for the new
  primary's first synchronous standby, which receives everything from it.
- No replication (Compose, chaos): one local WAL flush on a rare path.
- Asynchronous replication: nothing to wait for (§11 item 6).

## Rejected

- `pg_current_xact_id()` in the same transaction: its commit doesn't wait.
- Never cancel a commit (statements detached from their callers): a timed-out
  write holds its connection until a standby returns, so a standby outage
  fills the pool and stops reads too; the batchers' cancel-on-last-caller and
  10 s cap would go, making a held flight unbounded; the deadline tests would
  invert unless every write took an explicit transaction and one more round
  trip; and cancels hopper doesn't send (`pg_cancel_backend`,
  `pg_terminate_backend`, a pooler) would still open the window.
- `on conflict do update` as the dedup: a row version per retry, a lock that
  conflicts with the holder's Ack and Heartbeat, and Enqueue only.
- Polling a standby's `flush_lsn`: needs `pg_read_all_stats`, and redoes what
  the lock's commit does.

## What it leaves

- hopper still cancels the waits of callers that leave, so rows still become
  visible on the primary alone while a standby lags (seconds after a
  promotion). Nobody is told they succeeded.
- Reads (`GetJob`, `ListJobs`, depth) can show such a row, and refusals can
  rest on one; a crash can take either back. That is a repeat, not a loss.
- No worker is handed such a row: Lease is a write, and its commit waits.

## Cost

One commit that waits for a standby (a standby round trip and its flush) on
the dedup, already-applied and already-cancelled paths; Ack, Nack and Cancel
add one statement there. `EnqueueBatch` pays it per deduplicated item. The hot
paths (a fresh Enqueue, AckMany, ExtendLeaseMany, Lease) don't change.
