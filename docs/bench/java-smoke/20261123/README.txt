Java worker against the Compose stack, 2026-11-23 (UTC 2026-11-24 06:49-06:55), on AC power.
Plan: docs/plans/2026-11-23-phase-3a-3b.md, Task 20. Script: scripts/java-smoke.sh.

Refs: undisturbed, sigterm, the first cancel run and abandon ran at e39bd1c (plus
test/accounting/javalog_test.go, then uncommitted); the cancel rerun at 3461424.
Image hopper-example-worker:dev sha256:e1c7bb7571a5, built from e39bd1c's java/.
Compose project hopperjava: postgres + one broker (HOPPER_LEASE_DURATION=30s, cron off);
worker concurrency 4, HOPPER_SHUTDOWN_TIMEOUT=45s (abandon: 2s).

undisturbed  PASS  jobs 240, completed 240, processes 1, redelivered 20, restarted 20,
                   repeats 20 (the fail_n_times nacks), shutdowns []
sigterm      exit=0, PASS graceful  jobs 400, completed 400, processes 2, redelivered 0,
                   shutdown started running_handlers 4, worker stopped
cancel1      FAIL  the client cancelled 15.7 ms after the lease, before the worker's
                   ownership check on its idle stream (64 ms): the check was refused
                   FAILED_PRECONDITION and the handler never started (cancel1-*.log).
                   Correct behaviour, Go's too (worker/execute.go:42-54), but not the
                   running case the check is about: the driver's race, fixed in 3461424
cancel       PASS  stopped 10.04991663s after the handler started (G/3 = 10s); no job
                   confirmed for it
abandon      exit=1, one "shutdown deadline reached" line (active_deliveries 2,
                   running_handlers 2, acquiring 0, confirming 0); stderr:
                   worker: worker: shutdown deadline reached with 2 active deliveries
                   (2 running handlers, 0 acquiring a lease, 0 awaiting confirmation)
                   Not run through accounting: that stderr line is non-JSON by design.

negative-controls.txt: the same check on damaged copies of these logs. A deleted job
started, an inserted non-JSON line and a deleted nack confirmation each fail; a deleted
ack confirmation passes, because accounting proves a completed job's Ack from its row
and reads "job confirmed" only to explain a lease that repeated.
