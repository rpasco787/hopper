# The conformance suite (Phase 3c)

Status: designed 2026-11-24, before the suite's code. It holds the Go worker SDK
(`worker/`, run as `cmd/worker`) and the Java worker SDK (`java/hopper-worker`,
run as `java/hopper-example-worker`) to one delivery contract,
docs/semantics.md, with one set of scenarios both must pass. Results go in
docs/semantics.md §15.

## What it proves, and what it can't

Each scenario drives or breaks a running Compose stack the way one section of
docs/semantics.md describes, then judges the database's rows and every
worker's output. A worker passes when both say what that section promises.
The suite sees only what a client can: rows, log lines and exit codes. A
capacity token, credit still outstanding, or a renewal that succeeded leaves
no line, so the checks are bounds on what the log shows.

## Where it lives

`test/chaos`, package chaos, behind the build tag `conformance`. The chaos
suite's stack helpers (stack_test.go) compile under `chaos || conformance`.
The chaos tests stay `chaos` only and the scenarios are `conformance` only, so
neither run compiles the other's tests. The judges are pure functions in
test/accounting.

- Rejected: extracting stack_test.go into a non-test package. It would move
  600 proven lines and change every chaos test's calls, for nothing the
  scenarios need.
- Rejected: an independent helper in test/conformance. It would have to
  re-learn what stack_test.go already handles: bounded external calls,
  evidence saved on failure, and cleanup inside the test binary's deadline.

## The worker under test

`HOPPER_CONFORMANCE_WORKER` names a worker image.
- Empty runs the Go worker built from the working tree, as the chaos suite
  always has.
- Anything else is an image that already exists. test/chaos/worker-image.yaml
  runs it as the `worker` service, never builds it, limits its memory, and
  adds `worker-go`, the Go worker built from the tree, for the mixed fleet.
- The same variable runs the chaos suite against that image, and
  `HOPPER_CONFORMANCE_WORKER=java` makes scripts/kind-up.sh install the Java
  image as the chart's worker.

A worker counts as ready when a broker logs `lease stream opened` for its
worker id, after it accepted the subscription. The worker's own line of that
name is not enough: the Java SDK writes it when it hands the subscription to
grpc-java, which can be before any connection exists.

Every lease is 30 s, Compose's default. The PostgreSQL scenario needs the
renewal interval (G/3 = 10 s) to outlast a ~3 s outage. The cancel and expiry
timings then match §11.3's table.

## Scenarios

| # | Test (subtest) | Contract | Stack | Passes when, for both SDKs |
|---|---|---|---|---|
| 1 | `TestConformanceQuietRun/Burst` | §1 | quiet, 3 workers | 1,000 × 100 ms `sleep` jobs: every job completes; no repeat; no job leased twice; every worker ran some |
| 2 | `…/ConcurrencyBound` | §4 | quiet | on the burst's logs, no process ever has more deliveries open than its `worker starting` concurrency, and some process reaches it |
| 3 | `…/FailBelowMaxAttempts` | §6 | quiet | 20 × `fail_n_times {"fail":2}`, max 5: completed, attempts 3, 3 starts, repeats `nacked`, `last_error` `intentional failure on attempt 2 of 2` |
| 4 | `…/FailAtMaxAttempts` | §7 | quiet | 20 × `{"fail":3}`, max 3: `dead`, attempts 3, 3 starts, the last lease ends on a nack confirmation, `last_error` `intentional failure on attempt 3 of 3` |
| 5 | `…/CancelWhileRunning` | §10 | quiet | a 20 s job cancelled after `job started`: stopped at its first renewal, [G/3 − 1 s, G/3 + 2 s] after the start, left unconfirmed; `cancelled`, attempts 1 |
| 6 | `…/CancelAfterAShortHandler` | §10 | quiet | a 3 s job cancelled after `job started`: its Ack refused; `cancelled`, attempts 1 |
| 7 | `…/CancelAtLeaseTime` | §10 | quiet | a 20 s job cancelled the moment its row is `leased`: never started (check refused) or stopped at its first renewal; which one happened is reported; `cancelled`, attempts 1 |
| 8 | `TestConformanceKilledWorker` | §11.1 | 3 workers | SIGKILL the busiest worker mid-run, start it 2 s later: every job completes; every repeat `killed`, ≥ 25 s after its first start; one process cut, with a handler unfinished |
| 9 | `TestConformanceBrokerGoneLongerThanTheLease` | §11.2 | 3 workers, 3 gated 45 s jobs | SIGKILL `broker` for 45 s: each gated handler stopped at its local deadline, 18.5-30.5 s after the kill; `handler finished` within 2 s; left unconfirmed; each job completes later; repeats `lost` |
| 10 | `TestConformanceShortPostgresOutage` | §11.3 | 3 workers, 6 gated 3 s jobs | graceful stop and start of PostgreSQL, new sessions failing < 10 s: no re-execution; every gated job completes under its first lease |
| 11 | `TestConformanceSigtermDrainsCleanly` | §11.4 | 3 workers | `docker stop` of the busiest worker: exit 0; `shutdown started` with running handlers; `worker stopped`; stderr empty; every job once; every delivery open at the signal ends on a confirmation |
| 12 | `TestConformanceShutdownDeadlineExceeded` | §11.4 | 1 worker, 2 s budget, 2 × 20 s jobs | exit 1; one `shutdown deadline reached` naming 2, 2, 0, 0; stderr exactly `worker: worker: shutdown deadline reached with 2 active deliveries (2 running handlers, 0 acquiring a lease, 0 awaiting confirmation)`; both jobs complete later; repeats `lost` or `killed` |
| 13 | `TestConformanceMixedFleet/Burst`, `/Kill` | all | 2 Java + 2 Go | rows 1 and 8, with both SDKs' logs judged together and one kill per SDK (Java pass only) |

Every scenario also requires, of every worker in it:
- stdout is JSON log lines only, and stderr is empty, except where a scenario
  names the one line it expects;
- the log contract, below;
- the concurrency bound, below.

## How a run is judged

**The Ack, and its log line, separately.** A completed job's Ack is proven by
its row: completed, under a lease some captured process started
(test/accounting's Reconcile). Reconcile reads `job confirmed` only to explain a
lease that ran again, so a worker that never logged its Ack would pass it.
ContractProblems checks the log on its own terms. Per worker process, per
delivery (one job under one lease), the structural lines are one of:
- started: `job started`, then `handler finished`, then one terminal line;
- unstarted: `nacking an unstarted job`, then one terminal line, a `job
  confirmed` nack;
- unchecked: `lease not confirmed; leaving the job to be redelivered`, alone.

A process that was cut (killed, or abandoned and restarted) may stop a
delivery part way through. The terminal lines are:
- `job confirmed`;
- `job confirmation refused; someone else may have it now`;
- the two `job left unconfirmed; …` lines;
- `handler returned after the shutdown deadline; not confirming`.

Then, per row:
- a completed job's final lease ends on an ack `job confirmed`, unless its
  process was cut before any terminal line;
- a dead job's final lease does not end on an ack;
- a cancelled job is never confirmed.

Every line about a run job carries `lease_id` and `queue`. Every process that
ran a delivery logged `worker starting` with its `concurrency` first.

**Concurrency.** A delivery is open from its first `job started` or `nacking
an unstarted job` until its terminal line. Open deliveries are counted per
process, in the order the process wrote its lines. A killed process's
deliveries stay open until its log ends. That interval lies inside the time
the delivery's capacity token is held: the token left the pool with its
credit, and it returns only after the terminal line is written (both SDKs). So
more open than the `worker starting` concurrency proves a violation. No more
proves the bound only for what the log shows: not for credit outstanding, and
not for an ownership check in progress.

**Cancellation.** A job cancelled while leased has three correct outcomes:
1. Stopped at its first renewal: `heartbeat rejected; cancelling the handler`
   between G/3 − 1 s and G/3 + 2 s after `job started`, then `handler
   finished`, then `job left unconfirmed; its lease ran out and it will be
   redelivered`.
2. Its Ack refused: `job confirmation refused; someone else may have it now`
   with operation `ack`.
3. Never started: the ownership check a stale delivery takes was refused,
   `lease not confirmed; leaving the job to be redelivered`, with no `job
   started`.

None of them confirms the job, and none leases it again. A scenario that
cancels after `job started` requires 1 or 2; one that cancels at lease time
accepts 1 or 3.

**Local expiry.** A handler whose renewals can't get through is stopped at its
local deadline: its last successful renewal's send (or its start's evidence)
plus G less the margin. That send was at most G/3 before the outage began, so
`lease expired locally; cancelling the handler` comes between G − margin − G/3
− 1 s and G − margin + 1 s after it (18.5-30.5 s at G = 30 s). `handler
finished` follows within 2 s, and the lease is left unconfirmed.

**The deadline.** A drain that outlives its budget logs one `shutdown deadline
reached` naming `active_deliveries`, `running_handlers`, `acquiring` and
`confirming`. It prints `worker: worker: shutdown deadline reached with …` on
stderr, the same in both SDKs, and exits 1.

**Clocks.** Times compared with a log line's time come from the Docker VM's
clock: another log line, or `docker inspect`'s FinishedAt. The host's clock is
never compared with one.

**Expect and Evidence.** A scenario's expectations are data,
`accounting.Expect`, saved beside its evidence: `rows.csv`, each worker's
`.stdout` and `.stderr`, `exits.json`. `accounting.Judge` judges the evidence
live, and `go test -tags evidence` judges a saved copy again offline. That is
how each check is shown able to fail: damage a copy, judge it.

## Running it

```bash
scripts/conformance.sh all   # the Go pass, then the Java pass with the mixed fleet
scripts/conformance.sh java  # builds hopper-example-worker:dev first
HOPPER_CONFORMANCE_WORKER=hopper-example-worker:dev go test -tags chaos -count=1 -timeout 60m -v ./test/chaos
```

## CI

.github/workflows/ci.yml has three jobs: Go checks and the race suite, the Java
build and the stub check, and both conformance passes. Each step is one call of
scripts/ci.sh, which runs the same on a laptop. Every action is pinned by
commit.

## Rejected

- **A 10 s lease** to save time: it shrinks §11.3's window below a PostgreSQL
  restart.
- **Forbidding redeliveries after SIGTERM or a PostgreSQL stop:** §11 allows
  unstarted nacks and never-received leases there. The scenarios forbid
  re-execution, and require each running or gated delivery to complete under
  its own lease.
- **Judging the deadline from stderr alone:** the log line must carry the counts
  too; the Go line was fixed to.
- **JAVA_TOOL_OPTIONS** to tune the JVM: the JVM announces it on stderr.
- **Running the k8s proofs in this phase:** the laptop's kind node stays
  stopped; scripts/kind-up.sh is parameterized for a machine that can.
