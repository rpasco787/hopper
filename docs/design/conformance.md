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

## Results (2026-11-24)

Run on one laptop (Docker Desktop VM, 3.83 GiB), on AC power, with no sleep or
wake in any window, and with 0 containers and 0 volumes left behind. Evidence:
docs/bench/conformance/20261124/.

**Both passes, through `scripts/ci.sh conformance`** (15:03:49–15:14:50, at
89b4d21 with the scripts then uncommitted, committed as d485947):

| Pass | Time | Estimated | Aim |
|---|---|---|---|
| Go worker from the tree | 301.9 s | 660 s | 10 min |
| Java worker, `hopper-example-worker:dev` (image 61a44b130a45), with the mixed fleet | 353.6 s | 840 s | 10 min |

Both passed every scenario the first time they ran. The whole script took 11 min
1 s, the Java image build included. The estimates were conservative because
three scenarios wait out the 30 s lease (D11) and the stacks start faster than
guessed.

| # | Scenario | Go | Java |
|---|---|---|---|
| 1-4, 5-7 | `TestConformanceQuietRun`, seven subtests | 49.8 s | 42.1 s |
| 8 | killed worker | 41.9 s; 4 handlers interrupted, 4 repeats, all `killed` | 42.6 s; the same counts |
| 9 | broker gone past the lease | 103.1 s; 3 repeats, all `lost` | 106.2 s; the same |
| 10 | short PostgreSQL outage | 23.7 s; sessions failed 2.96–3.17 s, 0 repeats | 25.8 s; 0 repeats |
| 11 | SIGTERM drain | 19.7 s; exit 0, 0 unstarted nacks, 0 never received | 20.1 s; the same |
| 12 | shutdown deadline | 60.3 s; exit 1, counts 2/2/0/0, 2 `lost` repeats | 59.8 s; the same, stderr identical |
| 13 | mixed fleet, `Burst` and `Kill` | skipped | 55.2 s; 8 handlers interrupted, 4 on each SDK |

The per-scenario peaks are in each `judgement.txt` and the pass logs
(`summary.txt`): in every scenario no process held more deliveries open than the
concurrency its `worker starting` line named, and in the burst every worker
reached it (4 of 4).

**The cancel outcomes.** A job cancelled while its handler ran stopped at its
first renewal, G/3 after the start, on both SDKs. A short job cancelled after
its start had its `Ack` refused on both. A job cancelled the moment its row was
`leased` stopped at its first renewal on Go (`heartbeat rejected; cancelling the
handler`, 10 s after the start) and was never started on Java (`lease not
confirmed; leaving the job to be redelivered`: the ownership check was refused).
Both are accepted outcomes; the third one no Go test ran end to end before.

**Scenario 9's window.** The gated handlers' local expiry fell 27.22, 27.22 and
27.27 s after the kill on Go, and 27.44, 27.44 and 27.23 s on Java, in the
Task 12 runs against a window of 18.5–30.5 s; the handler finished at the same
instant each time. The full passes judged the same window clean; they did not
log the offsets.

**The negative controls** (`negative-controls.txt`). Each of the 16 undamaged
evidence sets judged clean, then 25 damaged copies (20 rows on Java, the 5
daggered rows again on Go) each failed with the line the row names: 25 of 25, 0
survivors. No judge was strengthened and there is no equivalent mutant among
them. The first pass of the driver failed 19 of 20; the twentieth was a driver
bug (it picked a job outside the scenario's rows), fixed in the driver. One
equivalent mutant exists in the unit tests of the concurrency bound (a repeated
first line), kept as written.

**Memory and startup** (`measure.txt`, 3 workers then 8, 3,000 jobs, one worker
killed and restarted 8 s in): the Java worker's peak was 104 MiB of its 384 MiB
limit (27%); the whole stack with eight workers peaked at 845 MiB of 3,919 MiB;
nothing was OOM-killed. D9's 384m stays. A container started to its first log
line in 0.13–0.20 s; a restart under load reached `worker starting` after
0.25 s, its own `lease stream opened` after 0.36 s and the broker's acceptance
after 0.49 s. That makes the chaos kill loop's 5 s between a restart and the
next kill safe. The caveat is nine `docker stats` samples about 6 s apart, so a
spike shorter than that could have been missed.

**`lease stream opened`** (D22): against `127.0.0.1:1` for 3 s the Go worker
logged the line 0 times and the Java worker 5 times. Readiness is the broker's
line (§14 of semantics.md).

**The chaos suite unchanged with the Go worker** (D6): 605.6 s before the image
switch and 600.4 s after, the same 9 tests and 4 subtests passing, the same
render, the same kinds of counts.

**The chaos suite five times against the Java worker** (`chaos-java-5.txt`):
**four of five passed** (628.9, 619.6, 615.6 and 782.9 s). Pass 2 failed
`TestBrokerRestartLosesNothing/crash`: one job's handler started twice. The
`Ack` of its first attempt was in flight when the broker was killed and failed
`UNKNOWN: channel closed`, which grpc-netty returns for a write onto a
connection that has just closed (Go's gRPC reports `UNAVAILABLE`). The Java
worker retried only `UNAVAILABLE` and `DEADLINE_EXCEEDED`, so it took the
failure as a refusal, the lease ran out and the job ran again. It is a Java SDK
bug, fixed in a595053 with `Heartbeat.isNetwork`, shared by confirmations and
renewals (a renewal would have ended its lease on the same status); a unit test
pins it. The five passes ran against the image from before the fix; the image
was not rebuilt and the suite was not rerun against it, so the fix is proven by
its unit test and not yet by a chaos pass. Pass 5 took 782.9 s, about 160 s more
than the others, with no sleep in the window; its cause is unknown.

**Window tests.** Both SDKs' last two window tests now run with the unexported
`checkEveryDelivery` option, because a mutant that let a failed renewal move the
deadline survived the old setup and died with it.

**What did not run.** The first GitHub Actions run: the workflow's toolchain
installs, the Linux branch of `java-gen.sh`, and the runner's Docker, Compose
and Docker Hub pulls can only be seen there. The Kubernetes proof with the Java
worker: `scripts/kind-up.sh` is parameterized for it, and the laptop's kind node
stayed stopped. Raw worker stdout is not in the evidence directory: it would
take 20 MB, and the judges' inputs (`expect.json`, `rows.csv`) and verdicts are.
