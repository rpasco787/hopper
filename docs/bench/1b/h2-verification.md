# H2 confirmation batching — tasks 6–8 verification

Date: 2026-11-16 (America/Chicago). Tasks 0–5 were accepted as complete at the user's instruction; their completion was not audited. The full regression command below is required by task 8.

Batching remains off by default (`confirm-batch=1`, `confirm-flights=2`). This records correctness checks, not performance measurements.

| Check | Result |
|---|---|
| Task 6 new tests before implementation | Expected missing `Config.ConfirmBatch` / `ConfirmFlights` compilation failure |
| `go test -race ./internal/broker -count=2` | PASS |
| Task 7 Config tests before implementation | Expected missing confirmation config fields compilation failure |
| `go test -race ./cmd/broker -count=1` | PASS |
| `HOPPER_TEST_CONFIRM_BATCH=64 go test -count=1 ./test/integration` before wrapper forwarding | Expected failure identifying all six wrappers hiding `BatchConfirmer`; no other failing tests |
| `go test -race -count=1 ./test/integration` | PASS |
| `HOPPER_TEST_CONFIRM_BATCH=64 go test -race -count=1 ./test/integration` | PASS |
| `HOPPER_TEST_CONFIRM_BATCH=64 go test -race -count=3 -run 'TestHeartbeatRidesOutADatabaseOutage\|TestWorkerLeaseLossCancelsHandler\|TestWorkerInitialValidationRejectsDelayedStaleDelivery\|TestWorkerConfirmationAmbiguityReusesLeaseToken' ./test/integration` | PASS (all four tests, three repetitions) |
| `gofmt -l .` and `go vet ./...` | PASS; no formatting or vet output |
| `go test -race -count=2 ./...` | PASS |
| `HOPPER_TEST_CONFIRM_BATCH=64 go test -race -count=2 ./test/integration` | PASS |
| Restart constructor switch regression | RED: zero batch calls; GREEN after sharing `confirmationConfig` across both constructors |
| Cancellation mutation: remove last-caller `cancel()` via a Go overlay | Expected FAIL: abandoned batch statement remained in PostgreSQL's lock queue |
| `go test -race -count=3 -run 'TestClientDeadlineCancels' ./test/integration` after removing overlay | PASS |
| Chaos Compose configuration with switch `64`, and default/off `1` | Both broker services receive the requested value |
| Three requested chaos tests with `HOPPER_CHAOS_CONFIRM_BATCH=64` | PASS: control, broker graceful/crash restart, PostgreSQL graceful/crash restart; zero repeated handler starts in every case |
| Batching-on outage tests, five repetitions (`TestAckRidesOutADatabaseOutage`, `TestHeartbeatRidesOutADatabaseOutage`, `TestMetricsMatchTheDatabase`, `TestStaleAckOverGRPC`, `TestCancelledLeasedJobTellsTheWorkerToStop`) | PASS |
| Saved chaos broker logs | All 10 logs contain `confirmation batching on` |

Chaos command:

```bash
HOPPER_CHAOS_DIR=/tmp/hopper-chaos-h2 HOPPER_CHAOS_CONFIRM_BATCH=64 \
  go test -tags chaos -count=1 -timeout 40m -v \
  -run 'TestUndisturbedRunIsClean|TestBrokerRestartLosesNothing|TestPostgresRestartLosesNoAck' ./test/chaos
```

Evidence: `/tmp/hopper-chaos-h2/`; command output: `/tmp/hopper-task8-chaos.log`.

The initial sandboxed broker/CLI runs could not bind loopback listeners; reruns with local-network permissions passed. No test assertions or fault injections were weakened.

Plan adjustment: the plan named only `startBrokerWithConfig`, but restart tests use `serveLiveBroker`. Both now share the batching switch and refusal check. This changes test configuration only; the regression test caught the bypass before the fix.

Independent review found no Critical or Important issues. Deferred minor: `batchingOn()` treats any nonempty test switch as enabled for counter assertions, so explicit `HOPPER_TEST_CONFIRM_BATCH=1` trips them; the prescribed unset/64 runs and production behavior are unaffected.

Code commits: `1b87cff` (task 6), `a838706` (task 7), `e01a606` and `85471a2` (task 8). This note remains uncommitted for user review.

Final outage command:

```bash
HOPPER_TEST_CONFIRM_BATCH=64 go test -race -count=5 \
  -run 'TestAckRidesOutADatabaseOutage|TestHeartbeatRidesOutADatabaseOutage|TestMetricsMatchTheDatabase|TestStaleAckOverGRPC|TestCancelledLeasedJobTellsTheWorkerToStop' ./test/integration
```

All required task 8 checks completed successfully. No runs were skipped and no correctness tests were weakened. Tasks 9–11 were not started.
