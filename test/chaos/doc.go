// Package chaos breaks a running Compose stack on purpose and checks, against
// the database and every worker's log, that no job was lost and that no
// handler ran more often than attempt accounting allows (docs/semantics.md
// §11, §15). The tests are behind the chaos build tag, so `go test ./...`
// never needs Docker Compose. With Docker running:
//
//	go test -tags chaos -count=1 -timeout 60m -v ./test/chaos
//
// Each test brings up its own stack as Compose project hopper-chaos, from
// docker-compose.yml with test/chaos/compose.yaml merged over it, and removes
// it afterwards, volumes included. TestMain builds the images from the working
// tree first, so the stack always runs the code under test. Run one `go test`
// at a time: the project name is fixed.
//
// HOPPER_CHAOS_DIR keeps each test's evidence (every container's log, the
// actions taken, and the reconciliation report) instead of discarding it with
// the test's temp directory. HOPPER_CHAOS_JOBS and HOPPER_CHAOS_JOB_MS size the
// worker-kill run (default 10000 jobs of 100ms), HOPPER_CHAOS_SEED fixes its
// choice of victims, and HOPPER_CHAOS_LONG_OUTAGE sets how long the
// longer-than-the-lease outage lasts (default 40s).
package chaos
