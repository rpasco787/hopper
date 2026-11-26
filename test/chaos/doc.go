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
//
// HOPPER_CHAOS_SHARDS=2 runs every stack over two PostgreSQL shards:
// shards.yaml adds postgres-1 and gives both brokers HOPPER_SHARD_DSNS in
// shard order (and an empty HOPPER_DSN, since a broker refuses both). Unset or
// 1 is one database, as before; no other value is allowed. The suite reads
// every shard when it counts and reconciles a run, while the tests that stop
// "PostgreSQL" stop shard 0 only, and the probe watches shard 0. A test can
// pass HOPPER_CHAOS_SHARDS=2 to up itself to run sharded whatever the suite's
// setting, as the one-shard-down test does.
//
// HOPPER_CONFORMANCE_WORKER runs every test against a named worker image
// instead of the Go worker built from the tree (scripts/conformance.sh java
// builds the Java one); worker-image.yaml is merged over the stack for it. The
// build tag conformance selects the conformance scenarios instead of the chaos
// tests (docs/design/conformance.md).
package chaos
