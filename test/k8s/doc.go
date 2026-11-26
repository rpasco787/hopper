// Package k8s holds end-to-end proofs against a kind cluster running the
// hopper chart. The tests are behind the k8s build tag, so `go test ./...`
// never needs a cluster. Run one proof at a time; each needs its release set
// up its own way:
//
//	# The rollout proof needs a fixed worker count. --force-conflicts takes
//	# the count back from the HPA if autoscaling was on.
//	scripts/kind-up.sh --set worker.replicas=3 --force-conflicts
//	go test -tags k8s -count=1 -timeout 20m -v -run TestRolloutRestartDuplicatesNothing ./test/k8s
//
//	# The autoscaling proofs need KEDA and the monitors.
//	scripts/kind-addons.sh
//	scripts/kind-up.sh -f deploy/kind/autoscaling.yaml
//	go test -tags k8s -count=1 -timeout 30m -v -run TestAutoscalingRedeliversNothing ./test/k8s
//	go test -tags k8s -count=1 -timeout 15m -v -run TestUnknownDepthHoldsTheFallback ./test/k8s
//
//	# The failover proofs need the operator and the replicated database.
//	# HOPPER_PROOF_DIR must be absolute: go test runs in test/k8s.
//	scripts/kind-cnpg.sh
//	scripts/kind-up.sh -f deploy/kind/replicated.yaml --timeout 10m
//	go test -tags k8s -count=5 -timeout 90m -v -run TestPrimaryFailoverLosesNothing ./test/k8s
//	scripts/kind-up.sh -f deploy/kind/replicated.yaml -f deploy/kind/asynchronous.yaml --timeout 10m
//	go test -tags k8s -count=3 -timeout 60m -v -run TestAsynchronousFailoverCountsWhatItLoses ./test/k8s
//
//	# The soak needs a release with several workers; on kind, two shards.
//	# With postgres.mode=cloudnativepg (after scripts/kind-cnpg.sh) it also
//	# fails one shard over; in bundled mode it logs that it skipped that.
//	scripts/kind-up.sh -f deploy/kind/shards.yaml
//	HOPPER_SOAK_DURATION=15m HOPPER_SOAK_CHUNK=100000 HOPPER_SOAK_BROKER_RESTART_EVERY=5m \
//	HOPPER_PROOF_DIR=$PWD/docs/sharding/soak-kind \
//	  go test -tags k8s -count=1 -timeout 60m -v -run TestSoak ./test/k8s
//
// HOPPER_K8S_CONTEXT, HOPPER_K8S_NAMESPACE and HOPPER_K8S_RELEASE override
// kind-hopper, hopper and hopper. For the rollout proof, HOPPER_PROOF_WORKERS
// (default 3) must equal the release's worker replicas, and HOPPER_PROOF_JOBS
// and HOPPER_PROOF_JOB_MS size its load. HOPPER_SCALE_JOBS (default 20000)
// and HOPPER_SCALE_JOB_MS (default 2000) size the autoscaling proof's.
// HOPPER_FAILOVER_RATE (default 200 enqueues/s) and HOPPER_FAILOVER_JOB_MS
// (default 50) size the failover proofs' load. HOPPER_FAILOVER_LAG (default
// 0s) pauses replication for that long before the crash. HOPPER_K8S_NODE
// (default hopper-control-plane) is the kind node whose runtime the crash
// goes through.
// The soak (TestSoak) offers keyed sleep jobs open loop, at HOPPER_SOAK_RATE
// (default 200) a second for HOPPER_SOAK_DURATION (default 2h), with at most
// HOPPER_SOAK_INFLIGHT (default 256) calls in flight: a call that can't start
// on time is counted as behind, never queued. Jobs run HOPPER_SOAK_JOB_MS
// (default 50), and HOPPER_SOAK_CONNS (default 4) client connections reach
// the brokers through a local proxy that follows them across restarts. Every
// HOPPER_SOAK_CHUNK (default 1000000) jobs share a run name and are
// reconciled together. Faults: a worker SIGKILL every
// HOPPER_SOAK_WORKER_KILL_EVERY (default 3m), a broker rolling restart every
// HOPPER_SOAK_BROKER_RESTART_EVERY (default 15m), and one failover of shard
// HOPPER_SOAK_FAILOVER_SHARD (default 1) at HOPPER_SOAK_FAILOVER_AT (default
// 0.5) of the duration; 0 turns any off. HOPPER_SOAK_KILL is how a container
// is killed: crictl-docker (default; docker exec into the kind node) or
// crictl-debug (a privileged kubectl debug pod on the node, in namespace
// HOPPER_SOAK_DEBUG_NAMESPACE, default "default", from image
// HOPPER_SOAK_DEBUG_IMAGE, default busybox:1.37.0). HOPPER_SOAK_DRAIN_TIMEOUT
// (default 30m) bounds the drain, and HOPPER_SOAK_PROMETHEUS (default
// monitoring/prometheus-operated:9090) is where pickup p99 comes from; without
// it that column is blank.
// HOPPER_PROOF_DIR keeps the captured logs, timeline and report instead of
// discarding them with the test's temp directory.
// A failover run writes failover.json, lost.json when a job was lost, and,
// under synchronous replication, standbys.json: the surviving instances
// sampled from the crash until one had a synchronous standby streaming to it.
package k8s
