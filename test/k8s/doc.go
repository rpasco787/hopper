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
// HOPPER_K8S_CONTEXT, HOPPER_K8S_NAMESPACE and HOPPER_K8S_RELEASE override
// kind-hopper, hopper and hopper. For the rollout proof, HOPPER_PROOF_WORKERS
// (default 3) must equal the release's worker replicas, and HOPPER_PROOF_JOBS
// and HOPPER_PROOF_JOB_MS size its load. HOPPER_SCALE_JOBS (default 20000)
// and HOPPER_SCALE_JOB_MS (default 2000) size the autoscaling proof's.
// HOPPER_PROOF_DIR keeps the captured logs, timeline and report instead of
// discarding them with the test's temp directory.
package k8s
