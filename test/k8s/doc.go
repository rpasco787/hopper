// Package k8s holds end-to-end proofs against a kind cluster running the
// hopper chart. The tests are behind the k8s build tag, so `go test ./...`
// never needs a cluster. To run them:
//
//	scripts/kind-up.sh --set worker.replicas=3
//	go test -tags k8s -count=1 -timeout 20m -v ./test/k8s
//
// HOPPER_K8S_CONTEXT, HOPPER_K8S_NAMESPACE and HOPPER_K8S_RELEASE override
// kind-hopper, hopper and hopper. HOPPER_PROOF_WORKERS (default 3) must equal
// the release's worker replicas. HOPPER_PROOF_JOBS and HOPPER_PROOF_JOB_MS
// size the load. HOPPER_PROOF_DIR keeps the captured logs and report instead
// of discarding them with the test's temp directory.
package k8s
