#!/usr/bin/env bash
# Brings up hopper on the local kind cluster "hopper": creates the cluster if it
# does not exist, builds the three images under a fresh tag, loads them into
# the node, and installs or upgrades the chart into a namespace that enforces
# the restricted Pod Security Standard. Extra arguments go to helm:
#
#   scripts/kind-up.sh --set worker.replicas=3
#
# HOPPER_CONFORMANCE_WORKER=java runs the Java example worker as the chart's
# worker instead of the Go one (docs/design/conformance.md);
# HOPPER_CONFORMANCE_WORKER_MEMORY sets its memory limit (default 384Mi).
#
# Every kubectl and helm call names the kind-hopper context, so this never
# touches another cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

cluster=hopper
context=kind-hopper
ns="${HOPPER_NAMESPACE:-hopper}"
release="${HOPPER_RELEASE:-hopper}"
# A fresh tag per run: with IfNotPresent, reusing a tag would leave running
# pods on the old image after the node had loaded the new one.
tag="${TAG:-dev-$(date +%s)}"

if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --config deploy/kind/cluster.yaml
fi

images=()
for bin in broker worker hopperctl; do
  case "$bin" in
    hopperctl) image="ghcr.io/rpasco787/hopperctl:$tag" ;;
    *) image="ghcr.io/rpasco787/hopper-$bin:$tag" ;;
  esac
  # No provenance attestation: kind load imports every platform in the image
  # index, and attestation manifests have broken that.
  docker build -q --provenance=false -f deploy/docker/Dockerfile \
    --build-arg BINARY="$bin" -t "$image" . >/dev/null
  images+=("$image")
done

# HOPPER_CONFORMANCE_WORKER=java runs the Java example worker as the chart's
# worker (docs/design/conformance.md): built from java/ under the same tag,
# set as worker.image, with the memory limit the Compose runs settled on.
worker_args=()
if [ "${HOPPER_CONFORMANCE_WORKER:-}" = java ]; then
  image="ghcr.io/rpasco787/hopper-example-worker:$tag"
  docker build -q --provenance=false -f java/hopper-example-worker/Dockerfile -t "$image" java >/dev/null
  images+=("$image")
  worker_args=(--set worker.image=ghcr.io/rpasco787/hopper-example-worker
    --set "worker.resources.limits.memory=${HOPPER_CONFORMANCE_WORKER_MEMORY:-384Mi}")
fi
kind load docker-image --name "$cluster" "${images[@]}"

kubectl --context "$context" create namespace "$ns" --dry-run=client -o yaml |
  kubectl --context "$context" apply -f -
kubectl --context "$context" label namespace "$ns" --overwrite \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/enforce-version=v1.35

helm upgrade --install "$release" deploy/helm/hopper \
  --kube-context "$context" -n "$ns" \
  --set image.tag="$tag" --wait --timeout 5m ${worker_args[@]+"${worker_args[@]}"} "$@"

echo "release $release in namespace $ns runs image tag $tag (commit $(git rev-parse --short HEAD)$(git diff --quiet || echo ', dirty'))"
