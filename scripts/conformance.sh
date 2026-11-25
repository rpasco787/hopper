#!/usr/bin/env bash
# The conformance suite (docs/design/conformance.md): every scenario against
# the Go worker built from the tree, then against the Java example worker,
# whose pass also runs the mixed fleet. One Compose stack at a time, as
# project hopper-chaos.
#   scripts/conformance.sh go | java | all     (default: all)
# HOPPER_CHAOS_DIR keeps the evidence; the default is a fresh directory under
# TMPDIR. JAVA_IMAGE names the Java image it builds (hopper-example-worker:dev).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

need=2.24.4 # !reset in test/chaos/worker-image.yaml, !override in compose.yaml
have=$(docker compose version --short)
if [ "$(printf '%s\n%s\n' "$need" "${have%%-*}" | sort -V | head -1)" != "$need" ]; then
  echo "conformance.sh: Docker Compose $have is older than $need" >&2
  exit 2
fi
JAVA_IMAGE=${JAVA_IMAGE:-hopper-example-worker:dev}
EVIDENCE=${HOPPER_CHAOS_DIR:-${TMPDIR:-/tmp}/hopper-conformance-$(date +%s)}
mkdir -p "$EVIDENCE"

pass() { # name image
  mkdir -p "$EVIDENCE/$1"
  echo "conformance.sh: the $1 pass, evidence in $EVIDENCE/$1"
  HOPPER_CHAOS_DIR="$EVIDENCE/$1" HOPPER_CONFORMANCE_WORKER="$2" \
    go test -tags conformance -count=1 -timeout 45m -v ./test/chaos 2>&1 | tee "$EVIDENCE/$1.log"
}
java_image() {
  docker build -t "$JAVA_IMAGE" -f java/hopper-example-worker/Dockerfile java
}

case "${1:-all}" in
  go) pass go "" ;;
  java) java_image && pass java "$JAVA_IMAGE" ;;
  all)
    rc=0
    pass go "" || rc=1
    { java_image && pass java "$JAVA_IMAGE"; } || rc=1
    exit $rc
    ;;
  *)
    echo "usage: $0 go|java|all" >&2
    exit 2
    ;;
esac
