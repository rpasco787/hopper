#!/usr/bin/env bash
# The CI steps (.github/workflows/ci.yml), one per call, so each runs the same
# way on a laptop as on a runner (docs/design/conformance.md, "CI"):
#   go-checks | go-test | java-build | java-gen-check | conformance
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

version() { # what wanted have
  case "$3" in
    "$2"*) ;;
    *) echo "ci.sh: $1 is $3, want $2" >&2; exit 1 ;;
  esac
}

case "${1:-}" in
  go-checks)
    version go "go version go1.27.1" "$(go version)"
    version helm v4.3.0 "$(helm version --short)"
    version buf 1.73.0 "$(buf --version)"
    unformatted=$(gofmt -l .)
    [ -z "$unformatted" ] || { echo "$unformatted"; echo "ci.sh: gofmt would change the files above" >&2; exit 1; }
    go vet ./...
    go vet -tags chaos ./test/chaos
    go vet -tags conformance ./test/chaos
    go vet -tags k8s ./test/k8s
    go vet -tags javalog ./test/accounting
    go vet -tags evidence ./test/accounting
    go vet -tags peaks ./test/accounting
    if go list -deps ./worker | grep -q prometheus; then echo "ci.sh: the worker SDK depends on Prometheus" >&2; exit 1; fi
    buf lint
    helm lint --strict deploy/helm/hopper
    helm lint --strict deploy/helm/hopper -f deploy/kind/autoscaling.yaml
    for f in scripts/*.sh; do bash -n "$f"; done
    docker compose -p hopper-ci config >/dev/null
    docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/p:ro" prom/prometheus:v3.14.0 test rules /p/queries_test.yml
    ;;
  go-test)
    go test -race -count=1 ./...
    ;;
  java-build)
    if [ "${CI:-}" = true ]; then version java 'openjdk version "25.0.5"' "$(java -version 2>&1 | head -1)"; fi
    (cd java && ./gradlew --no-daemon build)
    ;;
  java-gen-check)
    bash scripts/java-gen.sh --check
    ;;
  conformance)
    bash scripts/conformance.sh all
    ;;
  *)
    echo "usage: $0 go-checks|go-test|java-build|java-gen-check|conformance" >&2
    exit 2
    ;;
esac
