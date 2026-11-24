#!/usr/bin/env bash
# Smoke runs of the Java client and worker against the Compose stack, for
# Phase 3a and 3b (docs/plans/2026-11-23-phase-3a-3b.md, Tasks 9 and 20).
# One stage per call:
#   up | client | worker-smoke | undisturbed | sigterm | cancel | abandon | down
# It uses its own Compose project, so it never touches a development stack,
# but it publishes the broker on 127.0.0.1:9090: run one stack at a time.
# Evidence goes to $OUT.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

P=hopperjava
OUT=${OUT:-/tmp/hopper-p3/smoke}
IMAGE=hopper-example-worker:dev
W=hopperjava-worker
RUN=${RUN:-$(date +%s)}
mkdir -p "$OUT"

compose() { HOPPER_SCHEDULER=false docker compose -p "$P" "$@"; }
sql() { compose exec -T postgres psql -U hopper -d hopper -At -F, -c "$1"; }
hopperctl() { go run ./cmd/hopperctl "$@"; }
# waitfor SECONDS DESCRIPTION COMMAND...: poll once a second until COMMAND succeeds.
waitfor() {
  local n=$1 what=$2
  shift 2
  for _ in $(seq "$n"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "java-smoke.sh: timed out after ${n}s waiting for $what" >&2
  return 1
}
logged() { docker logs "$W" 2>&1 | grep -c "\"msg\":\"$1\"" || true; }
at_least() { [ "$(logged "$1")" -ge "$2" ]; }
completed() { [ "$(sql "select count(*) from jobs where idempotency_key like '$1-%' and state = 'completed'")" -ge "$2" ]; }
rows() { sql "select id, state, attempts, coalesce(lease_id::text, '') from jobs where idempotency_key like '$1-%'"; }
start_worker() { # extra `docker run` arguments
  docker rm -f "$W" >/dev/null 2>&1 || true
  docker run -d --name "$W" --network "${P}_default" --stop-timeout 60 \
    -e HOPPER_BROKER=broker:9090 -e HOPPER_CONCURRENCY=4 -e HOPPER_SHUTDOWN_TIMEOUT=45s \
    -e HOPPER_LOG_LEVEL=info "$@" "$IMAGE" >/dev/null
  waitfor 60 "the worker's lease stream" at_least "lease stream opened" 1
}
accounting() { # name mode
  HOPPER_JAVALOG_LOG="$OUT/$1.log" HOPPER_JAVALOG_ROWS="$OUT/$1-rows.csv" HOPPER_JAVALOG_MODE="$2" \
    go test -tags javalog -count=1 -run 'TestJavaWorkerLog$' -v ./test/accounting 2>&1 | tee "$OUT/$1-accounting.log"
}

case "${1:-}" in
  up)
    compose up -d --build --wait postgres broker
    ;;
  client)
    (cd java && ./gradlew --no-daemon :hopper-client:composeTest --tests '*ComposeClientSmokeTest' \
      -PsmokeBroker=127.0.0.1:9090 -PsmokeOut="$OUT") 2>&1 | tee "$OUT/client.log"
    ;;
  worker-smoke)
    start_worker
    (cd java && ./gradlew --no-daemon :hopper-client:composeTest --tests '*ComposeWorkerSmokeTest' \
      -PsmokeBroker=127.0.0.1:9090 -PsmokeOut="$OUT") 2>&1 | tee "$OUT/worker-smoke.log"
    docker logs "$W" >"$OUT/worker-smoke-worker.log" 2>&1
    ;;
  undisturbed)
    k="jsu-$RUN"
    start_worker
    hopperctl enqueue -queue sleep -payload '{"ms":50}' -n 200 -key-prefix "$k-s"
    hopperctl enqueue -queue fail_n_times -payload '{"fail":1}' -n 20 -key-prefix "$k-f"
    hopperctl enqueue -queue cpu_burn -payload '{"ms":20}' -n 20 -key-prefix "$k-c"
    waitfor 180 "240 completed jobs" completed "$k" 240
    docker logs "$W" >"$OUT/undisturbed.log" 2>&1
    rows "$k" >"$OUT/undisturbed-rows.csv"
    accounting undisturbed undisturbed
    ;;
  sigterm)
    k="jss-$RUN"
    start_worker
    hopperctl enqueue -queue sleep -payload '{"ms":250}' -n 400 -key-prefix "$k"
    waitfor 60 "20 handler starts" at_least "job started" 20
    docker stop -t 60 "$W" >/dev/null
    echo "exit=$(docker inspect -f '{{.State.ExitCode}}' "$W")" | tee "$OUT/sigterm-exit.txt"
    docker start "$W" >/dev/null
    waitfor 240 "400 completed jobs" completed "$k" 400
    # Capture while the second process still runs: accounting keeps one
    # shutdown per log, and it must be the one that caught handlers running.
    docker logs "$W" >"$OUT/sigterm.log" 2>&1
    rows "$k" >"$OUT/sigterm-rows.csv"
    accounting sigterm graceful
    ;;
  cancel)
    [ -s "$OUT/cancelled-id.txt" ] || { echo "run worker-smoke first" >&2; exit 2; }
    docker logs "$W" >"$OUT/cancel.log" 2>&1
    HOPPER_JAVALOG_LOG="$OUT/cancel.log" HOPPER_JAVALOG_CANCELLED="$(cat "$OUT/cancelled-id.txt")" \
      HOPPER_JAVALOG_GRANT=30s go test -tags javalog -count=1 -run 'TestJavaWorkerStopsACancelledJobAtItsFirstRenewal$' \
      -v ./test/accounting 2>&1 | tee "$OUT/cancel-accounting.log"
    ;;
  abandon)
    k="jsa-$RUN"
    start_worker -e HOPPER_SHUTDOWN_TIMEOUT=2s
    hopperctl enqueue -queue sleep -payload '{"ms":20000}' -n 2 -key-prefix "$k"
    waitfor 60 "2 handler starts" at_least "job started" 2
    docker stop -t 30 "$W" >/dev/null
    echo "exit=$(docker inspect -f '{{.State.ExitCode}}' "$W")" | tee "$OUT/abandon-exit.txt"
    docker logs "$W" >"$OUT/abandon.log" 2>&1
    echo "deadline-lines=$(grep -c '"msg":"shutdown deadline reached"' "$OUT/abandon.log" || true)"
    grep -E '^worker: ' "$OUT/abandon.log" | head -1 || true
    docker start "$W" >/dev/null
    waitfor 120 "the abandoned jobs completing" completed "$k" 2
    ;;
  down)
    docker rm -f "$W" >/dev/null 2>&1 || true
    compose down -v
    ;;
  *)
    echo "usage: $0 up|client|worker-smoke|undisturbed|sigterm|cancel|abandon|down" >&2
    exit 2
    ;;
esac
