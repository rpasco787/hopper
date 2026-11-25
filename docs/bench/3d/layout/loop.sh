#!/usr/bin/env bash
# Phase 3d follow-up: is the Java worker's drain gap the layout (eight JVMs on
# three CPUs) or its per-job cost? The same 128 slots on the same three worker
# CPUs (PIN=1), split 8x16 (the comparison's layout, rerun here as the in-loop
# baseline), 2x64 and 1x128, for both SDKs. 1 broker, 300-byte jobs, the
# pilot's JOBS and WARM_JOBS, metrics off. Memory: 384m per worker at 8x16 (as
# compare/), 1g at 2x64 and 1x128 (as wide/), so no JVM is starved of heap by
# fewer, larger workers. Three repetitions, rotated. About 35 minutes.
# Reproduce with: bash docs/bench/3d/layout/loop.sh
set -u
cd "$(git rev-parse --show-toplevel)"
. docs/bench/3d/common.sh
. docs/bench/3d/pilot/settings.env
OUT=docs/bench/3d/layout
preflight
start=$(now)
echo "ref=$REF jobs=$JOBS warm_jobs=$WARM_JOBS started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
run() { # sdk workers concurrency repetition
  local sdk="$1" w="$2" c="$3" i="$4" mem=1g
  [ "$w" = 8 ] && mem=384m
  env REF="$REF" PIN=1 BROKERS=1 WORKERS="$w" CONCURRENCY="$c" PAYLOAD_BYTES=300 WORKER="$sdk" \
    WORKER_MEM="$mem" WORKER_ENV="HOPPER_METRICS_LISTEN=" OUT="$OUT" VARIANT="$sdk-w${w}c$c" \
    LABEL="3d layout $sdk ${w}x$c" JOBS="$JOBS" WARM_JOBS="$WARM_JOBS" \
    caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $sdk ${w}x$c run $i"
}
configs=("go 8 16" "java 8 16" "go 2 64" "java 2 64" "go 1 128" "java 1 128")
for i in 1 2 3; do
  for k in 0 1 2 3 4 5; do
    read -r sdk w c <<<"${configs[$(((i + k) % 6))]}"
    run "$sdk" "$w" "$c" "$i"
  done
done
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
sleeps_between "$start" "$(now)" >"$OUT/sleeps.txt"
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
c() { python3 scripts/bench_compare.py "$OUT/results.csv" "$@"; }
{
  for l in w8c16 w2c64 w1c128; do
    echo "== java against go at $l"
    for m in throughput warm_throughput; do c "go-$l" "java-$l" --metric "$m"; done
    c "go-$l" "java-$l" --metric worker_cpu_ms_per_1k --lower-is-better
  done
  for l in w2c64 w1c128; do
    echo "== java at $l against java at w8c16"
    for m in throughput warm_throughput; do c java-w8c16 "java-$l" --metric "$m"; done
    c java-w8c16 "java-$l" --metric worker_cpu_ms_per_1k --lower-is-better
  done
} >"$OUT/decision.txt" 2>&1
cat "$OUT/decision.txt"
[ ! -s "$OUT/sleeps.txt" ] || echo "THE LAPTOP SLEPT DURING THIS LOOP: it is void (sleeps.txt)" >&2
