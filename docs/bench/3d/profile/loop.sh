#!/usr/bin/env bash
# Phase 3d profiles (diagnostics, not results): where each worker's CPU goes in
# a long drain, and the Go worker's memory with 10,000 handlers open. Built
# from HEAD, the ref with cmd/worker -debug-listen, not docs/bench/3d/ref.txt.
# About 10 minutes. Reproduce with: bash docs/bench/3d/profile/loop.sh
set -u
cd "$(git rev-parse --show-toplevel)"
. docs/bench/3d/common.sh
. docs/bench/3d/pilot/settings.env
REF=$(git rev-parse --short HEAD)
OUT=docs/bench/3d/profile
preflight
start=$(now)
echo "ref=$REF started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
common=(REF="$REF" PIN=1 BROKERS=1 PAYLOAD_BYTES=300 OUT="$OUT")
env "${common[@]}" WORKERS=8 WORKER=go WORKER_MEM=384m JOBS=200000 WORKER_ENV="HOPPER_METRICS_LISTEN=" \
  WORKER_PPROF=5 WORKER_PPROF_SECONDS=10 VARIANT=go-profile LABEL="3d go profile" \
  caffeinate -dimsu scripts/bench.sh drain || echo "FAILED go-profile"
env "${common[@]}" WORKERS=8 WORKER=java WORKER_MEM=384m JOBS=200000 WORKER_ENV="HOPPER_METRICS_LISTEN=" \
  JFR=25 VARIANT=java-profile LABEL="3d java profile" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED java-profile"
env "${common[@]}" WORKERS=1 CONCURRENCY=10000 JOBS=10000 WORK_MS="$WIDE_MS" WORKER=go WORKER_MEM="$WIDE_MEM" \
  WORKER_ENV="HOPPER_METRICS_LISTEN= HOPPER_LEASE_DURATION=$WIDE_LEASE" WORKER_PPROF=8 VARIANT=go-wide-profile \
  LABEL="3d go wide profile" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED go-wide-profile"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
sleeps_between "$start" "$(now)" >"$OUT/sleeps.txt"
for v in go-profile go-wide-profile; do
  d=$(evidence_of "$OUT/results.csv" "$v" | tail -n 1)
  go tool pprof -top -nodecount=40 "$d/worker-cpu.pprof" >"$d/worker-cpu-top.txt" 2>&1 || true
  grep -E '^# (HeapAlloc|HeapInuse|HeapSys|Stack|Sys|NumGC) = ' "$d/worker-heap.txt" >"$d/worker-memstats.txt" || true
  head -n 1 "$d/worker-goroutine.txt" >"$d/worker-goroutines.txt" || true
done
d=$(evidence_of "$OUT/results.csv" java-profile | tail -n 1)
"$JFR_TOOL" summary "$d/worker-1.jfr" >"$d/worker-1.jfr-summary.txt"
for view in hot-methods cpu-load gc gc-pauses gc-cpu-time longest-compilations compiler-statistics pinned-threads thread-count; do
  "$JFR_TOOL" view --width 200 "$view" "$d/worker-1.jfr" >"$d/worker-1.jfr-$view.txt" 2>&1 || true
done
