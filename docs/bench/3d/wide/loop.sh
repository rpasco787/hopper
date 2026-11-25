#!/usr/bin/env bash
# Phase 3d: 10,000 handlers at once on one worker, Go against Java. Each run
# preloads 10,000 sleep jobs of WIDE_MS and starts one worker at concurrency
# 10,000, which asks for a WIDE_LEASE lease, so every handler is open at once
# and none renews (docs/bench/java.md). Three measured runs per SDK at warn,
# then one proof run each at info, whose logs show the handlers open at once;
# the Java proof run records JFR with native memory tracking. About 15 minutes.
# Reproduce with: bash docs/bench/3d/wide/loop.sh
set -u
cd "$(git rev-parse --show-toplevel)"
. docs/bench/3d/common.sh
. docs/bench/3d/pilot/settings.env
OUT=docs/bench/3d/wide
preflight
start=$(now)
echo "ref=$REF wide_mem=$WIDE_MEM wide_ms=$WIDE_MS wide_lease=$WIDE_LEASE wide_jfr=$WIDE_JFR started=$(date -u +%FT%TZ)" \
  >"$OUT/loop-settings.txt"
base="HOPPER_METRICS_LISTEN= HOPPER_LEASE_DURATION=$WIDE_LEASE"
wide() { # sdk variant worker-env [NAME=value ...]
  local sdk="$1" v="$2" we="$3"
  shift 3
  env REF="$REF" PIN=1 BROKERS=1 WORKERS=1 CONCURRENCY=10000 JOBS=10000 WORK_MS="$WIDE_MS" PAYLOAD_BYTES=300 \
    WORKER="$sdk" WORKER_MEM="$WIDE_MEM" WORKER_ENV="$we" OUT="$OUT" VARIANT="$v" LABEL="3d $v" "$@" \
    caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $v"
}
for i in 1 2 3; do
  if [ $((i % 2)) = 1 ]; then order="go java"; else order="java go"; fi
  for sdk in $order; do wide "$sdk" "$sdk-wide" "$base"; done
done
wide go go-proof "$base HOPPER_LOG_LEVEL=info"
wide java java-proof "$base HOPPER_LOG_LEVEL=info" \
  WORKER_JVM="-XX:+UseSerialGC -XX:NativeMemoryTracking=summary" JFR="$WIDE_JFR"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
sleeps_between "$start" "$(now)" >"$OUT/sleeps.txt"
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
{
  for m in worker_peak_rss_mib worker_threads worker_cpu_ms_per_1k startup_ms; do
    python3 scripts/bench_compare.py "$OUT/results.csv" go-wide java-wide --metric "$m" --lower-is-better
  done
  python3 scripts/bench_compare.py "$OUT/results.csv" go-wide java-wide --metric max_leased
} >"$OUT/decision.txt" 2>&1
# The proof runs: every handler open at once, judged from each worker's log.
go_proof=$(evidence_of "$OUT/results.csv" go-proof | tail -n 1)
java_proof=$(evidence_of "$OUT/results.csv" java-proof | tail -n 1)
HOPPER_PEAKS="$PWD/$go_proof/worker-1.stdout $PWD/$java_proof/worker-1.stdout" HOPPER_PEAKS_WANT=10000 \
  go test -tags peaks -count=1 -run TestPeaks -v ./test/accounting >"$OUT/peaks.txt" 2>&1
echo "go test exit: $?" >>"$OUT/peaks.txt"
# The Java proof run's recording: pinning, the collector, native memory, threads.
rec="$java_proof/worker-1.jfr"
"$JFR_TOOL" summary "$rec" >"$java_proof/worker-1.jfr-summary.txt"
for view in pinned-threads gc gc-pauses heap-configuration native-memory-committed thread-count allocation-by-class; do
  "$JFR_TOOL" view --width 200 "$view" "$rec" >"$java_proof/worker-1.jfr-$view.txt" 2>&1 || true
done
"$JFR_TOOL" print --events jdk.VirtualThreadPinned "$rec" >"$java_proof/worker-1.jfr-pinned.txt"
pinned=$(awk '$1 == "jdk.VirtualThreadPinned" { n = $2 } END { print n + 0 }' "$java_proof/worker-1.jfr-summary.txt")
echo "jdk.VirtualThreadPinned events in the Java proof run: $pinned" >>"$OUT/decision.txt"
# The pinning count's positive control, in the image these runs used.
bash docs/bench/3d/pinning/control.sh "hopper-bench/worker-java:ref-$REF" || echo "THE PINNING CONTROL FAILED" >&2
cat "$OUT/decision.txt" "$OUT/peaks.txt"
[ ! -s "$OUT/sleeps.txt" ] || echo "THE LAPTOP SLEPT DURING THIS LOOP: it is void (sleeps.txt)" >&2
