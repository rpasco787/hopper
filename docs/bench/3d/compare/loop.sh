#!/usr/bin/env bash
# Phase 3d: the Go worker (cmd/worker) against the Java example worker, drain
# and steady, on one build, in one interleaved loop: 1 broker, 8 workers x 16
# slots, 300-byte jobs, PIN=1, every worker container limited to 384 MiB and
# serving no metrics. Five repetitions; each starts one configuration later.
# About 45 minutes. Reproduce with: bash docs/bench/3d/compare/loop.sh
set -u
cd "$(git rev-parse --show-toplevel)"
. docs/bench/3d/common.sh
. docs/bench/3d/pilot/settings.env
OUT=docs/bench/3d/compare
preflight
start=$(now)
echo "ref=$REF jobs=$JOBS warm_jobs=$WARM_JOBS rate=$RATE started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
run() { # sdk scenario repetition
  local sdk="$1" sc="$2" i="$3" load
  if [ "$sc" = drain ]; then load=(JOBS="$JOBS" WARM_JOBS="$WARM_JOBS"); else load=(RATE="$RATE" DURATION=30s WARMUP=60); fi
  env REF="$REF" PIN=1 BROKERS=1 WORKERS=8 CONCURRENCY=16 PAYLOAD_BYTES=300 WORKER="$sdk" WORKER_MEM=384m \
    WORKER_ENV="HOPPER_METRICS_LISTEN=" OUT="$OUT" VARIANT="$sdk" LABEL="3d $sdk $sc" "${load[@]}" \
    caffeinate -dimsu scripts/bench.sh "$sc" || echo "FAILED $sdk $sc run $i"
}
configs=("go drain" "java drain" "go steady" "java steady")
for i in 1 2 3 4 5; do
  for k in 0 1 2 3; do
    read -r sdk sc <<<"${configs[$(((i + k) % 4))]}"
    run "$sdk" "$sc" "$i"
  done
done
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
sleeps_between "$start" "$(now)" >"$OUT/sleeps.txt"
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
c() { python3 scripts/bench_compare.py "$OUT/results.csv" go java "$@"; }
{
  for m in throughput warm_throughput; do c --where scenario=drain --metric "$m"; done
  for m in worker_cpu_ms_per_1k worker_peak_rss_mib worker_threads startup_ms; do
    c --where scenario=drain --metric "$m" --lower-is-better
  done
  c --where scenario=steady --metric throughput
  for m in pickup_p99_ms e2e_p99_ms worker_cpu_ms_per_1k worker_peak_rss_mib startup_ms; do
    c --where scenario=steady --metric "$m" --lower-is-better
  done
} >"$OUT/decision.txt" 2>&1
cat "$OUT/decision.txt"
[ ! -s "$OUT/sleeps.txt" ] || echo "THE LAPTOP SLEPT DURING THIS LOOP: it is void (sleeps.txt)" >&2
