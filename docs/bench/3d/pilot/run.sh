#!/usr/bin/env bash
# Phase 3d pilot: the settings the loops use, decided by the rules in the plan
# (Task 12) and recorded in decision.txt and settings.env. Not results. About
# 25 minutes.
set -u
cd "$(git rev-parse --show-toplevel)"
. docs/bench/3d/common.sh
OUT=docs/bench/3d/pilot
preflight
start=$(now)
echo "ref=$REF started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
common=(REF="$REF" PIN=1 BROKERS=1 PAYLOAD_BYTES=300 WORKER_MEM=384m OUT="$OUT")
for sdk in go java; do
  env "${common[@]}" WORKERS=8 WORKER="$sdk" JOBS=200000 WORKER_ENV="HOPPER_METRICS_LISTEN=" \
    VARIANT="$sdk-long" LABEL="pilot $sdk long drain" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $sdk long"
done
for sdk in go java; do
  env "${common[@]}" WORKERS=8 WORKER="$sdk" RATE="${RATE:-1000}" DURATION=30s WARMUP=60 \
    WORKER_ENV="HOPPER_METRICS_LISTEN=" VARIANT="$sdk-steady" LABEL="pilot $sdk steady" \
    caffeinate -dimsu scripts/bench.sh steady || echo "FAILED $sdk steady"
done
for sdk in go java; do
  env "${common[@]}" WORKER_MEM="${WIDE_MEM:-1g}" WORKERS=1 CONCURRENCY=10000 JOBS=10000 WORKER="$sdk" \
    WORK_MS="${WIDE_MS:-15000}" WORKER_ENV="HOPPER_METRICS_LISTEN= HOPPER_LEASE_DURATION=${WIDE_LEASE:-60s}" \
    VARIANT="$sdk-wide" LABEL="pilot $sdk wide" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $sdk wide"
done
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
sleeps_between "$start" "$(now)" >"$OUT/sleeps.txt"
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
python3 docs/bench/3d/pilot/warmup.py $(evidence_of "$OUT/results.csv" go-long | sed 's|$|/rate.psv|') \
  $(evidence_of "$OUT/results.csv" java-long | sed 's|$|/rate.psv|') | tee "$OUT/warmup.txt"
