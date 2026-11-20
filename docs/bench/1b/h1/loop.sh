#!/usr/bin/env bash
# H1: lease streams 4, 8 and 16 at 128 slots, 1 broker, one build.
set -u
cd "$(git rev-parse --show-toplevel)"
REF=$(git rev-parse --short HEAD)
OUT=docs/bench/1b/h1
echo "ref=$REF started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
docker stop hopper-control-plane 2>/dev/null || true
configs=("4 32" "8 16" "16 8")
for i in 1 2 3 4 5; do
  for k in 0 1 2; do
    read -r w c <<<"${configs[$(((i + k) % 3))]}"
    env REF="$REF" PIN=1 BROKERS=1 WORKERS="$w" CONCURRENCY="$c" PAYLOAD_BYTES=300 OUT="$OUT" \
      LABEL="h1 ${w}x$c" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED ${w}x$c run $i"
  done
done
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
