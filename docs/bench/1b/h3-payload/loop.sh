#!/usr/bin/env bash
# H3: payload 0, 300 and 1,500 bytes, 1 broker, one build.
set -u
cd "$(git rev-parse --show-toplevel)"
REF=$(git rev-parse --short HEAD)
OUT=docs/bench/1b/h3-payload
echo "ref=$REF started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
docker stop hopper-control-plane 2>/dev/null || true
payloads=(0 300 1500)
for i in 1 2 3 4 5; do
  for k in 0 1 2; do
    p=${payloads[$(((i + k) % 3))]}
    env REF="$REF" PIN=1 BROKERS=1 WORKERS=8 PAYLOAD_BYTES="$p" OUT="$OUT" LABEL="payload $p" \
      caffeinate -dimsu scripts/bench.sh drain || echo "FAILED payload $p run $i"
  done
done
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
