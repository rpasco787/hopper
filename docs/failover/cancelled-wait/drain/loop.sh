#!/usr/bin/env bash
# The drain before (a3fcbda) and after (HEAD) the cancelled-wait fix, one
# broker, five interleaved pairs; each repetition swaps which goes first.
set -u
cd "$(git rev-parse --show-toplevel)"
BEFORE=a3fcbda
AFTER=$(git rev-parse --short HEAD)
OUT=docs/failover/cancelled-wait/drain
echo "before=$BEFORE after=$AFTER started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
docker stop hopper-control-plane 2>/dev/null || true
configs=(before after)
for i in 1 2 3 4 5; do
  for k in 0 1; do
    v=${configs[$(((i + k) % 2))]}
    ref=$AFTER; [ "$v" = before ] && ref=$BEFORE
    env REF="$ref" PIN=1 BROKERS=1 WORKERS=8 PAYLOAD_BYTES=300 OUT="$OUT" VARIANT="$v" \
      LABEL="cancelled-wait $v" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $v run $i"
  done
done
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
