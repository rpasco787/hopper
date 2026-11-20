#!/usr/bin/env bash
# Phase 1b before (e61ae1c) and after (HEAD), at 1, 2 and 4 brokers, in one
# loop. Five repetitions; each starts one configuration later.
set -u
cd "$(git rev-parse --show-toplevel)"
BEFORE=e61ae1c
AFTER=$(git rev-parse --short HEAD)
OUT=docs/bench/1b/final
echo "before=$BEFORE after=$AFTER started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
docker stop hopper-control-plane 2>/dev/null || true
configs=("before 1" "after 1" "before 2" "after 2" "before 4" "after 4")
for i in 1 2 3 4 5; do
  for k in 0 1 2 3 4 5; do
    read -r v b <<<"${configs[$(((i + k) % 6))]}"
    ref=$AFTER; [ "$v" = before ] && ref=$BEFORE
    env REF="$ref" PIN=1 BROKERS="$b" WORKERS=8 PAYLOAD_BYTES=300 OUT="$OUT" VARIANT="$v" \
      LABEL="final $v b$b" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $v b$b run $i"
  done
done
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
