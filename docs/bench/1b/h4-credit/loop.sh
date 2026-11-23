#!/usr/bin/env bash
# Credit-anchored freshness: the drain before (770fdb5, the ownership check on
# every delivery) and after (HEAD), at 1 and 4 brokers, with H4's settings.
# Five repetitions; each starts one configuration later.
set -u
cd "$(git rev-parse --show-toplevel)"
BEFORE=770fdb5
AFTER=$(git rev-parse --short HEAD)
OUT=docs/bench/1b/h4-credit
echo "before=$BEFORE after=$AFTER started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
if [ "$(docker inspect -f '{{.State.Running}}' hopper-control-plane 2>/dev/null)" = true ]; then
  echo "hopper-control-plane is running; stop it first" >&2
  exit 2
fi
run() { # variant, ref, broker count, repetition
  local v="$1" ref="$2" b="$3" i="$4"
  env REF="$ref" PIN=1 BROKERS="$b" WORKERS=8 PAYLOAD_BYTES=300 OUT="$OUT" VARIANT="$v" \
    LABEL="h4-credit $v b$b" caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $v b$b run $i"
}
configs=("before 1" "after 1" "before 4" "after 4")
for i in 1 2 3 4 5; do
  for k in 0 1 2 3; do
    read -r v b <<<"${configs[$(((i + k) % 4))]}"
    ref=$AFTER
    [ "$v" = before ] && ref=$BEFORE
    run "$v" "$ref" "$b" "$i"
  done
done
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
