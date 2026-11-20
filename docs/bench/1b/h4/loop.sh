#!/usr/bin/env bash
# H4: the ownership heartbeat on (check) and off (skip), at 1 and 4 brokers,
# one build. Five repetitions; each starts one configuration later.
set -u
cd "$(git rev-parse --show-toplevel)"
REF=$(git rev-parse --short HEAD)
OUT=docs/bench/1b/h4
echo "ref=$REF started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
docker stop hopper-control-plane 2>/dev/null || true
run() { # name, broker count, repetition, then the run's settings
  local name="$1" b="$2" i="$3"; shift 3
  env REF="$REF" PIN=1 BROKERS="$b" WORKERS=8 PAYLOAD_BYTES=300 OUT="$OUT" LABEL="h4 $name b$b" "$@" \
    caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $name b$b run $i"
}
configs=("check 1" "skip 1" "check 4" "skip 4")
for i in 1 2 3 4 5; do
  for k in 0 1 2 3; do
    read -r v b <<<"${configs[$(((i + k) % 4))]}"
    case "$v" in
    check) run check "$b" "$i" VARIANT=check ;;
    skip) run skip "$b" "$i" VARIANT=skip WORKER_ENV="HOPPER_BENCH_SKIP_OWNERSHIP_CHECK=1" ;;
    esac
  done
done
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
