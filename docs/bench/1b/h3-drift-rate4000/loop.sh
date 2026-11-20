#!/usr/bin/env bash
# H3: lease cost and dead tuples over 10 minutes of steady load, default
# storage settings against tuned ones. Two repetitions, order alternated.
set -u
cd "$(git rev-parse --show-toplevel)"
REF=$(git rev-parse --short HEAD)
OUT=docs/bench/1b/h3-drift
RATE="${RATE:-4000}"
echo "ref=$REF rate=$RATE started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
docker stop hopper-control-plane 2>/dev/null || true
TUNED_SQL="alter table jobs set (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0)"
run() { # variant, repetition, then the run's settings
  local v="$1" i="$2"; shift 2
  env REF="$REF" PIN=1 BROKERS=1 WORKERS=8 PAYLOAD_BYTES=300 RATE="$RATE" DURATION=10m TRACE=1 \
    OUT="$OUT" VARIANT="$v" LABEL="drift $v" "$@" caffeinate -dimsu scripts/bench.sh steady || echo "FAILED $v run $i"
}
for i in 1 2; do
  order=(default tuned); [ "$i" = 2 ] && order=(tuned default)
  for v in "${order[@]}"; do
    case "$v" in
    default) run default "$i" ;;
    tuned) run tuned "$i" BENCH_SQL="$TUNED_SQL" PG_ARGS="autovacuum_naptime=5s" ;;
    esac
  done
done
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
