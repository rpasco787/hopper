#!/usr/bin/env bash
# H5: cross-broker NOTIFY on and off at 4 brokers, loaded (RATE) and idle
# (20 jobs/s, one producer). Order alternates each repetition.
set -u
cd "$(git rev-parse --show-toplevel)"
REF=$(git rev-parse --short HEAD)
RATE="${RATE:-2500}"
echo "ref=$REF rate=$RATE started=$(date -u +%FT%TZ)" >docs/bench/1b/h5/loop-settings.txt
docker stop hopper-control-plane 2>/dev/null || true
run() { # out, variant, repetition, then the run's settings
  local out="$1" v="$2" i="$3"; shift 3
  env REF="$REF" PIN=1 BROKERS=4 WORKERS=8 PAYLOAD_BYTES=300 OUT="$out" VARIANT="wakeups-$v" \
    LABEL="$(basename "$out") $v" "$@" caffeinate -dimsu scripts/bench.sh steady || echo "FAILED $out $v run $i"
}
for i in 1 2 3 4 5; do
  order=(on off); [ $((i % 2)) = 0 ] && order=(off on)
  for v in "${order[@]}"; do
    # macOS bash 3.2 rejects an empty "${array[@]}" under set -u, so pass a
    # harmless setting when wakeups stay on.
    extra="BROKER_ENV=HOPPER_WAKEUPS=true"; [ "$v" = off ] && extra="BROKER_ENV=HOPPER_WAKEUPS=false"
    run docs/bench/1b/h5 "$v" "$i" RATE="$RATE" DURATION=60s "$extra"
    if [ "$i" -le 3 ]; then run docs/bench/1b/h5-idle "$v" "$i" RATE=20 PRODUCERS=1 "$extra"; fi
  done
done
for e in h5 h5-idle; do python3 scripts/bench_summary.py docs/bench/1b/$e/results.csv >docs/bench/1b/$e/summary.md; done
echo "finished=$(date -u +%FT%TZ)" >>docs/bench/1b/h5/loop-settings.txt
