#!/usr/bin/env bash
set -euo pipefail
cd /Users/ryanpascual/Documents/Docs/hopper
h2_ref=$(git rev-parse --short HEAD)
evidence=docs/bench/1b/h2
mkdir -p "$evidence"
printf 'ref=%s\nstarted_utc=%s\n' "$h2_ref" "$(date -u +%FT%TZ)" > "$evidence/loop-settings.txt"
docker stop hopper-control-plane 2>/dev/null || true
variants=(nobatch batch flights1 async)
for i in 1 2 3 4 5; do
  for b in 1 2 4; do
    for k in 0 1 2 3; do
      name=${variants[$(((i + k) % 4))]}
      echo "START repetition=$i brokers=$b variant=$name utc=$(date -u +%FT%TZ)"
      settings=(VARIANT=nobatch)
      case "$name" in
        batch) settings=(VARIANT=batch 'BROKER_ENV=HOPPER_CONFIRM_BATCH=64');;
        flights1) settings=(VARIANT=batch-flights1 'BROKER_ENV=HOPPER_CONFIRM_BATCH=64 HOPPER_CONFIRM_FLIGHTS=1');;
        async) settings=(VARIANT=nobatch SYNC_COMMIT=off);;
      esac
      if env -u BROKER_ENV -u WORKER_ENV -u PG_ARGS -u BENCH_SQL -u EXPLAIN -u DB_MAX_CONNS REF="$h2_ref" SYNC_COMMIT=on PROFILE=0 KEEP=0 STATS_SETTLE=11 PIN=1 BROKERS="$b" WORKERS=8 CONCURRENCY=16 PAYLOAD_BYTES=300 JOBS=40000 OUT="$evidence" LABEL="$name b$b" "${settings[@]}" scripts/bench.sh drain; then
        echo "DONE repetition=$i brokers=$b variant=$name utc=$(date -u +%FT%TZ)"
      else
        echo "FAILED repetition=$i brokers=$b variant=$name utc=$(date -u +%FT%TZ)" | tee -a "$evidence/failures.txt"
      fi
    done
  done
done
python3 scripts/bench_summary.py "$evidence/results.csv" > "$evidence/summary.md"
