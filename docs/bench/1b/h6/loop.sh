#!/usr/bin/env bash
# H6: is the 8x16 single-broker drain held back by the credit per stream, by
# the number of lease streams, or by its 128 slots? Go workers only, 1 broker,
# PIN=1, 300-byte jobs, the 3d pilot's JOBS and WARM_JOBS, metrics off, every
# worker container limited to 384m. Seven arms, five repetitions, rotated:
#   w8c16 w4c32 w2c64 w1c128   the same 128 slots over 8, 4, 2 and 1 streams
#   w8c64 w2c256               512 slots over 8 and 2 streams
#   w2c64-lb6                  2x64 with the broker's lease batch capped at 6,
#                              the rows per call 8x16 leased in docs/bench/3d/layout
# The decision rule is D5 in the plan, copied into docs/scaling.md (H6).
# About 45 minutes. Reproduce with: bash docs/bench/1b/h6/loop.sh
set -u
cd "$(git rev-parse --show-toplevel)"
. docs/bench/3d/common.sh          # REF (14e87ee), preflight, now, sleeps_between
. docs/bench/3d/pilot/settings.env # JOBS, WARM_JOBS
OUT=docs/bench/1b/h6
preflight
start=$(now)
echo "ref=$REF jobs=$JOBS warm_jobs=$WARM_JOBS started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
run() { # variant workers concurrency broker-env repetition
  local v="$1" w="$2" c="$3" benv="$4" i="$5"
  env REF="$REF" PIN=1 BROKERS=1 WORKERS="$w" CONCURRENCY="$c" PAYLOAD_BYTES=300 WORKER=go \
    WORKER_MEM=384m WORKER_ENV="HOPPER_METRICS_LISTEN=" BROKER_ENV="$benv" OUT="$OUT" VARIANT="$v" \
    LABEL="h6 $v" JOBS="$JOBS" WARM_JOBS="$WARM_JOBS" \
    caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $v run $i"
}
configs=("go-w8c16 8 16 -" "go-w4c32 4 32 -" "go-w2c64 2 64 -" "go-w1c128 1 128 -"
         "go-w8c64 8 64 -" "go-w2c256 2 256 -" "go-w2c64-lb6 2 64 HOPPER_MAX_LEASE_BATCH=6")
for i in 1 2 3 4 5; do
  for k in 0 1 2 3 4 5 6; do
    read -r v w c benv <<<"${configs[$(((i + k) % 7))]}"
    [ "$benv" = - ] && benv=""
    run "$v" "$w" "$c" "$benv" "$i"
  done
done
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
sleeps_between "$start" "$(now)" >"$OUT/sleeps.txt"
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary.md"
# Per-arm medians of what the decision reads, in one table.
python3 - "$OUT/results.csv" >"$OUT/lease.txt" <<'EOF'
import csv, statistics, sys
by = {}
for r in csv.DictReader(open(sys.argv[1])):
    by.setdefault(r['variant'], []).append(r)
cols = ['throughput', 'warm_throughput', 'lease_rows_per_call', 'lease_calls_per_job',
        'lease_mean_ms', 'commits_per_job', 'postgres_cpu', 'broker_cpu', 'worker_cpu',
        'active_backends', 'max_leased', 'ack_batch']
print('variant|runs|' + '|'.join(cols))
for v in sorted(by):
    rs = by[v]
    def med(k):
        xs = [float(r[k]) for r in rs if r[k] != '']
        return '%g' % statistics.median(xs) if xs else '-'
    print('%s|%d|%s' % (v, len(rs), '|'.join(med(k) for k in cols)))
EOF
c() { python3 scripts/bench_compare.py "$OUT/results.csv" "$@"; }
pair() { # control treatment: throughput both ways, then the mechanism columns
  for m in throughput warm_throughput lease_rows_per_call; do c "$1" "$2" --metric "$m"; done
  for m in lease_calls_per_job commits_per_job postgres_cpu; do c "$1" "$2" --metric "$m" --lower-is-better; done
}
{
  echo "== Q2 credit per stream at 8 streams: go-w8c16 -> go-w8c64"; pair go-w8c16 go-w8c64
  echo "== Q2 stream count at 512 slots: go-w2c256 -> go-w8c64"; pair go-w2c256 go-w8c64
  echo "== stream count at 64 credit (slots differ): go-w2c64 -> go-w8c64"; pair go-w2c64 go-w8c64
  echo "== Q3 slots at 2 streams: go-w2c64 -> go-w2c256"; pair go-w2c64 go-w2c256
  echo "== Q4 rows per lease call at 2 streams: go-w2c64 -> go-w2c64-lb6"; pair go-w2c64 go-w2c64-lb6
  echo "== Q4 does lb6 reproduce 8x16: go-w8c16 -> go-w2c64-lb6"; pair go-w8c16 go-w2c64-lb6
  for v in go-w4c32 go-w2c64 go-w1c128 go-w2c256; do
    echo "== Q1 the curve: go-w8c16 -> $v"; pair go-w8c16 "$v"
  done
} >"$OUT/decision.txt" 2>&1
cat "$OUT/lease.txt" "$OUT/decision.txt"
[ ! -s "$OUT/sleeps.txt" ] || echo "THE LAPTOP SLEPT DURING THIS LOOP: it is void (sleeps.txt)" >&2
