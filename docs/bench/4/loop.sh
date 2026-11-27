#!/usr/bin/env bash
# Phase 4, Task 17: one PostgreSQL shard against two, each shard on one CPU
# (PIN=1: shard 0 on CPU 0, shard 1 on CPU 1), 2 brokers, Go workers, 40,000
# jobs of 300 B per drain. Two layouts, so the shard ratio isn't one layout's:
#   w8c16    8 workers x 16 slots, the plan's arm (the 1b loops' layout)
#   w2c256   2 workers x 256 slots, the credit-rich layout H6 found
#            (docs/scaling.md H6: 8 x 16 gets 0.47x of one broker's best)
# Four configurations, five repetitions; each repetition starts one
# configuration later, as docs/bench/1b/final/loop.sh does. The prediction
# (prediction.md) was committed before the first run.
# Reproduce with: bash docs/bench/4/loop.sh
set -u
cd "$(git rev-parse --show-toplevel)"
. docs/bench/3d/common.sh # preflight, now, sleeps_between (its REF is 3d's; this loop sets its own)
REF=$(git rev-parse --short HEAD)
OUT=docs/bench/4
preflight
start=$(now)
echo "ref=$REF started=$(date -u +%FT%TZ)" >"$OUT/loop-settings.txt"
configs=("w8c16 8 16 1" "w8c16 8 16 2" "w2c256 2 256 1" "w2c256 2 256 2")
for i in 1 2 3 4 5; do
  for k in 0 1 2 3; do
    read -r v w c s <<<"${configs[$(((i + k) % 4))]}"
    env REF="$REF" PIN=1 BROKERS=2 SHARDS="$s" WORKERS="$w" CONCURRENCY="$c" PAYLOAD_BYTES=300 \
      WORKER=go OUT="$OUT" VARIANT="$v" LABEL="shards $v s$s" \
      caffeinate -dimsu scripts/bench.sh drain || echo "FAILED $v s$s run $i"
  done
done
echo "finished=$(date -u +%FT%TZ)" >>"$OUT/loop-settings.txt"
sleeps_between "$start" "$(now)" >"$OUT/sleeps.txt"
python3 scripts/bench_summary.py "$OUT/results.csv" >"$OUT/summary-tables.md"
# Per-arm medians of what the verdict reads, in one table.
python3 - "$OUT/results.csv" >"$OUT/medians.txt" <<'PY'
import csv, statistics, sys
by = {}
for r in csv.DictReader(open(sys.argv[1])):
    by.setdefault((r['variant'], r['shards']), []).append(r)
cols = ['throughput', 'commits_per_job', 'wal_syncs_per_job', 'wal_kb_per_job', 'postgres_cpu',
        'broker_cpu', 'worker_cpu', 'lease_rows_per_call', 'lease_calls_per_job', 'lease_mean_ms',
        'ack_batch', 'active_backends', 'max_leased']
print('variant|shards|runs|min|max|' + '|'.join(cols))
for (v, s) in sorted(by):
    rs = by[(v, s)]
    ts = [float(r['throughput']) for r in rs]
    def med(k):
        xs = [float(r[k]) for r in rs if r[k] != '']
        return '%g' % statistics.median(xs) if xs else '-'
    print('%s|%s|%d|%g|%g|%s' % (v, s, len(rs), min(ts), max(ts), '|'.join(med(k) for k in cols)))
PY
{
  for v in w8c16 w2c256; do
    echo "== $v: one shard -> two"
    for m in throughput; do
      python3 scripts/bench_compare.py "$OUT/results.csv" 1 2 --column shards --where variant="$v" --metric "$m"
    done
    for m in commits_per_job wal_syncs_per_job lease_calls_per_job; do
      python3 scripts/bench_compare.py "$OUT/results.csv" 1 2 --column shards --where variant="$v" --metric "$m" --lower-is-better
    done
  done
  for s in 1 2; do
    echo "== shards=$s: w8c16 -> w2c256"
    python3 scripts/bench_compare.py "$OUT/results.csv" w8c16 w2c256 --where shards="$s"
  done
} >"$OUT/decision.txt" 2>&1
cat "$OUT/medians.txt" "$OUT/decision.txt"
[ ! -s "$OUT/sleeps.txt" ] || echo "THE LAPTOP SLEPT DURING THIS LOOP: it is void (sleeps.txt)" >&2
