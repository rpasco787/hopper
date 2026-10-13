#!/usr/bin/env bash
# Runs one benchmark against a fresh Compose project, hopper-bench, and keeps
# the evidence:
#
#   scripts/bench.sh drain    # preload JOBS, start the workers, time the drain
#   scripts/bench.sh steady   # workers up; loadgen offers RATE jobs/s for DURATION
#
# Settings, all from the environment:
#   REF            build broker and worker from this git ref (default: the working tree)
#   BROKERS        1 or 2 (default 1); with 2, workers split evenly and producers alternate
#   WORKERS        worker containers (default 4), CONCURRENCY slots each (default 16)
#   JOBS           drain: jobs to preload (default 40000)
#   RATE DURATION PRODUCERS BATCH WORK_MS
#                  steady: the offered load (default 1000 jobs/s, 30s, 8, 1, 0)
#   DB_MAX_CONNS   the brokers' pool size; unset keeps the binary's default
#   SYNC_COMMIT    PostgreSQL synchronous_commit (default on; off is a diagnostic)
#   PROFILE        1 takes a CPU profile of `broker` mid-run (REF must have -debug-listen)
#   OUT            evidence root (default ${TMPDIR:-/tmp}/hopper-bench); LABEL names the row
#   KEEP           1 leaves the stack up afterwards
#
# Prints one CSV row and appends it to $OUT/results.csv. Nothing else may use
# Docker heavily during a run: stop a kind cluster or chaos project first.
set -euo pipefail
cd "$(dirname "$0")/.."

scenario="${1:-}"
case "$scenario" in
  drain | steady) ;;
  *) echo "usage: scripts/bench.sh drain|steady" >&2; exit 2 ;;
esac
brokers="${BROKERS:-1}"
workers="${WORKERS:-4}"
concurrency="${CONCURRENCY:-16}"
case "$brokers" in 1 | 2) ;; *) echo "BROKERS must be 1 or 2" >&2; exit 2 ;; esac

# Images. A ref is built from `git archive`, so uncommitted files never leak
# into it, and its images are reused across runs; the working tree is rebuilt
# every time.
if [ -n "${REF:-}" ]; then
  sha="$(git rev-parse --short "${REF}^{commit}")"
  tag="ref-$sha"
  for bin in broker worker; do
    if ! docker image inspect "hopper-bench/$bin:$tag" >/dev/null 2>&1; then
      git archive --format=tar "$sha" |
        docker build -q -f deploy/docker/Dockerfile --build-arg BINARY="$bin" -t "hopper-bench/$bin:$tag" - >/dev/null
    fi
  done
else
  sha="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- . || echo -dirty)"
  tag="wt" # rebuilt every run; the previous build is left dangling
  for bin in broker worker; do
    docker build -q -f deploy/docker/Dockerfile --build-arg BINARY="$bin" -t "hopper-bench/$bin:$tag" . >/dev/null
  done
fi
# loadgen is the measuring instrument, so it always comes from the working tree.
docker build -q -f deploy/docker/Dockerfile --build-arg BINARY=loadgen -t hopper-bench/loadgen:wt . >/dev/null

out="${OUT:-${TMPDIR:-/tmp}/hopper-bench}"
dir="$out/$scenario-b$brokers-w$workers-$sha-$(date +%Y%m%dT%H%M%S)"
mkdir -p "$dir"
label="${LABEL:-$scenario $sha}"
: >"$dir/broker.env"
[ -n "${DB_MAX_CONNS:-}" ] && echo "HOPPER_DB_MAX_CONNS=$DB_MAX_CONNS" >>"$dir/broker.env"

export HOPPER_BENCH_TAG="$tag" HOPPER_BENCH_BROKER_ENV="$dir/broker.env"
export HOPPER_BENCH_SYNC_COMMIT="${SYNC_COMMIT:-on}" HOPPER_BENCH_CONCURRENCY="$concurrency"
compose() { docker compose -p hopper-bench -f docker-compose.yml -f deploy/bench/compose.yaml "$@"; }
sql() { compose exec -T postgres psql -U hopper -d hopper -qAt -c "$1"; }

sampler=""
profiler=""
cleanup() {
  if [ -n "$sampler" ]; then { kill "$sampler" && wait "$sampler"; } 2>/dev/null || true; fi
  compose logs --no-color >"$dir/logs.txt" 2>&1 || true
  [ "${KEEP:-0}" = 1 ] || compose down -v --remove-orphans -t 10 >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose down -v --remove-orphans >/dev/null 2>&1 || true
services="postgres broker"
[ "$brokers" = 2 ] && services="$services broker-standby"
# shellcheck disable=SC2086 # word splitting is the point
compose up -d --no-build --wait $services >/dev/null
sql "create extension pg_stat_statements" >/dev/null

# Workers split evenly; with one broker every worker dials it.
wa="$workers"; wb=0
if [ "$brokers" = 2 ]; then wa=$(((workers + 1) / 2)); wb=$((workers - wa)); fi
start_workers() {
  compose up -d --no-build --no-deps --scale worker="$wa" --scale worker-b="$wb" worker worker-b >/dev/null
}
brokers_arg="broker:9090"
[ "$brokers" = 2 ] && brokers_arg="broker:9090,broker-standby:9090"
metrics_ports="16091"
[ "$brokers" = 2 ] && metrics_ports="16091 16092"
loadgen() { compose run --rm -T --no-deps loadgen "$@"; }

# CPU per container, sampled through workload completion, with no idle tail.
# Drain includes worker startup: collect a baseline before any worker can
# consume, then sample throughout Compose startup as well as the drain.
sample_once() {
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}}' | grep '^hopper-bench-' >>"$dir/stats.csv" || true
}
sample() {
  while :; do sample_once; done
}
stop_sampler() {
  if [ -n "$sampler" ]; then { kill "$sampler" && wait "$sampler"; } 2>/dev/null || true; fi
  sampler=""
}
# Every broker's pool counters, before and after the measured interval, so
# their deltas leave out the preload and startup.
pool_snapshot() {
  for port in $metrics_ports; do
    echo "# $1 :$port"
    curl -sf "http://127.0.0.1:$port/metrics" | grep '^hopper_db_pool' || echo "# no pool metrics"
  done >>"$dir/pool.txt"
}
profile() {
  sleep 3
  curl -sf -o "$dir/broker-cpu.pprof" "http://127.0.0.1:16060/debug/pprof/profile?seconds=${PROFILE_SECONDS:-5}" ||
    echo "no profile: does $sha have -debug-listen?" >&2
}

case "$scenario" in
drain)
  jobs="${JOBS:-40000}"
  loadgen -broker "$brokers_arg" -n "$jobs" -batch 500 -producers 4 -wait=false -json >"$dir/preload.json"
  sql "vacuum analyze jobs" >/dev/null
  sql "select pg_stat_statements_reset()" >/dev/null
  pool_snapshot before
  sample_once
  sample & sampler=$!
  start_workers
  if [ "${PROFILE:-0}" = 1 ]; then profile & profiler=$!; fi
  deadline=$((SECONDS + 600))
  until [ "$(sql "select count(*) from jobs where state = 'completed'")" -ge "$jobs" ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "drain did not finish within 10 minutes" >&2; exit 1; }
    sleep 0.25
  done
  stop_sampler
  pool_snapshot after
  if [ -n "$profiler" ]; then wait "$profiler" || true; fi
  # The drain runs from the first lease to the last completion, on the
  # database's clock; every job must have completed on its first lease.
  IFS=, read -r throughput retried <<<"$(sql "
    select round(count(*) / extract(epoch from max(finished_at) - min(leased_at))),
           count(*) filter (where attempts > 1)
    from jobs" | tr '|' ',')"
  enqueue_p50=""; enqueue_p99=""; pickup_p50=""; pickup_p99=""; e2e_p50=""; e2e_p99=""
  offered=""; max_lag=""; kept_up=""
  ;;
steady)
  start_workers
  # Every worker has a stream open before the offered load starts.
  deadline=$((SECONDS + 120))
  # shellcheck disable=SC2086
  until [ "$(compose logs ${services#postgres } 2>/dev/null | grep -c '"lease stream opened"')" -ge "$workers" ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "the workers did not all open lease streams" >&2; exit 1; }
    sleep 0.25
  done
  sql "select pg_stat_statements_reset()" >/dev/null
  pool_snapshot before
  sample & sampler=$!
  if [ "${PROFILE:-0}" = 1 ]; then profile & profiler=$!; fi
  loadgen -broker "$brokers_arg" -rate "${RATE:-1000}" -duration "${DURATION:-30s}" \
    -producers "${PRODUCERS:-8}" -batch "${BATCH:-1}" -work-ms "${WORK_MS:-0}" -json >"$dir/loadgen.json"
  stop_sampler
  pool_snapshot after
  if [ -n "$profiler" ]; then wait "$profiler" || true; fi
  read -r throughput retried enqueue_p50 enqueue_p99 pickup_p50 pickup_p99 e2e_p50 e2e_p99 offered max_lag <<<"$(jq -r '
    [(.throughput | round), .retried, .enqueue.p50_ms, .enqueue.p99_ms, .pickup.p50_ms, .pickup.p99_ms,
     .end_to_end.p50_ms, .end_to_end.p99_ms, .offered_rate, (.max_lag_ms | round)] | @tsv' "$dir/loadgen.json")"
  # Keeping up: at least 98% of offered, no call a second late.
  kept_up="$(awk -v t="$throughput" -v o="$offered" -v l="$max_lag" \
    'BEGIN { print (o > 0 && t >= 0.98 * o && l < 1000) ? "yes" : "no" }')"
  ;;
esac
stop_sampler

# Every statement counts toward the total, so a share is of all database time
# in the interval, the harness's own polling included.
{
  echo "calls|total_ms|pct_of_total|mean_ms|rows_per_call|blks_per_call|query"
  sql "select calls, round(total_exec_time::numeric),
              round((100 * total_exec_time / nullif(sum(total_exec_time) over (), 0))::numeric, 1),
              round(mean_exec_time::numeric, 3), round(rows::numeric / calls, 1),
              round((shared_blks_hit + shared_blks_read)::numeric / calls, 1),
              left(regexp_replace(query, '\s+', ' ', 'g'), 90)
       from pg_stat_statements order by total_exec_time desc limit 15"
  sql "select 'total|statements=' || count(*) || '|calls=' || sum(calls)
              || '|total_ms=' || round(sum(total_exec_time)::numeric) from pg_stat_statements"
} >"$dir/statements.txt"
# Mean CPU per role, in percent of one core.
cpu() { awk -F, -v re="$1" '$1 ~ re { gsub("%", "", $2); s[$1] += $2; n[$1]++ }
  END { t = 0; for (c in s) t += s[c] / n[c]; printf "%.0f", t }' "$dir/stats.csv"; }
pg_cpu="$(cpu 'postgres')"; broker_cpu="$(cpu 'broker')"; worker_cpu="$(cpu 'worker')"
pool="${DB_MAX_CONNS:-default}"

header="label,scenario,ref,brokers,workers,concurrency,db_max_conns,sync_commit,throughput,retried,enqueue_p50_ms,enqueue_p99_ms,pickup_p50_ms,pickup_p99_ms,e2e_p50_ms,e2e_p99_ms,offered,max_lag_ms,kept_up,postgres_cpu,broker_cpu,worker_cpu,evidence"
row="$label,$scenario,$sha,$brokers,$workers,$concurrency,$pool,$HOPPER_BENCH_SYNC_COMMIT,$throughput,$retried,$enqueue_p50,$enqueue_p99,$pickup_p50,$pickup_p99,$e2e_p50,$e2e_p99,$offered,$max_lag,$kept_up,$pg_cpu,$broker_cpu,$worker_cpu,$dir"
[ -f "$out/results.csv" ] || echo "$header" >"$out/results.csv"
echo "$row" >>"$out/results.csv"
echo "$header"
echo "$row"
