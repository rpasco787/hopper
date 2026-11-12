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
#   PIN            1 pins each role to its own Docker VM CPUs (needs 8; see the layout below)
#   OUT            evidence root (default ${TMPDIR:-/tmp}/hopper-bench); LABEL names the row
#   KEEP           1 leaves the stack up afterwards
#
# Prints one CSV row and appends it to $OUT/results.csv. Nothing else may use
# Docker heavily during a run: stop a kind cluster or chaos project first.
#
# Each run keeps machine.txt (the Docker VM), waits.psv/waits.txt (active
# PostgreSQL backends by wait event, once a second) and, with PIN=1,
# cpuset.yaml. A run with no wait samples, no broker CPU samples, no lease
# statement or (drain) any retried job writes rejected.txt and no row.
# Summarize a results.csv with scripts/bench_summary.py.
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
pin="${PIN:-0}"
case "$pin" in 0 | 1) ;; *) echo "PIN must be 0 or 1" >&2; exit 2 ;; esac
vm_cpus="$(docker info --format '{{.NCPU}}')"
if [ "$pin" = 1 ] && [ "$vm_cpus" -lt 8 ]; then
  echo "PIN=1 needs a Docker VM with at least 8 CPUs; this one has $vm_cpus" >&2
  exit 2
fi

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
header="label,scenario,ref,brokers,workers,concurrency,db_max_conns,sync_commit,vm_cpus,pinned,throughput,retried,enqueue_p50_ms,enqueue_p99_ms,pickup_p50_ms,pickup_p99_ms,e2e_p50_ms,e2e_p99_ms,offered,max_lag_ms,kept_up,postgres_cpu,broker_cpu,worker_cpu,active_backends,on_cpu_pct,lock_pct,lwlock_pct,io_pct,lease_mean_ms,lease_blks_per_call,evidence"
if [ -f "$out/results.csv" ] && [ "$(head -n 1 "$out/results.csv")" != "$header" ]; then
  echo "$out/results.csv has a different header; use a new OUT" >&2
  exit 2
fi
dir="$out/$scenario-b$brokers-w$workers-c$vm_cpus-p$pin-$sha-$(date +%Y%m%dT%H%M%S)"
mkdir -p "$dir"
files=(-f docker-compose.yml -f deploy/bench/compose.yaml)
if [ "$pin" = 1 ]; then
  cat >"$dir/cpuset.yaml" <<'EOF'
# Written by scripts/bench.sh for PIN=1. Each role gets its own VM CPUs, and
# the layout is the same at one and two brokers, so only the broker count
# changes. With one broker, CPU 3 is idle.
services:
  postgres: {cpuset: "0-1"}
  broker: {cpuset: "2"}
  broker-standby: {cpuset: "3"}
  worker: {cpuset: "4-6"}
  worker-b: {cpuset: "4-6"}
  loadgen: {cpuset: "7"}
EOF
  files+=(-f "$dir/cpuset.yaml")
fi
{
  echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
  echo "docker: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes, server {{.ServerVersion}}')"
  echo "pinned: $pin"
} >"$dir/machine.txt"
label="${LABEL:-$scenario $sha}"
: >"$dir/broker.env"
[ -n "${DB_MAX_CONNS:-}" ] && echo "HOPPER_DB_MAX_CONNS=$DB_MAX_CONNS" >>"$dir/broker.env"

export HOPPER_BENCH_TAG="$tag" HOPPER_BENCH_BROKER_ENV="$dir/broker.env"
export HOPPER_BENCH_SYNC_COMMIT="${SYNC_COMMIT:-on}" HOPPER_BENCH_CONCURRENCY="$concurrency"
compose() { docker compose -p hopper-bench "${files[@]}" "$@"; }
sql() { compose exec -T postgres psql -U hopper -d hopper -qAt -c "$1"; }

samplers=""
profiler=""
cleanup() {
  stop_samplers
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
  until [ -e "$dir/samplers.stop" ]; do sample_once; done
}
# Samplers are asked to stop, not killed: killing one orphans the docker call
# it is in (a docker stats sample takes about 2s), whose output then lands
# after the row has been computed. Each finishes its current sample first, so
# the last sample can reach up to ~2s past the end of the workload.
stop_samplers() {
  [ -n "$samplers" ] || return 0
  : >"$dir/samplers.stop"
  for pid in $samplers; do wait "$pid" 2>/dev/null || true; done
  rm -f "$dir/samplers.stop"
  samplers=""
}
# Active client backends by wait event, once a second, as
# time|type|event|count, with one time|SAMPLE|-|0 line per sample. A backend
# that is active with no wait event is on a CPU or queued for one, so a starved
# host grows "CPU"; contention between lessors grows Lock and LWLock instead.
# The harness's own psql sessions are left out.
waits_once() {
  sql "select to_char(clock_timestamp(), 'HH24:MI:SS.MS'), 'SAMPLE', '-', 0
       union all
       select to_char(clock_timestamp(), 'HH24:MI:SS.MS'), coalesce(wait_event_type, 'CPU'),
              coalesce(wait_event, 'running'), count(*)
       from pg_stat_activity
       where backend_type = 'client backend' and state = 'active'
         and application_name <> 'psql' and pid <> pg_backend_pid()
       group by 2, 3" >>"$dir/waits.psv" || true
}
waits() { until [ -e "$dir/samplers.stop" ]; do sleep 1; [ -e "$dir/samplers.stop" ] || waits_once; done; }
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
  waits_once
  sample & samplers="$!"
  waits & samplers="$samplers $!"
  start_workers
  if [ "${PROFILE:-0}" = 1 ]; then profile & profiler=$!; fi
  deadline=$((SECONDS + 600))
  until [ "$(sql "select count(*) from jobs where state = 'completed'")" -ge "$jobs" ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "drain did not finish within 10 minutes" >&2; exit 1; }
    sleep 0.25
  done
  stop_samplers
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
  waits_once
  # Every worker has a stream open before the offered load starts.
  deadline=$((SECONDS + 120))
  # shellcheck disable=SC2086
  until [ "$(compose logs ${services#postgres } 2>/dev/null | grep -c '"lease stream opened"')" -ge "$workers" ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "the workers did not all open lease streams" >&2; exit 1; }
    sleep 0.25
  done
  sql "select pg_stat_statements_reset()" >/dev/null
  pool_snapshot before
  sample & samplers="$!"
  waits & samplers="$samplers $!"
  if [ "${PROFILE:-0}" = 1 ]; then profile & profiler=$!; fi
  loadgen -broker "$brokers_arg" -rate "${RATE:-1000}" -duration "${DURATION:-30s}" \
    -producers "${PRODUCERS:-8}" -batch "${BATCH:-1}" -work-ms "${WORK_MS:-0}" -json >"$dir/loadgen.json"
  stop_samplers
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
stop_samplers

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
# Wait shares are of all active-backend samples; active_mean is per sample.
awk -F'|' '
  $2 == "SAMPLE" { n++; next }
  NF == 4 { c[$2 "|" $3] += $4; t[$2] += $4; total += $4 }
  END {
    printf "samples|%d\nactive_mean|%.2f\n", n, (n ? total / n : 0)
    for (k in t) printf "type|%s|%d|%.1f\n", k, t[k], (total ? 100 * t[k] / total : 0)
    for (k in c) printf "event|%s|%d|%.1f\n", k, c[k], (total ? 100 * c[k] / total : 0)
  }' "$dir/waits.psv" >"$dir/waits.txt"
wait_value() { awk -F'|' -v k="$1" '$1 == k { print $2 }' "$dir/waits.txt"; }
type_pct() { awk -F'|' -v k="$1" '$1 == "type" && $2 == k { print $4; f = 1 } END { if (!f) print "0.0" }' "$dir/waits.txt"; }
samples="$(wait_value samples)"; active_mean="$(wait_value active_mean)"
on_cpu_pct="$(type_pct CPU)"; lock_pct="$(type_pct Lock)"; lwlock_pct="$(type_pct LWLock)"; io_pct="$(type_pct IO)"
# The lease statement (internal/store/postgres/lease.go), found by its text.
IFS='|' read -r lease_mean lease_blks <<<"$(sql "
  select round((sum(total_exec_time) / nullif(sum(calls), 0))::numeric, 3),
         round(sum(shared_blks_hit + shared_blks_read)::numeric / nullif(sum(calls), 0), 1)
  from pg_stat_statements
  where ltrim(regexp_replace(query, '\s+', ' ', 'g')) like 'with next as%'")"
# Mean CPU per role, in percent of one core.
cpu() { awk -F, -v re="$1" '$1 ~ re { gsub("%", "", $2); s[$1] += $2; n[$1]++ }
  END { t = 0; for (c in s) t += s[c] / n[c]; printf "%.0f", t }' "$dir/stats.csv"; }
pg_cpu="$(cpu 'postgres')"; broker_cpu="$(cpu 'broker')"; worker_cpu="$(cpu 'worker')"
pool="${DB_MAX_CONNS:-default}"

# A run that isn't fully measured never becomes a row.
reject() {
  echo "$1" >"$dir/rejected.txt"
  echo "rejected, no row written: $1 ($dir)" >&2
  exit 1
}
[ "${samples:-0}" -gt 0 ] || reject "no wait-event samples"
grep -q broker "$dir/stats.csv" 2>/dev/null || reject "no broker CPU samples"
[ -n "$lease_mean" ] || reject "the lease statement is missing from pg_stat_statements"
if [ "$scenario" = drain ] && [ "$retried" != 0 ]; then reject "$retried jobs were retried"; fi
row="$label,$scenario,$sha,$brokers,$workers,$concurrency,$pool,$HOPPER_BENCH_SYNC_COMMIT,$vm_cpus,$pin,$throughput,$retried,$enqueue_p50,$enqueue_p99,$pickup_p50,$pickup_p99,$e2e_p50,$e2e_p99,$offered,$max_lag,$kept_up,$pg_cpu,$broker_cpu,$worker_cpu,$active_mean,$on_cpu_pct,$lock_pct,$lwlock_pct,$io_pct,$lease_mean,$lease_blks,$dir"
[ -f "$out/results.csv" ] || echo "$header" >"$out/results.csv"
echo "$row" >>"$out/results.csv"
echo "$header"
echo "$row"
