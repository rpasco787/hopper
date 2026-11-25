#!/usr/bin/env bash
# Runs one benchmark against a fresh Compose project, hopper-bench, and keeps
# the evidence:
#
#   scripts/bench.sh drain    # preload JOBS, start the workers, time the drain
#   scripts/bench.sh steady   # workers up; loadgen offers RATE jobs/s for DURATION
#
# Settings, all from the environment:
#   REF            build broker and worker from this git ref (default: the working tree)
#   BROKERS        1, 2 or 4 (default 1); workers split evenly, one worker service per broker, and producers alternate
#   WORKERS        worker containers (default 4), CONCURRENCY slots each (default 16)
#   WORKER         go (default) runs cmd/worker; java runs the Java example worker
#                  (java/hopper-example-worker), built from REF's java/ (deploy/bench/java.yaml)
#   WORKER_MEM     every worker container's memory limit, with no swap beyond it, e.g. 384m
#                  (default none). A Java worker's heap is 75% of it (-XX:MaxRAMPercentage=75)
#   WORKER_JVM     WORKER=java: the JVM's flags, space-separated (default -XX:+UseSerialGC)
#   JOBS           drain: jobs to preload (default 40000)
#   WARM_JOBS      drain: also report warm_throughput, over the jobs that finished after
#                  the WARM_JOBS-th completion (default 0: not reported)
#   RATE DURATION PRODUCERS BATCH WORK_MS
#                  steady: the offered load (default 1000 jobs/s, 30s, 8, 1, 0). WORK_MS
#                  also sets a drain's preloaded handler time
#   WARMUP         steady: seconds of the same load, under its own run id, before the
#                  measured run (default 0)
#   DB_MAX_CONNS   the brokers' pool size; unset keeps the binary's default
#   SYNC_COMMIT    PostgreSQL synchronous_commit (default on; off is a diagnostic)
#   PG_ARGS        extra server settings as name=value words, e.g. "commit_delay=200 commit_siblings=5"
#   EXPLAIN        1 logs a sample of executed plans with buffer counts (auto_explain) to
#                  logs.txt. A diagnostic: its overhead means the run's numbers are not a result
#   BENCH_SQL      SQL run once the brokers have migrated and before the preload
#   PAYLOAD_BYTES  pad every job's payload to at least this many bytes (default 0)
#   VARIANT        free text naming what this run changes (default empty). Rows
#                  group by it, so an A/B on one build is two variants
#   BROKER_ENV WORKER_ENV
#                  space-separated NAME=value settings for every broker or worker,
#                  e.g. WORKER_ENV="HOPPER_LOG_LEVEL=info". They override the stack's
#                  own (settings.yaml), and a run stops if Compose wouldn't apply one
#   TRACE          1 samples cumulative lease calls and time, and dead tuples, every
#                  TRACE_EVERY seconds (default 10) into trace.psv, to see drift
#   STATS_SETTLE   seconds to wait before each snapshot of cumulative statistics (default 11)
#   PROFILE        1 takes a CPU profile of `broker` mid-run (REF must have -debug-listen)
#   PIN            1 pins each role to its own Docker VM CPUs (needs 8; see the layout below)
#   JFR            WORKER=java drains: a JFR recording (settings=profile, pinned virtual
#                  threads at any duration) of this many seconds from each JVM's start,
#                  copied to <worker>.jfr JFR_SETTLE (default 5) seconds after it ends
#   WORKER_PPROF   WORKER=go drains: this many seconds after the workers start, the first
#                  worker's heap and goroutine profiles and a CPU profile of
#                  WORKER_PPROF_SECONDS (default 5); needs a cmd/worker with -debug-listen
#   OUT            evidence root (default ${TMPDIR:-/tmp}/hopper-bench); LABEL names the row
#   KEEP           1 leaves the stack up afterwards
#
# Prints one CSV row and appends it to $OUT/results.csv. Nothing else may use
# Docker heavily during a run: stop a kind cluster or chaos project first.
#
# Each run keeps machine.txt (the Docker VM), waits.psv/waits.txt (active
# PostgreSQL backends by wait event, once a second) and, with PIN=1,
# cpuset.yaml. For the workers it keeps proc.txt (each worker process's /proc
# stat and status at the measured interval's ends, read from the VM's PID
# namespace), workers.json (their docker inspect, trimmed), brokers.log (the
# brokers' lines, for startup) and <worker>.stdout and .stderr;
# scripts/bench_workers.py turns them into the worker columns. A run with no
# wait samples, no broker CPU samples, no lease statement, (drain) any retried
# job, or a worker that exited, was OOM-killed or has no /proc statistics
# writes rejected.txt and no row. Summarize a results.csv with
# scripts/bench_summary.py.
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
case "$brokers" in 1 | 2 | 4) ;; *) echo "BROKERS must be 1, 2 or 4" >&2; exit 2 ;; esac
pin="${PIN:-0}"
case "$pin" in 0 | 1) ;; *) echo "PIN must be 0 or 1" >&2; exit 2 ;; esac
worker_sdk="${WORKER:-go}"
case "$worker_sdk" in go | java) ;; *) echo "WORKER must be go or java" >&2; exit 2 ;; esac
worker_mem="${WORKER_MEM:-}"
mem_bytes=0
if [ -n "$worker_mem" ]; then
  [[ "$worker_mem" =~ ^[1-9][0-9]*[kmg]$ ]] || { echo "WORKER_MEM must be a size such as 384m or 1g" >&2; exit 2; }
  n="${worker_mem%?}"
  case "$worker_mem" in
    *k) mem_bytes=$((n * 1024)) ;;
    *m) mem_bytes=$((n * 1048576)) ;;
    *g) mem_bytes=$((n * 1073741824)) ;;
  esac
fi
java_flags="${WORKER_JVM:--XX:+UseSerialGC}"
if [ -n "${WORKER_JVM:-}" ]; then
  [ "$worker_sdk" = java ] || { echo "WORKER_JVM applies to WORKER=java only" >&2; exit 2; }
  case "$WORKER_JVM" in *[\"\'\\]*) echo "WORKER_JVM may not contain quotes or backslashes" >&2; exit 2 ;; esac
fi
java_opts="$java_flags"
jobs="${JOBS:-40000}"
for knob in JOBS WARM_JOBS WARMUP WORK_MS JFR JFR_SETTLE WORKER_PPROF WORKER_PPROF_SECONDS; do
  case "${!knob:-0}" in *[!0-9]*) echo "$knob must be a whole number" >&2; exit 2 ;; esac
done
if [ -n "${JFR:-}" ]; then
  { [ "$worker_sdk" = java ] && [ "$scenario" = drain ]; } || { echo "JFR applies to WORKER=java drains only" >&2; exit 2; }
  [ "$JFR" -gt 0 ] || { echo "JFR must be at least 1 second" >&2; exit 2; }
  # A fixed duration, because a recording left for exit is lost: the example
  # worker halts in its shutdown hook. Pinned threads at any duration (JFR's
  # default threshold is 20 ms); the startup lines off, so stdout stays JSON.
  java_opts="$java_opts -XX:StartFlightRecording=filename=/tmp/hopper.jfr,settings=profile,duration=${JFR}s,jdk.VirtualThreadPinned#threshold=0ms -Xlog:jfr+startup=off"
fi
if [ -n "${WORKER_PPROF:-}" ]; then
  { [ "$worker_sdk" = go ] && [ "$scenario" = drain ]; } || { echo "WORKER_PPROF applies to WORKER=go drains only" >&2; exit 2; }
  # cmd/worker serves its profiles only when asked (docs/semantics.md §14).
  WORKER_ENV="${WORKER_ENV:+$WORKER_ENV }HOPPER_DEBUG_LISTEN=0.0.0.0:6060"
fi
if [ "${WARM_JOBS:-0}" != 0 ]; then
  [ "$scenario" = drain ] || { echo "WARM_JOBS applies to drain only" >&2; exit 2; }
  [ "$WARM_JOBS" -lt "$jobs" ] || { echo "WARM_JOBS must be less than JOBS" >&2; exit 2; }
fi
if [ "${WARMUP:-0}" != 0 ] && [ "$scenario" != steady ]; then
  echo "WARMUP applies to steady only" >&2; exit 2
fi
payload_bytes="${PAYLOAD_BYTES:-0}"
variant="${VARIANT:-}"
case "$variant" in *,*) echo "VARIANT may not contain a comma" >&2; exit 2 ;; esac
for setting in BROKER_ENV WORKER_ENV; do
  # shellcheck disable=SC2086 # one word per setting
  for kv in ${!setting:-}; do
    case "$kv" in [A-Z_]*=*) ;; *) echo "$setting entries must be NAME=value: $kv" >&2; exit 2 ;; esac
    case "${kv%%=*}" in *[!A-Z0-9_]*) echo "$setting entries must be NAME=value: $kv" >&2; exit 2 ;; esac
    case "$kv" in *[\"\\]*) echo "$setting entries may not contain quotes or backslashes: $kv" >&2; exit 2 ;; esac
  done
done
# shellcheck disable=SC2086
for kv in ${PG_ARGS:-}; do
  case "$kv" in *=*) ;; *) echo "PG_ARGS entries must be name=value: $kv" >&2; exit 2 ;; esac
  case "$kv" in *\"*) echo "PG_ARGS entries may not contain quotes: $kv" >&2; exit 2 ;; esac
done
# Rows group by their columns, and these settings aren't columns, so a run
# that changes one must name itself or the summary would pool it with
# default runs.
if [ -z "$variant" ]; then
  for knob in BROKER_ENV WORKER_PPROF WORKER_ENV PG_ARGS BENCH_SQL WORKER_JVM; do
    [ -z "${!knob:-}" ] || { echo "$knob changes the run: name it with VARIANT" >&2; exit 2; }
  done
  [ "${WARMUP:-0}" = 0 ] || { echo "WARMUP changes the run: name it with VARIANT" >&2; exit 2; }
  if [ "$scenario" = drain ] && [ "${WORK_MS:-0}" != 0 ]; then
    echo "WORK_MS changes the drain: name it with VARIANT" >&2; exit 2
  fi
  [ "${EXPLAIN:-0}" != 1 ] || { echo "EXPLAIN changes the run: name it with VARIANT" >&2; exit 2; }
  [ "${JFR:-0}" = 0 ] || { echo "JFR changes the run: name it with VARIANT" >&2; exit 2; }
fi
# The instruments (this script, loadgen and the Compose files) always come
# from the working tree, whatever REF says, so each row names their revision
# too. Evidence and notes under docs/ don't count.
harness="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- . ':(exclude)docs' || echo -dirty)"
vm_cpus="$(docker info --format '{{.NCPU}}')"
if [ "$pin" = 1 ] && [ "$vm_cpus" -lt 8 ]; then
  echo "PIN=1 needs a Docker VM with at least 8 CPUs; this one has $vm_cpus" >&2
  exit 2
fi

# Images. A ref is built from `git archive`, so uncommitted files never leak
# into it, and its images are reused across runs; the working tree is rebuilt
# every time. The Java worker builds from java/ alone, as its Dockerfile asks.
go_bins="broker"
if [ "$worker_sdk" = go ]; then go_bins="broker worker"; fi
if [ -n "${REF:-}" ]; then
  sha="$(git rev-parse --short "${REF}^{commit}")"
  tag="ref-$sha"
  for bin in $go_bins; do
    if ! docker image inspect "hopper-bench/$bin:$tag" >/dev/null 2>&1; then
      git archive --format=tar "$sha" |
        docker build -q -f deploy/docker/Dockerfile --build-arg BINARY="$bin" -t "hopper-bench/$bin:$tag" - >/dev/null
    fi
  done
  if [ "$worker_sdk" = java ] && ! docker image inspect "hopper-bench/worker-java:$tag" >/dev/null 2>&1; then
    git cat-file -e "$sha:java/hopper-example-worker/Dockerfile" 2>/dev/null ||
      { echo "$sha has no java/hopper-example-worker to build" >&2; exit 2; }
    git archive --format=tar "$sha:java" |
      docker build -q -f hopper-example-worker/Dockerfile -t "hopper-bench/worker-java:$tag" - >/dev/null
  fi
else
  sha="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- . || echo -dirty)"
  tag="wt" # rebuilt every run; the previous build is left dangling
  for bin in $go_bins; do
    docker build -q -f deploy/docker/Dockerfile --build-arg BINARY="$bin" -t "hopper-bench/$bin:$tag" . >/dev/null
  done
  if [ "$worker_sdk" = java ]; then
    docker build -q -f java/hopper-example-worker/Dockerfile -t "hopper-bench/worker-java:$tag" java >/dev/null
  fi
fi
# loadgen is the measuring instrument, so it always comes from the working tree.
docker build -q -f deploy/docker/Dockerfile --build-arg BINARY=loadgen -t hopper-bench/loadgen:wt . >/dev/null

out="${OUT:-${TMPDIR:-/tmp}/hopper-bench}"
header="label,scenario,ref,brokers,workers,concurrency,db_max_conns,sync_commit,vm_cpus,pinned,throughput,retried,enqueue_p50_ms,enqueue_p99_ms,pickup_p50_ms,pickup_p99_ms,e2e_p50_ms,e2e_p99_ms,offered,max_lag_ms,kept_up,postgres_cpu,broker_cpu,worker_cpu,active_backends,on_cpu_pct,lock_pct,lwlock_pct,io_pct,lease_mean_ms,lease_blks_per_call,variant,payload_bytes,harness,commits_per_job,wal_syncs_per_job,wal_kb_per_job,hot_update_pct,dead_tuples,ack_p99_ms,heartbeat_p99_ms,ack_batch,ack_fallback_pct,heartbeat_batch,heartbeat_fallback_pct,worker_sdk,worker_mem,warm_throughput,max_leased,worker_cpu_ms_per_1k,worker_peak_rss_mib,worker_threads,startup_ms,evidence"
if [ -f "$out/results.csv" ] && [ "$(head -n 1 "$out/results.csv")" != "$header" ]; then
  echo "$out/results.csv has a different header; use a new OUT" >&2
  exit 2
fi
dir="$out/$scenario-b$brokers-w$workers-c$vm_cpus-p$pin-$sha-$(date +%Y%m%dT%H%M%S)"
mkdir -p "$dir"
files=(-f docker-compose.yml -f deploy/bench/compose.yaml)
if [ "$worker_sdk" = java ]; then files+=(-f deploy/bench/java.yaml); fi
if [ "$pin" = 1 ]; then
  cat >"$dir/cpuset.yaml" <<'EOF'
# Written by scripts/bench.sh for PIN=1. Each role gets its own VM CPUs, and
# the layout is the same at every broker count, so only the broker count
# changes. Every broker may use CPUs 2 and 3, so the brokers' CPU budget is
# two CPUs however many there are; Go sizes GOMAXPROCS from the set.
services:
  postgres: {cpuset: "0-1"}
  broker: {cpuset: "2-3"}
  broker-standby: {cpuset: "2-3"}
  broker-3: {cpuset: "2-3"}
  broker-4: {cpuset: "2-3"}
  worker: {cpuset: "4-6"}
  worker-b: {cpuset: "4-6"}
  worker-c: {cpuset: "4-6"}
  worker-d: {cpuset: "4-6"}
  loadgen: {cpuset: "7"}
EOF
  files+=(-f "$dir/cpuset.yaml")
fi
# Throwaway helper containers (the /proc reader, profile fetches) run on
# loadgen's CPU under PIN=1, away from every measured role.
helper=(--rm)
if [ "$pin" = 1 ]; then helper+=(--cpuset-cpus 7); fi
# The server's command line for this run, kept as evidence. It replaces the
# overlay's command, which stays for anyone running the stack by hand. (The
# array is never empty when expanded: bash 3.2, macOS's, treats an empty
# "${a[@]}" as unbound under set -u.)
preload_libs="pg_stat_statements"
if [ "${EXPLAIN:-0}" = 1 ]; then preload_libs="pg_stat_statements,auto_explain"; fi
pg_settings=("shared_preload_libraries=$preload_libs" "synchronous_commit=${SYNC_COMMIT:-on}")
if [ "${EXPLAIN:-0}" = 1 ]; then
  pg_settings+=(auto_explain.log_min_duration=0 auto_explain.sample_rate=0.002
    auto_explain.log_analyze=on auto_explain.log_buffers=on auto_explain.log_timing=off)
fi
# shellcheck disable=SC2086
for kv in ${PG_ARGS:-}; do pg_settings+=("$kv"); done
{
  echo "# Written by scripts/bench.sh: the server settings for this run."
  echo "services:"
  echo "  postgres:"
  printf '    command: ["postgres"'
  for kv in "${pg_settings[@]}"; do printf ', "-c", "%s"' "$kv"; done
  echo ']'
} >"$dir/postgres.yaml"
files+=(-f "$dir/postgres.yaml")
# One memory limit for every worker container, with no swap beyond it, so both
# SDKs run under the same ceiling and a Java heap is sized from it.
if [ -n "$worker_mem" ]; then
  {
    echo "# Written by scripts/bench.sh: WORKER_MEM for every worker service, swap included."
    echo "services:"
    for svc in worker worker-b worker-c worker-d; do
      printf '  %s: {mem_limit: %s, memswap_limit: %s}\n' "$svc" "$worker_mem" "$worker_mem"
    done
  } >"$dir/workers-mem.yaml"
  files+=(-f "$dir/workers-mem.yaml")
fi
# Per-run settings, as a Compose file of their own merged after the bench
# overlay. Compose prefers a service's environment to its env_file, and a
# later file's environment to an earlier one's, so this is the one place a
# run can override what the stack already sets (the workers' log level, say).
bench_services="broker broker-standby broker-3 broker-4 worker worker-b worker-c worker-d"
settings_for() { case "$1" in broker*) echo "${BROKER_ENV:-}" ;; *) echo "${WORKER_ENV:-}" ;; esac; }
if [ -n "${BROKER_ENV:-}${WORKER_ENV:-}" ]; then
  {
    echo "# Written by scripts/bench.sh: the settings this run overrides."
    echo "services:"
    for svc in $bench_services; do
      kvs="$(settings_for "$svc")"
      [ -n "$kvs" ] || continue
      printf '  %s:\n    environment:\n' "$svc"
      # shellcheck disable=SC2086 # one word per setting
      for kv in $kvs; do printf '      %s: "%s"\n' "${kv%%=*}" "${kv#*=}"; done
    done
  } >"$dir/settings.yaml"
  files+=(-f "$dir/settings.yaml")
fi
{
  echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
  echo "docker: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes, server {{.ServerVersion}}')"
  echo "pinned: $pin"
  echo "worker: $worker_sdk"
  echo "worker memory: ${worker_mem:-none}"
  if [ "$worker_sdk" = java ]; then echo "java opts: $java_opts"; fi
  if [ "$scenario" = drain ]; then echo "warm jobs: ${WARM_JOBS:-0}"; else echo "warmup: ${WARMUP:-0}s"; fi
} >"$dir/machine.txt"
if [ "$worker_sdk" = java ]; then
  # The JVM's own account of its heap, collector and CPUs, through the image's
  # start script, under the workers' memory limit and CPU set.
  probe=(--rm)
  if [ -n "$worker_mem" ]; then probe+=(--memory "$worker_mem" --memory-swap "$worker_mem"); fi
  if [ "$pin" = 1 ]; then probe+=(--cpuset-cpus 4-6); fi
  docker run "${probe[@]}" -e "JAVA_OPTS=$java_flags -XX:+PrintFlagsFinal -version" "hopper-bench/worker-java:$tag" 2>&1 |
    grep -E ' (MaxHeapSize|InitialHeapSize|MaxRAMPercentage|UseSerialGC|UseG1GC|UseParallelGC|UseZGC|ActiveProcessorCount) |version' \
      >"$dir/jvm-flags.txt" || true
fi
label="${LABEL:-$scenario $sha}"
: >"$dir/broker.env"
[ -n "${DB_MAX_CONNS:-}" ] && echo "HOPPER_DB_MAX_CONNS=$DB_MAX_CONNS" >>"$dir/broker.env"

export HOPPER_BENCH_TAG="$tag" HOPPER_BENCH_BROKER_ENV="$dir/broker.env"
export HOPPER_BENCH_SYNC_COMMIT="${SYNC_COMMIT:-on}" HOPPER_BENCH_CONCURRENCY="$concurrency"
export HOPPER_BENCH_JAVA_OPTS="$java_opts"
compose() { docker compose -p hopper-bench "${files[@]}" "$@"; }
sql() { compose exec -T postgres psql -U hopper -d hopper -qAt -c "$1"; }
# Commits and WAL across the database, and how updates landed on jobs, as
# xact_commit|wal_sync|wal_bytes|n_tup_upd|n_tup_hot_upd|n_dead_tup.
counters() {
  sql "select d.xact_commit, w.wal_sync, w.wal_bytes, t.n_tup_upd, t.n_tup_hot_upd, t.n_dead_tup
       from pg_stat_database d, pg_stat_wal w, pg_stat_user_tables t
       where d.datname = current_database() and t.relname = 'jobs'"
}
# Backends publish statistics on their own schedule, and an idle one can hold
# them for up to 10 seconds (PGSTAT_IDLE_INTERVAL). Waiting this long before
# each snapshot keeps the preload out of the first and the run's tail in the
# second.
settle_stats() { sleep "${STATS_SETTLE:-11}"; }

# Read every per-run setting back from Compose's merged model, so one Compose
# would not apply stops the run instead of being silently ignored.
if [ -f "$dir/settings.yaml" ]; then
  model="$(compose config --format json)"
  for svc in $bench_services; do
    # shellcheck disable=SC2086
    for kv in $(settings_for "$svc"); do
      got="$(jq -r --arg s "$svc" --arg k "${kv%%=*}" '.services[$s].environment[$k] // empty' <<<"$model")"
      [ "$got" = "${kv#*=}" ] || { echo "$svc would run with ${kv%%=*}=${got:-unset}, not ${kv#*=}" >&2; exit 2; }
    done
  done
fi

samplers=""
profiler=""
warm_throughput=""
pprofer=""
jfr_missing=""
max_leased=""
cleanup() {
  stop_samplers
  compose logs --no-color >"$dir/logs.txt" 2>&1 || true
  [ "${KEEP:-0}" = 1 ] || compose down -v --remove-orphans -t 10 >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose down -v --remove-orphans >/dev/null 2>&1 || true
case "$brokers" in
1) broker_services="broker" ;;
2) broker_services="broker broker-standby" ;;
4) broker_services="broker broker-standby broker-3 broker-4" ;;
esac
services="postgres $broker_services"
# shellcheck disable=SC2086 # word splitting is the point
compose up -d --no-build --wait $services >/dev/null
sql "create extension pg_stat_statements" >/dev/null
# The brokers migrate on start, and nothing guarantees they are done when
# Compose says they are running.
deadline=$((SECONDS + 60))
until [ "$(sql "select to_regclass('public.jobs') is not null")" = t ]; do
  [ "$SECONDS" -lt "$deadline" ] || { echo "the brokers did not migrate within a minute" >&2; exit 1; }
  sleep 0.25
done
if [ -n "${BENCH_SQL:-}" ]; then
  printf '%s\n' "$BENCH_SQL" >"$dir/bench.sql"
  sql "$BENCH_SQL" >/dev/null
fi

# Workers split evenly across the brokers, the first services taking any
# remainder. Worker service i dials broker i, so the split is exact.
worker_services=(worker worker-b worker-c worker-d)
scale=()
for i in 0 1 2 3; do
  n=0
  if [ "$i" -lt "$brokers" ]; then n=$(((workers + brokers - 1 - i) / brokers)); fi
  scale+=(--scale "${worker_services[$i]}=$n")
done
start_workers() {
  compose up -d --no-build --no-deps "${scale[@]}" "${worker_services[@]}" >/dev/null
}
# Worker containers, stopped ones too, so a worker that died is still judged.
worker_ids() { compose ps -a -q "${worker_services[@]}"; }
# /hopper-bench-worker-b-1 -> worker-b-1
container_name() {
  local n
  n="$(docker inspect --format '{{.Name}}' "$1")"
  echo "${n#/hopper-bench-}"
}
# Every worker process's /proc/<pid>/stat and status, read from the Docker VM's
# PID namespace by a throwaway container of the stack's own PostgreSQL image,
# each pair after a line "== <label> <container> <pid>". stat's utime and stime
# count every thread of the process, a JVM's compiler and collector threads too.
proc_snapshot() { # label
  local pairs
  # shellcheck disable=SC2046 # one word per container
  pairs="$(docker inspect --format '{{.Id}} {{.State.Pid}}' $(worker_ids))" || return 0
  # shellcheck disable=SC2016,SC2086 # $1 and $2 are the helper's own; one word per id and pid
  docker run "${helper[@]}" --pid=host --entrypoint sh postgres:16-alpine -c '
    label=$1; shift
    echo "clk_tck $(getconf CLK_TCK)"
    while [ $# -ge 2 ]; do echo "== $label $1 $2"; cat "/proc/$2/stat" "/proc/$2/status"; shift 2; done
  ' snapshot "$1" $pairs >>"$dir/proc.txt" 2>&1 || true
}
# WORKER_PPROF seconds after the workers start: the first worker's heap and
# goroutine profiles (text, ending with the runtime's memory statistics) and a
# CPU profile, fetched over the Compose network by a throwaway container.
worker_pprof() {
  sleep "$WORKER_PPROF"
  local id host net
  id="$(compose ps -q worker | head -n 1)"
  host="$(docker inspect --format '{{.Name}}' "$id")"
  host="${host#/}"
  net="$(docker inspect --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' "$id" | head -n 1)"
  fetch() {
    docker run "${helper[@]}" --network "$net" --entrypoint wget postgres:16-alpine -qO- "http://$host:6060/debug/pprof/$1"
  }
  fetch 'heap?debug=1' >"$dir/worker-heap.txt" &&
    fetch 'goroutine?debug=1' >"$dir/worker-goroutine.txt" &&
    fetch "profile?seconds=${WORKER_PPROF_SECONDS:-5}" >"$dir/worker-cpu.pprof" ||
    echo "no worker profile: does $sha's cmd/worker have -debug-listen?" >&2
}
brokers_arg="broker:9090"; metrics_ports="16091"
if [ "$brokers" -ge 2 ]; then brokers_arg="$brokers_arg,broker-standby:9090"; metrics_ports="$metrics_ports 16092"; fi
if [ "$brokers" = 4 ]; then
  brokers_arg="$brokers_arg,broker-3:9090,broker-4:9090"; metrics_ports="$metrics_ports 16093 16094"
fi
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
# time|lease calls|lease total ms|n_dead_tup|n_live_tup|autovacuum_count
trace_once() {
  sql "select to_char(clock_timestamp(), 'HH24:MI:SS'), s.calls, round(s.total::numeric, 1),
              t.n_dead_tup, t.n_live_tup, t.autovacuum_count
       from (select coalesce(sum(calls), 0) as calls, coalesce(sum(total_exec_time), 0) as total
             from pg_stat_statements
             where ltrim(regexp_replace(query, '\s+', ' ', 'g')) like 'with next as%') s,
            pg_stat_user_tables t where t.relname = 'jobs'" >>"$dir/trace.psv" || true
}
# The first sample is taken at once, so a run has its starting point and a
# short run still leaves one row.
tracer() {
  trace_once
  until [ -e "$dir/samplers.stop" ]; do
    sleep "${TRACE_EVERY:-10}"; [ -e "$dir/samplers.stop" ] || trace_once
  done
}
# Every broker's pool counters and confirmation latency histograms, before and
# after the measured interval, so their deltas leave out the preload and
# startup. A failed scrape is written down, not skipped: bench_hist.py then
# rejects the measurement rather than summing fewer brokers.
pool_snapshot() {
  for port in $metrics_ports; do
    metrics="$(curl -sf "http://127.0.0.1:$port/metrics")" || metrics="# scrape failed"
    { echo "# $1 :$port"
      grep -E '^hopper_db_pool|^# scrape failed' <<<"$metrics" || echo "# no pool metrics"
    } >>"$dir/pool.txt"
    { echo "# $1 :$port"
      grep -E '^grpc_server_handling_seconds_bucket\{.*grpc_method="(Ack|Heartbeat)"|^# scrape failed' <<<"$metrics" || true
    } >>"$dir/rpc.txt"
  done
}
profile() {
  sleep 3
  curl -sf -o "$dir/broker-cpu.pprof" "http://127.0.0.1:16060/debug/pprof/profile?seconds=${PROFILE_SECONDS:-5}" ||
    echo "no profile: does $sha have -debug-listen?" >&2
}

case "$scenario" in
drain)
  loadgen -broker "$brokers_arg" -n "$jobs" -batch 500 -producers 4 -payload-bytes "$payload_bytes" \
    -work-ms "${WORK_MS:-0}" -wait=false -json >"$dir/preload.json"
  sql "vacuum analyze jobs" >/dev/null
  settle_stats
  sql "select pg_stat_statements_reset()" >/dev/null
  counters_before="$(counters)"
  sql "select current_setting('fsync') || '|' || current_setting('wal_sync_method')" >"$dir/wal_sync.txt"
  pool_snapshot before
  sample_once
  waits_once
  sample & samplers="$!"
  waits & samplers="$samplers $!"
  if [ "${TRACE:-0}" = 1 ]; then tracer & samplers="$samplers $!"; fi
  workers_started=$SECONDS
  start_workers
  if [ "${PROFILE:-0}" = 1 ]; then profile & profiler=$!; fi
  if [ -n "${WORKER_PPROF:-}" ]; then worker_pprof & pprofer=$!; fi
  # Progress four times a second, and the rows leased at that moment: at most
  # the workers' slots, because the broker leases only against credit a worker
  # sent for a free slot (docs/design/credit-anchored-freshness.md).
  max_leased=0
  deadline=$((SECONDS + 600))
  while :; do
    IFS='|' read -r done_jobs leased <<<"$(sql "select count(*) filter (where state = 'completed'),
      count(*) filter (where state = 'leased') from jobs")"
    if [ "${leased:-0}" -gt "$max_leased" ]; then max_leased="$leased"; fi
    [ "${done_jobs:-0}" -lt "$jobs" ] || break
    [ "$SECONDS" -lt "$deadline" ] || { echo "drain did not finish within 10 minutes" >&2; exit 1; }
    sleep 0.25
  done
  proc_snapshot end
  stop_samplers
  pool_snapshot after
  if [ -n "$profiler" ]; then wait "$profiler" || true; fi
  if [ -n "$pprofer" ]; then wait "$pprofer" || true; fi
  if [ -n "${JFR:-}" ]; then
    # Each recording is written when it ends, JFR seconds after its JVM started.
    until [ $((SECONDS - workers_started)) -ge $((JFR + ${JFR_SETTLE:-5})) ]; do sleep 1; done
    for id in $(worker_ids); do
      name="$(container_name "$id")"
      docker cp "$id:/tmp/hopper.jfr" "$dir/$name.jfr" >/dev/null 2>&1 || true
      [ -s "$dir/$name.jfr" ] || jfr_missing="${jfr_missing:+$jfr_missing }$name"
    done
  fi
  # The drain runs from the first lease to the last completion, on the
  # database's clock; every job must have completed on its first lease.
  IFS=, read -r throughput retried <<<"$(sql "
    select round(count(*) / extract(epoch from max(finished_at) - min(leased_at))),
           count(*) filter (where attempts > 1)
    from jobs" | tr '|' ',')"
  # Completions per tenth of a second from the first lease: how long the
  # workers take to reach their pace. The first lease is a scalar subquery,
  # which runs once; as a join, planned on statistics from before the drain
  # (no completed rows), it became a nested loop that rescanned jobs per job.
  {
    echo "# tenths of a second since the first lease|jobs completed in it"
    sql "select floor(extract(epoch from finished_at - (select min(leased_at) from jobs)) * 10)::int, count(*)
         from jobs where state = 'completed' group by 1 order by 1"
  } >"$dir/rate.psv"
  if [ "${WARM_JOBS:-0}" != 0 ]; then
    # The pace once warm: the jobs that finished after the WARM_JOBS-th
    # completion, over the time from it to the last.
    warm_throughput="$(sql "select round((count(*) - 1) / nullif(extract(epoch from max(finished_at) - min(finished_at)), 0))
      from (select finished_at from jobs where state = 'completed' order by finished_at offset $((WARM_JOBS - 1))) w")"
  fi
  completed="$jobs"
  enqueue_p50=""; enqueue_p99=""; pickup_p50=""; pickup_p99=""; e2e_p50=""; e2e_p99=""
  offered=""; max_lag=""; kept_up=""
  ;;
steady)
  workers_started=$SECONDS
  start_workers
  waits_once
  # Every worker has a stream open before the offered load starts.
  deadline=$((SECONDS + 120))
  # shellcheck disable=SC2086
  until [ "$(compose logs ${services#postgres } 2>/dev/null | grep -c '"lease stream opened"')" -ge "$workers" ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "the workers did not all open lease streams" >&2; exit 1; }
    sleep 0.25
  done
  if [ "${WARMUP:-0}" != 0 ]; then
    # The same load first, under its own run id, so the measured run meets warm
    # workers: compiled, with pools and connections open. loadgen reads back
    # only its own run's jobs, and every count below starts after this.
    loadgen -broker "$brokers_arg" -rate "${RATE:-1000}" -duration "${WARMUP}s" -run warmup \
      -producers "${PRODUCERS:-8}" -batch "${BATCH:-1}" -work-ms "${WORK_MS:-0}" -payload-bytes "$payload_bytes" \
      -json >"$dir/warmup.json"
  fi
  settle_stats
  sql "select pg_stat_statements_reset()" >/dev/null
  counters_before="$(counters)"
  completed_before="$(sql "select count(*) from jobs where state = 'completed'")"
  sql "select current_setting('fsync') || '|' || current_setting('wal_sync_method')" >"$dir/wal_sync.txt"
  pool_snapshot before
  proc_snapshot before
  sample & samplers="$!"
  waits & samplers="$samplers $!"
  if [ "${TRACE:-0}" = 1 ]; then tracer & samplers="$samplers $!"; fi
  if [ "${PROFILE:-0}" = 1 ]; then profile & profiler=$!; fi
  loadgen -broker "$brokers_arg" -rate "${RATE:-1000}" -duration "${DURATION:-30s}" \
    -producers "${PRODUCERS:-8}" -batch "${BATCH:-1}" -work-ms "${WORK_MS:-0}" -payload-bytes "$payload_bytes" -json >"$dir/loadgen.json"
  stop_samplers
  proc_snapshot after
  pool_snapshot after
  if [ -n "$profiler" ]; then wait "$profiler" || true; fi
  read -r throughput retried enqueue_p50 enqueue_p99 pickup_p50 pickup_p99 e2e_p50 e2e_p99 offered max_lag <<<"$(jq -r '
    [(.throughput | round), .retried, .enqueue.p50_ms, .enqueue.p99_ms, .pickup.p50_ms, .pickup.p99_ms,
     .end_to_end.p50_ms, .end_to_end.p99_ms, .offered_rate, (.max_lag_ms | round)] | @tsv' "$dir/loadgen.json")"
  # Keeping up: at least 98% of offered, no call a second late.
  kept_up="$(awk -v t="$throughput" -v o="$offered" -v l="$max_lag" \
    'BEGIN { print (o > 0 && t >= 0.98 * o && l < 1000) ? "yes" : "no" }')"
  completed=$(($(sql "select count(*) from jobs where state = 'completed'") - completed_before))
  ;;
esac
stop_samplers
settle_stats
counters_after="$(counters)"
printf '%s\n%s\n' "$counters_before" "$counters_after" >"$dir/counters.txt"

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
# Every confirmation statement's exact calls and rows, however small its share
# of time (statements.txt keeps the 15 most expensive), as kind|calls|rows. A
# batch statement's rows are the pairs it applied. The one-row statements are
# its fallbacks with batching on, and every confirmation with it off.
{
  echo "kind|calls|rows"
  sql "select kind, sum(calls), sum(rows) from (
         select case
                  when q like 'with mine as ( select j.id from %' then 'ack_batch'
                  when q like 'with mine as ( select j.id, c.dur_us %' then 'heartbeat_batch'
                  when q like 'update jobs set state = %, finished_at = now(), updated_at = now() where id = % and lease_id = %' then 'ack_one'
                  when q like 'update jobs set lease_expires = now() + %' then 'heartbeat_one'
                  when q like '% for key share' then 'locked_read'
                end as kind, calls, rows
         from (select ltrim(regexp_replace(query, '\s+', ' ', 'g')) as q, calls, rows from pg_stat_statements) s
       ) k where kind is not null group by kind order by kind"
} >"$dir/confirmations.txt"
confirm_calls() { awk -F'|' -v k="$1" '$1 == k { print $2; f = 1 } END { if (!f) print 0 }' "$dir/confirmations.txt"; }

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
[ -n "$counters_before" ] && [ -n "$counters_after" ] || reject "no statistics snapshot"
IFS='|' read -r c0 s0 w0 u0 h0 _ <<<"$counters_before"
IFS='|' read -r c1 s1 w1 u1 h1 dead_tuples <<<"$counters_after"
read -r commits_per_job wal_syncs_per_job wal_kb_per_job hot_update_pct <<<"$(awk \
  -v n="$completed" -v c=$((c1 - c0)) -v s=$((s1 - s0)) -v w=$((w1 - w0)) -v u=$((u1 - u0)) -v h=$((h1 - h0)) \
  'BEGIN { if (n < 1) n = 1; printf "%.2f %.3f %.2f %.1f\n", c / n, s / n, w / n / 1024, (u > 0 ? 100 * h / u : 0) }')"
# wal_sync stays zero unless fsync is on and the method is one it counts.
case "$(cat "$dir/wal_sync.txt")" in
  'on|fdatasync' | 'on|fsync' | 'on|fsync_writethrough') ;;
  *) wal_syncs_per_job="" ;;
esac
# A broker whose snapshots don't pair up makes the run incomplete, not quieter.
hist="$(python3 scripts/bench_hist.py "$dir/rpc.txt" Ack 2>&1)" || reject "$hist"
read -r ack_calls ack_p99 <<<"$hist"
hist="$(python3 scripts/bench_hist.py "$dir/rpc.txt" Heartbeat 2>&1)" || reject "$hist"
read -r heartbeat_calls heartbeat_p99 <<<"$hist"
# Every drained job was acked at least once, so fewer Ack calls than jobs
# means some broker's calls went uncounted.
if [ "$scenario" = drain ] && [ "$ack_calls" -lt "$jobs" ]; then
  reject "the Ack histograms saw $ack_calls calls for $jobs jobs"
fi
# The workers: their logs, how they ran, and what they cost.
for id in $(worker_ids); do
  name="$(container_name "$id")"
  docker logs "$id" >"$dir/$name.stdout" 2>"$dir/$name.stderr" || true
done
# shellcheck disable=SC2046 # one word per container
docker inspect $(worker_ids) | jq '[.[] | {Id, Name, Image,
    Config: {Hostname: .Config.Hostname, Env: .Config.Env},
    State: {StartedAt: .State.StartedAt, Running: .State.Running, OOMKilled: .State.OOMKilled,
            ExitCode: .State.ExitCode, Pid: .State.Pid},
    HostConfig: {Memory: .HostConfig.Memory, MemorySwap: .HostConfig.MemorySwap,
                 CpusetCpus: .HostConfig.CpusetCpus}}]' >"$dir/workers.json" || true
# shellcheck disable=SC2086 # one word per service
compose logs --no-color --no-log-prefix $broker_services >"$dir/brokers.log" 2>&1 || true
w="$(python3 scripts/bench_workers.py "$dir" "$completed" "$mem_bytes" 2>&1)" || reject "$w"
read -r worker_cpu_per_1k worker_peak_rss worker_threads startup_ms <<<"$w"
for v in worker_cpu_per_1k worker_peak_rss worker_threads startup_ms; do
  if [ "${!v}" = - ]; then printf -v "$v" '%s' ''; fi
done
if [ "$scenario" = drain ] && [ "$max_leased" -gt $((workers * concurrency)) ]; then
  reject "$max_leased rows were leased at once, more than the workers' $((workers * concurrency)) slots"
fi
[ -z "$jfr_missing" ] || reject "no JFR recording from $jfr_missing"
# Batch size as submitted: confirmation RPCs per batch statement (a batch's
# rows are only what it applied). Fallback share: one-row statements per RPC.
# Both per kind, and blank when no batch statement ran, as with batching off.
batch_stats() { # RPC calls, batch statements, one-row statements
  awk -v r="$1" -v b="$2" -v o="$3" 'BEGIN { if (b > 0 && r > 0) printf "%.1f %.1f\n", r / b, 100 * o / r; else print "- -" }'
}
read -r ack_batch ack_fallback_pct <<<"$(batch_stats "$ack_calls" "$(confirm_calls ack_batch)" "$(confirm_calls ack_one)")"
read -r heartbeat_batch heartbeat_fallback_pct <<<"$(batch_stats "${heartbeat_calls:-0}" "$(confirm_calls heartbeat_batch)" "$(confirm_calls heartbeat_one)")"
for v in ack_batch ack_fallback_pct heartbeat_batch heartbeat_fallback_pct; do
  if [ "${!v}" = - ]; then printf -v "$v" '%s' ''; fi
done

row="$label,$scenario,$sha,$brokers,$workers,$concurrency,$pool,$HOPPER_BENCH_SYNC_COMMIT,$vm_cpus,$pin,$throughput,$retried,$enqueue_p50,$enqueue_p99,$pickup_p50,$pickup_p99,$e2e_p50,$e2e_p99,$offered,$max_lag,$kept_up,$pg_cpu,$broker_cpu,$worker_cpu,$active_mean,$on_cpu_pct,$lock_pct,$lwlock_pct,$io_pct,$lease_mean,$lease_blks,$variant,$payload_bytes,$harness,$commits_per_job,$wal_syncs_per_job,$wal_kb_per_job,$hot_update_pct,$dead_tuples,$ack_p99,$heartbeat_p99,$ack_batch,$ack_fallback_pct,$heartbeat_batch,$heartbeat_fallback_pct,$worker_sdk,$worker_mem,$warm_throughput,$max_leased,$worker_cpu_per_1k,$worker_peak_rss,$worker_threads,$startup_ms,$dir"
[ -f "$out/results.csv" ] || echo "$header" >"$out/results.csv"
echo "$row" >>"$out/results.csv"
echo "$header"
echo "$row"
