"""Benchmark orchestration regression checks; run with python3 scripts/bench_test.py."""
import csv
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

# Stands in for docker. It answers the harness's SQL by substring and records
# events in BENCH_TEST_TRACE. BENCH_TEST_NCPU sets the VM's CPU count,
# BENCH_TEST_RETRIED the drain's retried jobs, BENCH_TEST_NO_WAITS=1 makes the
# wait-event sampler come back empty, and BENCH_TEST_STATS_DELAY makes stats
# collect for that many seconds before printing, as the real one does.
# The workers are BENCH_TEST_WORKER_IDS (two by default), each with its first
# 12 characters as hostname, as Docker's are. BENCH_TEST_COMPLETED is the
# successive answers to a count of completed jobs (the last repeats),
# BENCH_TEST_LEASED the rows leased now, BENCH_TEST_TICKS_<LABEL> each worker's
# CPU ticks in the /proc snapshot of that label (default 100),
# BENCH_TEST_MEMORY each worker's memory limit in bytes, BENCH_TEST_EXITED=1
# makes the workers exited, and BENCH_TEST_NO_JFR=1 leaves docker cp nothing.
# The warm-throughput query answers its offset plus 21, so a test sees the offset.
FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, re, sys, time
from pathlib import Path
args = sys.argv[1:]
trace = Path(os.environ['BENCH_TEST_TRACE'])
def event(name):
    with trace.open('a') as f: f.write(name + '\n')
def events():
    return trace.read_text().splitlines() if trace.exists() else []
ids = os.environ.get('BENCH_TEST_WORKER_IDS', 'a1b2c3d4e5f6aaaa f6e5d4c3b2a1bbbb').split()
def number(c):
    return ids.index(c) + 1 if c in ids else 0
def completed():
    seq = os.environ.get('BENCH_TEST_COMPLETED', '1').split(',')
    event('completed count')
    return seq[min(events().count('completed count'), len(seq)) - 1]
if args[0] == 'info':
    print(os.environ.get('BENCH_TEST_NCPU', '8'))
elif args[0] == 'stats':
    event('sample')
    delay = float(os.environ.get('BENCH_TEST_STATS_DELAY', '0'))
    time.sleep(delay)
    # With a delay, postgres reads 10%, 20%, ... so a lost sample moves the mean.
    n = trace.read_text().splitlines().count('sample') if delay else 1
    print('hopper-bench-postgres-1,%d%%' % (10 * n))
    print('hopper-bench-broker-1,50%')
    print('hopper-bench-worker-1,20%')
    time.sleep(0.02)
elif args[0] == 'inspect':
    if '--format' in args:
        i = args.index('--format')
        fmt, targets = args[i + 1], args[i + 2:]
        for c in targets:
            if '.State.Pid' in fmt: print(c, 100 + number(c))
            elif 'Networks' in fmt: print('hopper-bench_default')
            elif '.Name' in fmt: print('/hopper-bench-worker-%d' % number(c))
    else:
        exited = os.environ.get('BENCH_TEST_EXITED') == '1'
        print(json.dumps([{
            'Id': c, 'Name': '/hopper-bench-worker-%d' % number(c), 'Image': 'sha256:fake',
            'Config': {'Hostname': c[:12], 'Env': ['HOPPER_LOG_LEVEL=warn']},
            'State': {'StartedAt': '2026-11-25T00:00:00.000000000Z', 'Running': not exited,
                      'OOMKilled': False, 'ExitCode': 3 if exited else 0, 'Pid': 100 + number(c)},
            'HostConfig': {'Memory': int(os.environ.get('BENCH_TEST_MEMORY', '0')), 'MemorySwap': 0,
                           'CpusetCpus': ''},
        } for c in args[1:]]))
elif args[0] == 'run':
    event('docker run: ' + ' '.join(args))
    if '--pid=host' in args:
        i = args.index('snapshot')
        label, pairs = args[i + 1], args[i + 2:]
        ticks = int(os.environ.get('BENCH_TEST_TICKS_' + label.upper(), '100'))
        print('clk_tck 100')
        for c, pid in zip(pairs[0::2], pairs[1::2]):
            print('== %s %s %s' % (label, c, pid))
            print('%s (app) S 1 1 1 0 -1 4194560 10 0 0 0 %d %d 0 0 20 0 9 0 5 0'
                  % (pid, ticks - ticks // 2, ticks // 2))
            print('Name:\tapp\nVmHWM:\t   20480 kB\nVmRSS:\t   19456 kB\nThreads:\t9')
    elif any('PrintFlagsFinal' in a for a in args):
        print('     bool UseSerialGC                              = true                                      {product} {command line}')
    elif 'wget' in args:
        print('fake profile')
elif args[0] == 'cp':
    event('docker cp: ' + ' '.join(args[1:]))
    if os.environ.get('BENCH_TEST_NO_JFR') != '1':
        Path(args[2]).write_text('JFR')
elif args[0] == 'logs':
    print('{"time":"2026-11-25T00:00:01Z","level":"INFO","msg":"worker starting"}')
elif args[0] == 'compose':
    event('compose files: ' + ' '.join(a for a in args if a.endswith(('.yml', '.yaml'))))
    if 'up' in args:
        event('compose up: ' + ' '.join(args))
    if 'up' in args and 'worker-b' in args:
        event('workers consuming')
        time.sleep(0.2)
    elif 'exec' in args:
        sql = args[-1]
        event('sql: ' + ' '.join(sql.split())[:60])
        if "filter (where state = 'completed')" in sql:
            print('%s|%s' % (completed(), os.environ.get('BENCH_TEST_LEASED', '3')))
        elif 'offset' in sql: print(int(re.search(r'offset (\d+)', sql).group(1)) + 21)
        elif 'floor(extract' in sql:
            event('rate sql: ' + ' '.join(sql.split()))
            print('0|5\n1|7')
        elif "where state = 'completed'" in sql: print(completed())
        elif 'to_regclass' in sql: print('t')
        elif 'extract(epoch' in sql: print('100|' + os.environ.get('BENCH_TEST_RETRIED', '0'))
        elif "select 'total|" in sql: print('total|statements=1|calls=1|total_ms=1')
        elif 'pg_stat_wal' in sql:
            event('counters')
            n = trace.read_text().splitlines().count('counters')
            print('%d|%d|%d|%d|%d|%d' % (1000 * n, 100 * n, 4096000 * n, 300 * n, 30 * n, 50 * n))
        elif 'wal_sync_method' in sql: print(os.environ.get('BENCH_TEST_WAL_SYNC', 'on|fdatasync'))
        elif "'ack_batch'" in sql:
            print(os.environ.get('BENCH_TEST_CONFIRMATIONS', 'ack_batch|10|95\nack_one|5|5'))
        elif 'pg_stat_activity' in sql:
            if os.environ.get('BENCH_TEST_NO_WAITS') != '1':
                print('12:00:00.000|SAMPLE|-|0')
                print('12:00:00.000|LWLock|LockManager|1')
                print('12:00:00.000|CPU|running|3')
        elif 'n_live_tup' in sql: print('12:00:00|10|5.0|3|4|0')
        elif 'with next as' in sql: print('0.500|12.0|4.00|250')
    elif 'config' in args:
        services = {}
        path = next((a for a in args if a.endswith('settings.yaml')), None)
        if path and os.environ.get('BENCH_TEST_IGNORE_SETTINGS') != '1':
            for line in Path(path).read_text().splitlines():
                if line.startswith('      '):
                    k, v = line.strip().split(': ', 1)
                    services[svc]['environment'][k] = v.strip('"')
                elif line.startswith('  ') and not line.startswith('   '):
                    svc = line.strip().rstrip(':')
                    services[svc] = {'environment': {}}
        print(json.dumps({'services': services}))
    elif 'ps' in args:
        print('\n'.join(ids))
    elif 'logs' in args:
        for c in ids:
            for _ in range(8):
                print(json.dumps({'time': '2026-11-25T00:00:00.250000000Z', 'level': 'INFO',
                                  'msg': 'lease stream opened', 'worker_id': c[:12] + '-1'}))
    elif 'run' in args:
        event('compose run: ' + ' '.join(args))
        print(json.dumps({'throughput': 1000, 'retried': 0, 'enqueue': {'p50_ms': 1, 'p99_ms': 2},
                          'pickup': {'p50_ms': 1, 'p99_ms': 3}, 'end_to_end': {'p50_ms': 2, 'p99_ms': 4},
                          'offered_rate': 1000, 'max_lag_ms': 5}))
'''

# Stands in for curl. It counts its calls in BENCH_TEST_CURLS and reports Ack
# buckets that grow by BENCH_TEST_ACKS calls (default 100, 90% of them under
# 1 ms) per call. Call number BENCH_TEST_CURL_FAIL fails, as a dead broker would.
FAKE_CURL = r'''#!/bin/sh
n=$(cat "$BENCH_TEST_CURLS" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" >"$BENCH_TEST_CURLS"
[ "$n" = "${BENCH_TEST_CURL_FAIL:-0}" ] && exit 22
a=${BENCH_TEST_ACKS:-100}
echo "hopper_db_pool_acquires_total 1"
b='grpc_server_handling_seconds_bucket{grpc_method="Ack",grpc_service="s",grpc_type="unary",le='
echo "${b}\"0.001\"} $((a * 9 / 10 * n))"
echo "${b}\"0.0025\"} $((a * n))"
echo "${b}\"+Inf\"} $((a * n))"
'''


def run_bench(root, scenario='drain', **overrides):
    """Runs scripts/bench.sh against the fake docker; returns the CompletedProcess."""
    fake = root / 'docker'
    fake.write_text(FAKE_DOCKER)
    fake.chmod(0o755)
    curl = root / 'curl'
    curl.write_text(FAKE_CURL)
    curl.chmod(0o755)
    env = os.environ.copy()
    env.update(PATH=str(root) + os.pathsep + env['PATH'],
               BENCH_TEST_TRACE=str(root / 'trace'), OUT=str(root / 'out'),
               REF='HEAD', JOBS='1', BROKERS='1', WORKERS='4', PROFILE='0', KEEP='0',
               STATS_SETTLE='0', BENCH_TEST_CURLS=str(root / 'curls'))
    env.update(overrides)
    return subprocess.run(['bash', 'scripts/bench.sh', scenario], env=env,
                          capture_output=True, text=True, timeout=30)


def trace(root):
    """Events the fake recorded; none if it never ran a traced command."""
    path = root / 'trace'
    return path.read_text().splitlines() if path.exists() else []


def rows(root):
    path = root / 'out' / 'results.csv'
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def evidence_dir(root):
    """The single run directory under OUT."""
    dirs = [d for d in (root / 'out').iterdir() if d.is_dir()]
    assert len(dirs) == 1, dirs
    return dirs[0]


class BenchmarkTest(unittest.TestCase):
    def test_trace_samples_lease_cost_and_dead_tuples(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, scenario='steady', TRACE='1', TRACE_EVERY='0.05')
            trace_file = evidence_dir(root) / 'trace.psv'
            self.assertTrue(trace_file.exists(), result.stderr)
            self.assertIn('12:00:00|10|5.0|3|4|0', trace_file.read_text())

    def test_row_carries_batch_size_and_fallback_share(self):
        # 100 Ack RPCs (the fake curl), 10 batch statements, 5 one-row Acks.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual((row['ack_batch'], row['ack_fallback_pct']), ('10.0', '5.0'))
            self.assertEqual((row['heartbeat_batch'], row['heartbeat_fallback_pct']), ('', ''))
            self.assertIn('ack_one|5|5', (evidence_dir(root) / 'confirmations.txt').read_text())

    def test_no_batch_statement_leaves_the_batch_columns_blank(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_CONFIRMATIONS='ack_one|100|100')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual((row['ack_batch'], row['ack_fallback_pct']), ('', ''))

    def test_drain_samples_before_workers_can_consume(self):
        # Starting collection after Compose returns loses jobs completed during startup.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            events = trace(root)
            self.assertIn('sample', events)
            self.assertLess(events.index('sample'), events.index('workers consuming'), events)

    def test_row_records_the_vm_and_pinning(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual((row['vm_cpus'], row['pinned']), ('8', '0'))
            machine = (evidence_dir(root) / 'machine.txt').read_text()
            self.assertIn('pinned: 0', machine)
            self.assertNotIn('cpuset.yaml', '\n'.join(trace(root)))

    def test_pin_adds_the_cpuset_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, PIN='1')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['pinned'], '1')
            cpuset = (evidence_dir(root) / 'cpuset.yaml').read_text()
            self.assertIn('postgres: {cpuset: "0-1"}', cpuset)
            self.assertIn('broker-standby: {cpuset: "2-3"}', cpuset)
            self.assertIn('broker: {cpuset: "2-3"}', cpuset)
            self.assertIn('broker-4: {cpuset: "2-3"}', cpuset)
            self.assertIn('worker-d: {cpuset: "4-6"}', cpuset)
            compose_calls = [e for e in trace(root) if e.startswith('compose files:')]
            self.assertTrue(compose_calls)
            for call in compose_calls:
                self.assertIn('cpuset.yaml', call)

    def test_four_brokers_split_the_workers_evenly(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BROKERS='4', WORKERS='8')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['brokers'], '4')
            ups = [e for e in trace(root) if e.startswith('compose up: ')]
            self.assertTrue(any('broker-4' in e and 'postgres' in e for e in ups), ups)
            [workers] = [e for e in ups if 'worker-b' in e]
            for service in ('worker', 'worker-b', 'worker-c', 'worker-d'):
                self.assertIn(f'--scale {service}=2', workers)
            [preload] = [e for e in trace(root) if e.startswith('compose run: ')]
            self.assertIn('broker:9090,broker-standby:9090,broker-3:9090,broker-4:9090', preload)

    def test_one_broker_gives_every_worker_to_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BROKERS='1', WORKERS='5')
            self.assertEqual(result.returncode, 0, result.stderr)
            [workers] = [e for e in trace(root) if e.startswith('compose up: ') and 'worker-b' in e]
            self.assertIn('--scale worker=5', workers)
            self.assertIn('--scale worker-b=0', workers)
            self.assertIn('--scale worker-d=0', workers)

    def test_brokers_must_be_one_two_or_four(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BROKERS='3')
            self.assertEqual(result.returncode, 2)
            self.assertIn('BROKERS must be 1, 2 or 4', result.stderr)

    def test_settings_override_what_the_stack_already_sets(self):
        # The bench overlay sets the workers' HOPPER_LOG_LEVEL in environment,
        # where an env_file could not override it.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER_ENV='HOPPER_LOG_LEVEL=info', BROKER_ENV='HOPPER_MAX_LEASE_BATCH=8',
                               VARIANT='lease8', PAYLOAD_BYTES='300')
            self.assertEqual(result.returncode, 0, result.stderr)
            settings = (evidence_dir(root) / 'settings.yaml').read_text()
            self.assertIn('  worker-d:\n    environment:\n      HOPPER_LOG_LEVEL: "info"\n', settings)
            self.assertIn('  broker-4:\n    environment:\n      HOPPER_MAX_LEASE_BATCH: "8"\n', settings)
            for call in [e for e in trace(root) if e.startswith('compose files:')]:
                self.assertIn('settings.yaml', call)
            [row] = rows(root)
            self.assertEqual((row['variant'], row['payload_bytes']), ('lease8', '300'))
            [preload] = [e for e in trace(root) if e.startswith('compose run: ')]
            self.assertIn('-payload-bytes 300', preload)

    def test_a_setting_compose_would_not_apply_stops_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER_ENV='HOPPER_LOG_LEVEL=info', VARIANT='info',
                               BENCH_TEST_IGNORE_SETTINGS='1')
            self.assertEqual(result.returncode, 2)
            self.assertIn('worker would run with HOPPER_LOG_LEVEL=unset, not info', result.stderr)

    def test_a_run_without_settings_writes_no_settings_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((evidence_dir(root) / 'settings.yaml').exists())

    def test_settings_must_be_assignments(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER_ENV='oops')
            self.assertEqual(result.returncode, 2)
            self.assertIn('WORKER_ENV entries must be NAME=value: oops', result.stderr)

    def test_setting_values_may_not_hold_quotes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BROKER_ENV='HOPPER_X=a"b')
            self.assertEqual(result.returncode, 2)
            self.assertIn('BROKER_ENV entries may not contain quotes or backslashes: HOPPER_X=a"b', result.stderr)

    def test_a_variant_may_not_break_the_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, VARIANT='a,b')
            self.assertEqual(result.returncode, 2)
            self.assertIn('VARIANT may not contain a comma', result.stderr)

    def test_row_names_the_harness_revision(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertRegex(row['harness'], r'^[0-9a-f]{7,}(-dirty)?$')

    def test_server_settings_are_written_per_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, PG_ARGS='commit_delay=200 commit_siblings=5', EXPLAIN='1', VARIANT='delay')
            self.assertEqual(result.returncode, 0, result.stderr)
            pg = (evidence_dir(root) / 'postgres.yaml').read_text()
            self.assertIn('"-c", "commit_delay=200", "-c", "commit_siblings=5"', pg)
            self.assertIn('shared_preload_libraries=pg_stat_statements,auto_explain', pg)
            self.assertIn('"-c", "synchronous_commit=on"', pg)
            for call in [e for e in trace(root) if e.startswith('compose files:')]:
                self.assertIn('postgres.yaml', call)

    def test_pg_args_must_be_settings(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, PG_ARGS='oops')
            self.assertEqual(result.returncode, 2)
            self.assertIn('PG_ARGS entries must be name=value: oops', result.stderr)

    def test_bench_sql_runs_before_the_preload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_SQL='alter table jobs set (fillfactor = 80)', VARIANT='ff80')
            self.assertEqual(result.returncode, 0, result.stderr)
            events = trace(root)
            setup = events.index('sql: alter table jobs set (fillfactor = 80)')
            preload = next(i for i, e in enumerate(events) if e.startswith('compose run: '))
            self.assertLess(setup, preload)
            self.assertIn('fillfactor = 80', (evidence_dir(root) / 'bench.sql').read_text())

    def test_changed_settings_need_a_variant(self):
        for knob, value in [('PG_ARGS', 'commit_delay=200'), ('BROKER_ENV', 'HOPPER_MAX_LEASE_BATCH=8'),
                            ('WORKER_ENV', 'HOPPER_LOG_LEVEL=info'), ('BENCH_SQL', 'select 1'), ('EXPLAIN', '1')]:
            with self.subTest(knob=knob), tempfile.TemporaryDirectory() as tmp:
                result = run_bench(Path(tmp), **{knob: value})
                self.assertEqual(result.returncode, 2)
                self.assertIn(f'{knob} changes the run: name it with VARIANT', result.stderr)

    def test_pin_needs_eight_vm_cpus(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, PIN='1', BENCH_TEST_NCPU='4')
            self.assertEqual(result.returncode, 2)
            self.assertIn('PIN=1 needs a Docker VM with at least 8 CPUs; this one has 4', result.stderr)

    def test_row_carries_wait_shares_and_lease_cost(self):
        # The fake reports 1 LWLock and 3 on-CPU backends per sample.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['active_backends'], '4.00')
            self.assertEqual(row['on_cpu_pct'], '75.0')
            self.assertEqual(row['lwlock_pct'], '25.0')
            self.assertEqual(row['lock_pct'], '0.0')
            self.assertEqual(row['io_pct'], '0.0')
            self.assertEqual((row['lease_mean_ms'], row['lease_blks_per_call']), ('0.500', '12.0'))
            waits = (evidence_dir(root) / 'waits.txt').read_text()
            self.assertIn('event|LWLock|LockManager|', waits)

    def test_row_carries_lease_rows_and_calls_per_job(self):
        # The fake's lease statement returned 4.00 rows a call over 250 calls,
        # for 1,000 jobs (and 1,000 Acks, so the drain's Ack check passes).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, JOBS='1000', BENCH_TEST_COMPLETED='1000', BENCH_TEST_ACKS='1000')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual((row['lease_rows_per_call'], row['lease_calls_per_job']), ('4.00', '0.250'))

    def test_steady_samples_waits_too(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, scenario='steady')
            # Only the waits matter here.
            self.assertTrue((evidence_dir(root) / 'waits.psv').exists(), result.stderr)

    def test_no_sample_lands_after_the_run(self):
        # docker stats takes ~2s per sample; one still in flight when the sampler
        # stops would append after the row was computed from the file.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_STATS_DELAY='1')
            self.assertEqual(result.returncode, 0, result.stderr)
            stats = evidence_dir(root) / 'stats.csv'
            at_exit = stats.read_text()
            time.sleep(1.5)
            self.assertEqual(stats.read_text(), at_exit)
            pg = [float(l.split(',')[1].rstrip('%')) for l in at_exit.splitlines() if 'postgres' in l]
            [row] = rows(root)
            self.assertEqual(row['postgres_cpu'], '%.0f' % (sum(pg) / len(pg)), at_exit)

    def test_a_run_without_wait_samples_writes_no_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_NO_WAITS='1')
            self.assertEqual(result.returncode, 1)
            self.assertEqual(rows(root), [])
            self.assertIn('no wait-event samples', (evidence_dir(root) / 'rejected.txt').read_text())

    def test_a_drain_with_retries_writes_no_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_RETRIED='3')
            self.assertEqual(result.returncode, 1)
            self.assertEqual(rows(root), [])
            self.assertIn('3 jobs were retried', (evidence_dir(root) / 'rejected.txt').read_text())

    def test_refuses_to_append_to_a_different_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'out').mkdir()
            (root / 'out' / 'results.csv').write_text('label,scenario,ref\nold,drain,abc\n')
            result = run_bench(root)
            self.assertEqual(result.returncode, 2)
            self.assertIn('has a different header; use a new OUT', result.stderr)
            self.assertNotIn('workers consuming', trace(root))

    def test_row_carries_commits_wal_and_update_mix_per_job(self):
        # The fake's counters grow by 1000 commits, 100 syncs, 4,096,000 WAL
        # bytes, 300 updates and 30 HOT updates between snapshots, over JOBS=1.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['commits_per_job'], '1000.00')
            self.assertEqual(row['wal_syncs_per_job'], '100.000')
            self.assertEqual(row['wal_kb_per_job'], '4000.00')
            self.assertEqual(row['hot_update_pct'], '10.0')
            self.assertEqual(row['dead_tuples'], '100')
            self.assertEqual(len((evidence_dir(root) / 'counters.txt').read_text().splitlines()), 2)

    def test_wal_syncs_are_blank_where_postgres_does_not_count_them(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_WAL_SYNC='on|open_datasync')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['wal_syncs_per_job'], '')
            self.assertEqual(row['commits_per_job'], '1000.00')
            self.assertEqual((evidence_dir(root) / 'wal_sync.txt').read_text(), 'on|open_datasync\n')

    def test_statistics_settle_before_both_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            start = time.monotonic()
            result = run_bench(root, STATS_SETTLE='1')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertGreaterEqual(time.monotonic() - start, 2)

    def test_row_carries_confirmation_p99(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['ack_p99_ms'], '2.35')
            self.assertEqual(row['heartbeat_p99_ms'], '')
            self.assertIn('# after :16091', (evidence_dir(root) / 'rpc.txt').read_text())

    def test_a_failed_scrape_rejects_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_CURL_FAIL='2')
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(rows(root), [])
            self.assertIn(':16091 after scrape failed', (evidence_dir(root) / 'rejected.txt').read_text())

    def test_a_drain_with_fewer_acks_than_jobs_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_ACKS='0')
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertIn('the Ack histograms saw 0 calls for 1 jobs', (evidence_dir(root) / 'rejected.txt').read_text())

    def test_row_carries_worker_cpu_memory_threads_and_startup(self):
        # Two workers at 100 ticks (1 s) each since they started, 1,000 jobs
        # (and 1,000 Acks, so the drain's Ack check passes).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, JOBS='1000', BENCH_TEST_COMPLETED='1000', BENCH_TEST_ACKS='1000')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual((row['worker_sdk'], row['worker_cpu_ms_per_1k'], row['worker_peak_rss_mib'],
                              row['worker_threads'], row['startup_ms']), ('go', '2000', '20.0', '9', '250'))
            ev = evidence_dir(root)
            self.assertIn('== end a1b2c3d4e5f6aaaa 101', (ev / 'proc.txt').read_text())
            self.assertTrue((ev / 'worker-1.stdout').exists())
            self.assertTrue((ev / 'worker-2.stdout').exists())
            self.assertIn('"Hostname": "a1b2c3d4e5f6"', (ev / 'workers.json').read_text())
            self.assertIn('lease stream opened', (ev / 'brokers.log').read_text())

    def test_steady_counts_worker_cpu_between_its_snapshots(self):
        # 40 ticks before the measured run and 100 after, on each of two workers: 1.2 s for 1,000 jobs.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, scenario='steady', BENCH_TEST_TICKS_BEFORE='40',
                               BENCH_TEST_TICKS_AFTER='100', BENCH_TEST_COMPLETED='0,1000')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['worker_cpu_ms_per_1k'], '1200')
            events = trace(root)
            snaps = [i for i, e in enumerate(events) if e.startswith('docker run: ') and '--pid=host' in e]
            measured = next(i for i, e in enumerate(events) if e.startswith('compose run: ') and '-rate' in e)
            self.assertEqual(len(snaps), 2, events)
            self.assertLess(snaps[0], measured)
            self.assertGreater(snaps[1], measured)

    def test_a_worker_that_exited_rejects_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_EXITED='1')
            self.assertEqual(result.returncode, 1)
            self.assertEqual(rows(root), [])
            self.assertIn('worker-1 exited during the run (exit 3, OOM-killed false)',
                          (evidence_dir(root) / 'rejected.txt').read_text())

    def test_go_is_the_default_worker_and_adds_no_overlay(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual((row['worker_sdk'], row['worker_mem']), ('go', ''))
            self.assertNotIn('java.yaml', '\n'.join(trace(root)))
            self.assertFalse((evidence_dir(root) / 'jvm-flags.txt').exists())
            self.assertIn('worker: go', (evidence_dir(root) / 'machine.txt').read_text())

    def test_java_runs_the_java_overlay_and_records_its_jvm(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER='java')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['worker_sdk'], 'java')
            for call in [e for e in trace(root) if e.startswith('compose files:')]:
                self.assertIn('deploy/bench/java.yaml', call)
            ev = evidence_dir(root)
            self.assertIn('java opts: -XX:+UseSerialGC', (ev / 'machine.txt').read_text())
            self.assertIn('UseSerialGC', (ev / 'jvm-flags.txt').read_text())
            [probe] = [e for e in trace(root) if 'PrintFlagsFinal' in e]
            self.assertIn('JAVA_OPTS=-XX:+UseSerialGC -XX:+PrintFlagsFinal -version', probe)

    def test_worker_must_be_go_or_java(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = run_bench(Path(tmp), WORKER='python')
            self.assertEqual(result.returncode, 2)
            self.assertIn('WORKER must be go or java', result.stderr)

    def test_worker_jvm_applies_to_java_only_and_holds_no_quotes(self):
        for kw, msg in [({'WORKER_JVM': '-Xmx1g'}, 'WORKER_JVM applies to WORKER=java only'),
                        ({'WORKER': 'java', 'WORKER_JVM': '-Dx="y"'}, 'WORKER_JVM may not contain quotes')]:
            with self.subTest(kw=kw), tempfile.TemporaryDirectory() as tmp:
                result = run_bench(Path(tmp), VARIANT='x', **kw)
                self.assertEqual(result.returncode, 2)
                self.assertIn(msg, result.stderr)

    def test_worker_mem_limits_every_worker_service(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER_MEM='384m', BENCH_TEST_MEMORY='402653184')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['worker_mem'], '384m')
            mem = (evidence_dir(root) / 'workers-mem.yaml').read_text()
            for svc in ('worker', 'worker-b', 'worker-c', 'worker-d'):
                self.assertIn(f'  {svc}: {{mem_limit: 384m, memswap_limit: 384m}}', mem)
            for call in [e for e in trace(root) if e.startswith('compose files:')]:
                self.assertIn('workers-mem.yaml', call)

    def test_a_memory_limit_the_workers_did_not_get_rejects_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER_MEM='384m')
            self.assertEqual(result.returncode, 1)
            self.assertIn('worker-1 ran with memory limit 0, not 402653184',
                          (evidence_dir(root) / 'rejected.txt').read_text())

    def test_worker_mem_must_be_a_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = run_bench(Path(tmp), WORKER_MEM='lots')
            self.assertEqual(result.returncode, 2)
            self.assertIn('WORKER_MEM must be a size such as 384m or 1g', result.stderr)

    def test_drain_reports_warm_throughput_and_its_rate_curve(self):
        # 1,000 jobs, and 1,000 Acks so the drain's Ack check passes.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, JOBS='1000', WARM_JOBS='100', BENCH_TEST_COMPLETED='1000',
                               BENCH_TEST_ACKS='1000')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['warm_throughput'], '120')
            # The fake answers the query's offset plus 21: offset 99 is the 100th completion.
            self.assertIn('0|5', (evidence_dir(root) / 'rate.psv').read_text())
            self.assertIn('warm jobs: 100', (evidence_dir(root) / 'machine.txt').read_text())

    def test_the_rate_curve_reads_jobs_once_with_its_origin_computed_first(self):
        # The first lease is a scalar subquery, which PostgreSQL runs once
        # (an InitPlan). As a join it was planned on the preload's statistics,
        # which see no completed rows, and a nested loop then re-ran the
        # min(leased_at) scan for every completed job: the 200,000-job pilot
        # drain's rate.psv query was still running after 12 minutes.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, JOBS='1000', BENCH_TEST_COMPLETED='1000', BENCH_TEST_ACKS='1000')
            self.assertEqual(result.returncode, 0, result.stderr)
            [q] = [e for e in trace(root) if e.startswith('rate sql: ')]
            self.assertIn('(select min(leased_at) from jobs)', q)
            self.assertEqual(q.count(' from jobs'), 2, q)  # the curve's scan and the subquery's, no join
            self.assertNotIn('from jobs j,', q)

    def test_warm_jobs_and_warmup_belong_to_their_scenarios(self):
        for scenario, kw, msg in [('drain', {'JOBS': '10', 'WARM_JOBS': '10'}, 'WARM_JOBS must be less than JOBS'),
                                  ('steady', {'WARM_JOBS': '5'}, 'WARM_JOBS applies to drain only'),
                                  ('drain', {'WARMUP': '5', 'VARIANT': 'w'}, 'WARMUP applies to steady only'),
                                  ('drain', {'WARM_JOBS': 'some'}, 'WARM_JOBS must be a whole number')]:
            with self.subTest(kw=kw), tempfile.TemporaryDirectory() as tmp:
                result = run_bench(Path(tmp), scenario=scenario, **kw)
                self.assertEqual(result.returncode, 2)
                self.assertIn(msg, result.stderr)

    def test_steady_warmup_runs_first_and_counts_nothing(self):
        # 600 jobs completed by the warm-up, 1,600 by the end: the measured run's 1,000
        # carry the 1,000 commits between the two snapshots.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, scenario='steady', WARMUP='5', VARIANT='warm', BENCH_TEST_COMPLETED='600,1600')
            self.assertEqual(result.returncode, 0, result.stderr)
            loads = [e for e in trace(root) if e.startswith('compose run: ')]
            self.assertEqual(len(loads), 2, loads)
            self.assertIn('-duration 5s', loads[0])
            self.assertIn('-run warmup', loads[0])
            self.assertNotIn('-run warmup', loads[1])
            [row] = rows(root)
            self.assertEqual(row['commits_per_job'], '1.00')
            self.assertTrue((evidence_dir(root) / 'warmup.json').exists())

    def test_drain_records_the_most_rows_leased_at_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, BENCH_TEST_LEASED='7')
            self.assertEqual(result.returncode, 0, result.stderr)
            [row] = rows(root)
            self.assertEqual(row['max_leased'], '7')

    def test_more_rows_leased_than_slots_rejects_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKERS='1', CONCURRENCY='2', BENCH_TEST_LEASED='3')
            self.assertEqual(result.returncode, 1)
            self.assertIn("3 rows were leased at once, more than the workers' 2 slots",
                          (evidence_dir(root) / 'rejected.txt').read_text())

    def test_drain_preload_carries_work_ms(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORK_MS='15000', VARIANT='wide')
            self.assertEqual(result.returncode, 0, result.stderr)
            [preload] = [e for e in trace(root) if e.startswith('compose run: ')]
            self.assertIn('-work-ms 15000', preload)

    def test_jfr_records_each_java_worker_and_keeps_the_recordings(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER='java', JFR='1', JFR_SETTLE='0', VARIANT='jfr')
            self.assertEqual(result.returncode, 0, result.stderr)
            ev = evidence_dir(root)
            opts = (ev / 'machine.txt').read_text()
            self.assertIn('-XX:StartFlightRecording=filename=/tmp/hopper.jfr,settings=profile,duration=1s,'
                          'jdk.VirtualThreadPinned#threshold=0ms -Xlog:jfr+startup=off', opts)
            self.assertTrue((ev / 'worker-1.jfr').exists())
            self.assertTrue((ev / 'worker-2.jfr').exists())
            # The flags probe runs the JVM without the recording.
            [probe] = [e for e in trace(root) if 'PrintFlagsFinal' in e]
            self.assertNotIn('StartFlightRecording', probe)

    def test_a_missing_recording_rejects_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER='java', JFR='1', JFR_SETTLE='0', VARIANT='jfr', BENCH_TEST_NO_JFR='1')
            self.assertEqual(result.returncode, 1)
            self.assertIn('no JFR recording from worker-1 worker-2', (evidence_dir(root) / 'rejected.txt').read_text())

    def test_profiles_need_their_worker_and_a_drain(self):
        for scenario, kw, msg in [('drain', {'JFR': '1'}, 'JFR applies to WORKER=java drains only'),
                                  ('steady', {'JFR': '1', 'WORKER': 'java'}, 'JFR applies to WORKER=java drains only'),
                                  ('drain', {'JFR': '0', 'WORKER': 'java'}, 'JFR must be at least 1 second'),
                                  ('drain', {'WORKER_PPROF': '1', 'WORKER': 'java'},
                                   'WORKER_PPROF applies to WORKER=go drains only')]:
            with self.subTest(kw=kw), tempfile.TemporaryDirectory() as tmp:
                result = run_bench(Path(tmp), scenario=scenario, VARIANT='p', **kw)
                self.assertEqual(result.returncode, 2)
                self.assertIn(msg, result.stderr)

    def test_worker_pprof_fetches_three_profiles_from_the_first_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, WORKER_PPROF='0', WORKER_PPROF_SECONDS='1', VARIANT='pprof')
            self.assertEqual(result.returncode, 0, result.stderr)
            ev = evidence_dir(root)
            self.assertIn('  worker:\n    environment:\n      HOPPER_DEBUG_LISTEN: "0.0.0.0:6060"\n',
                          (ev / 'settings.yaml').read_text())
            fetches = [e for e in trace(root) if e.startswith('docker run: ') and 'wget' in e]
            for path in ('heap?debug=1', 'goroutine?debug=1', 'profile?seconds=1'):
                self.assertTrue(any(f'http://hopper-bench-worker-1:6060/debug/pprof/{path}' in e for e in fetches),
                                fetches)
            for f in ('worker-heap.txt', 'worker-goroutine.txt', 'worker-cpu.pprof'):
                self.assertTrue((ev / f).exists(), f)

    def test_new_knobs_need_a_variant(self):
        for scenario, kw, knob in [('steady', {'WARMUP': '5'}, 'WARMUP'),
                                   ('drain', {'WORKER': 'java', 'JFR': '1'}, 'JFR'),
                                   ('drain', {'WORKER_PPROF': '0'}, 'WORKER_PPROF'),
                                   ('drain', {'WORKER': 'java', 'WORKER_JVM': '-Xmx1g'}, 'WORKER_JVM'),
                                   ('drain', {'WORK_MS': '5'}, 'WORK_MS')]:
            with self.subTest(knob=knob), tempfile.TemporaryDirectory() as tmp:
                result = run_bench(Path(tmp), scenario=scenario, **kw)
                self.assertEqual(result.returncode, 2)
                self.assertIn(f'{knob} changes the', result.stderr)


if __name__ == '__main__':
    unittest.main()
