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
FAKE_DOCKER = r'''#!/usr/bin/env python3
import os, sys, time
from pathlib import Path
args = sys.argv[1:]
trace = Path(os.environ['BENCH_TEST_TRACE'])
def event(name):
    with trace.open('a') as f: f.write(name + '\n')
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
elif args[0] == 'compose':
    event('compose files: ' + ' '.join(a for a in args if a.endswith(('.yml', '.yaml'))))
    if 'up' in args:
        event('compose up: ' + ' '.join(args))
    if 'up' in args and 'worker-b' in args:
        event('workers consuming')
        time.sleep(0.2)
    elif 'exec' in args:
        sql = args[-1]
        if "where state = 'completed'" in sql: print('1')
        elif 'extract(epoch' in sql: print('100|' + os.environ.get('BENCH_TEST_RETRIED', '0'))
        elif "select 'total|" in sql: print('total|statements=1|calls=1|total_ms=1')
        elif 'pg_stat_activity' in sql:
            if os.environ.get('BENCH_TEST_NO_WAITS') != '1':
                print('12:00:00.000|SAMPLE|-|0')
                print('12:00:00.000|LWLock|LockManager|1')
                print('12:00:00.000|CPU|running|3')
        elif 'with next as' in sql: print('0.500|12.0')
    elif 'logs' in args: print('"lease stream opened"\n' * 16, end='')
    elif 'run' in args:
        event('compose run: ' + ' '.join(args))
        print('{}')
'''


def run_bench(root, scenario='drain', **overrides):
    """Runs scripts/bench.sh against the fake docker; returns the CompletedProcess."""
    fake = root / 'docker'
    fake.write_text(FAKE_DOCKER)
    fake.chmod(0o755)
    curl = root / 'curl'
    curl.write_text('#!/bin/sh\necho "hopper_db_pool_acquires_total 1"\n')
    curl.chmod(0o755)
    env = os.environ.copy()
    env.update(PATH=str(root) + os.pathsep + env['PATH'],
               BENCH_TEST_TRACE=str(root / 'trace'), OUT=str(root / 'out'),
               REF='HEAD', JOBS='1', BROKERS='1', WORKERS='4', PROFILE='0', KEEP='0')
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

    def test_steady_samples_waits_too(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_bench(root, scenario='steady')
            # The fake's loadgen JSON is {}, so steady's numbers are empty; only the waits matter here.
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


if __name__ == '__main__':
    unittest.main()
