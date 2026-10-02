"""Benchmark orchestration regression checks; run with python3 scripts/bench_test.py."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class BenchmarkTest(unittest.TestCase):
    def test_drain_samples_before_workers_can_consume(self):
        # Starting collection after Compose returns loses jobs completed during startup.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = root / 'docker'
            fake.write_text('''#!/usr/bin/env python3
import os, sys, time
from pathlib import Path
args = sys.argv[1:]
trace = Path(os.environ['BENCH_TEST_TRACE'])
def event(name):
    with trace.open('a') as f: f.write(name + '\\n')
if args[0] == 'stats':
    event('sample')
    print('hopper-bench-postgres-1,10%')
    time.sleep(0.02)
elif args[0] == 'compose':
    if 'up' in args and 'worker-b' in args:
        event('workers consuming')
        time.sleep(0.2)
    elif 'exec' in args:
        sql = args[-1]
        if "where state = 'completed'" in sql: print('1')
        elif 'extract(epoch' in sql: print('100|0')
        elif "select 'total|" in sql: print('total|statements=1|calls=1|total_ms=1')
    elif 'run' in args: print('{}')
''')
            fake.chmod(0o755)
            curl = root / 'curl'
            curl.write_text('#!/bin/sh\necho "hopper_db_pool_acquires_total 1"\n')
            curl.chmod(0o755)
            env = os.environ.copy()
            env.update(PATH=str(root) + os.pathsep + env['PATH'],
                       BENCH_TEST_TRACE=str(root / 'trace'), OUT=str(root / 'out'),
                       REF='HEAD', JOBS='1', BROKERS='1', WORKERS='4', PROFILE='0', KEEP='0')
            result = subprocess.run(['bash', 'scripts/bench.sh', 'drain'], env=env,
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            events = (root / 'trace').read_text().splitlines()
            self.assertIn('sample', events)
            self.assertLess(events.index('sample'), events.index('workers consuming'), events)


if __name__ == '__main__':
    unittest.main()
