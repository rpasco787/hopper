"""Checks for scripts/bench_workers.py; run with python3 scripts/bench_workers_test.py."""
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_workers  # noqa: E402

START = '2026-11-25T00:00:00.000000000Z'
A, B, C = 'a1b2c3d4e5f60000', 'f6e5d4c3b2a10000', '0123456789ab0000'


def worker(cid, n=1, running=True, oom=False, code=0, memory=0, started=START):
    return {'Id': cid, 'Name': f'/hopper-bench-worker-{n}', 'Config': {'Hostname': cid[:12]},
            'State': {'StartedAt': started, 'Running': running, 'OOMKilled': oom, 'ExitCode': code},
            'HostConfig': {'Memory': memory}}


def snap(label, cid, pid, utime, stime, hwm_kb=20480, threads=9, comm='app'):
    return (f'== {label} {cid} {pid}\n'
            f'{pid} ({comm}) S 1 1 1 0 -1 4194560 10 0 0 0 {utime} {stime} 0 0 20 0 {threads} 0 5 0\n'
            f'Name:\t{comm}\nVmHWM:\t{hwm_kb:>8} kB\nVmRSS:\t{hwm_kb:>8} kB\nThreads:\t{threads}\n')


def opened(cid, at):
    return ('{"time":"%s","level":"INFO","msg":"lease stream opened","worker_id":"%s-1","credit":16}'
            % (at, cid[:12]))


def cols(proc, workers, lines=(), jobs=1, memory=0):
    return bench_workers.columns(proc, workers, list(lines), jobs, memory)


class WorkersTest(unittest.TestCase):
    def test_a_drain_counts_cpu_since_each_process_started(self):
        proc = 'clk_tck 100\n' + snap('end', A, 7, 100, 50) + snap('end', B, 8, 200, 50)
        self.assertEqual(cols(proc, [worker(A), worker(B, 2)], jobs=2000)[0], '2000')  # 4 s over 2,000 jobs

    def test_a_steady_run_counts_cpu_between_its_snapshots(self):
        proc = ('clk_tck 100\n' + snap('before', A, 7, 30, 10) + snap('before', B, 8, 30, 10)
                + 'clk_tck 100\n' + snap('after', A, 7, 60, 40) + snap('after', B, 8, 90, 30))
        self.assertEqual(cols(proc, [worker(A), worker(B, 2)], jobs=1000)[0], '1400')  # 60 + 80 ticks

    def test_the_tick_rate_comes_from_the_snapshot(self):
        self.assertEqual(cols('clk_tck 250\n' + snap('end', A, 7, 250, 0), [worker(A)], jobs=1000)[0], '1000')

    def test_a_command_name_with_spaces_and_parentheses_still_parses(self):
        proc = 'clk_tck 100\n' + snap('end', A, 7, 100, 0, comm='my (odd) app')
        self.assertEqual(cols(proc, [worker(A)], jobs=1000)[0], '1000')

    def test_peak_memory_and_threads_are_medians_across_workers(self):
        proc = ('clk_tck 100\n' + snap('end', A, 7, 1, 0, hwm_kb=10240, threads=9)
                + snap('end', B, 8, 1, 0, hwm_kb=20480, threads=31)
                + snap('end', C, 9, 1, 0, hwm_kb=40960, threads=33))
        self.assertEqual(cols(proc, [worker(A), worker(B, 2), worker(C, 3)])[1:3], ('20.0', '31'))

    def test_startup_runs_from_container_start_to_the_first_accepted_stream(self):
        lines = ['not json', '{"msg":"lease stream opened"}',
                 opened(A, '2026-11-25T00:00:00.900000000Z'), opened(A, '2026-11-25T00:00:00.400123456Z'),
                 opened(B, '2026-11-25T00:00:00.600Z')]
        proc = 'clk_tck 100\n' + snap('end', A, 7, 1, 0) + snap('end', B, 8, 1, 0)
        self.assertEqual(cols(proc, [worker(A), worker(B, 2)], lines)[3], '500')  # median of 400.1 and 600

    def test_startup_is_blank_when_a_worker_never_opened_a_stream(self):
        proc = 'clk_tck 100\n' + snap('end', A, 7, 1, 0) + snap('end', B, 8, 1, 0)
        self.assertEqual(cols(proc, [worker(A), worker(B, 2)], [opened(A, START)])[3], '-')

    def test_no_jobs_leaves_cpu_per_job_blank(self):
        self.assertEqual(cols('clk_tck 100\n' + snap('end', A, 7, 1, 0), [worker(A)], jobs=0)[0], '-')

    def test_a_worker_that_exited_or_was_oom_killed_fails_the_run(self):
        proc = 'clk_tck 100\n' + snap('end', A, 7, 1, 0)
        for w, want in [(worker(A, running=False, code=3), 'worker-1 exited during the run (exit 3, OOM-killed false)'),
                        (worker(A, running=False, oom=True, code=137),
                         'worker-1 exited during the run (exit 137, OOM-killed true)')]:
            with self.subTest(want=want), self.assertRaisesRegex(ValueError, re.escape(want)):
                cols(proc, [w])

    def test_a_memory_limit_other_than_the_one_asked_fails_the_run(self):
        proc = 'clk_tck 100\n' + snap('end', A, 7, 1, 0)
        with self.assertRaisesRegex(ValueError, 'worker-1 ran with memory limit 0, not 402653184'):
            cols(proc, [worker(A)], memory=402653184)
        self.assertEqual(cols(proc, [worker(A, memory=402653184)], memory=402653184)[0], '10000')

    def test_missing_statistics_fail_the_run(self):
        with self.assertRaisesRegex(ValueError, 'no worker process statistics'):
            cols('', [worker(A)])
        broken = 'clk_tck 100\n' + snap('end', A, 7, 1, 0) + f'== end {B} 8\ncat: /proc/8/stat: No such file\n'
        with self.assertRaisesRegex(ValueError, 'no /proc statistics for worker-2 at "end"'):
            cols(broken, [worker(A), worker(B, 2)])
        with self.assertRaisesRegex(ValueError, 'no /proc statistics for worker-1 at "before"'):
            cols('clk_tck 100\n' + snap('after', A, 7, 1, 0), [worker(A)])
        with self.assertRaisesRegex(ValueError, 'no worker containers'):
            cols('clk_tck 100\n' + snap('end', A, 7, 1, 0), [])

    def test_times_keep_nanoseconds_and_zones(self):
        t = bench_workers.parse_time
        self.assertAlmostEqual(t('2026-11-25T00:00:01.123456789Z') - t('2026-11-25T00:00:00Z'), 1.123456789, places=6)
        self.assertEqual(t('2026-11-24T19:00:00-05:00'), t('2026-11-25T00:00:00Z'))
        with self.assertRaises(ValueError):
            t('yesterday')

    def test_main_reads_an_evidence_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / 'proc.txt').write_text('clk_tck 100\n' + snap('end', A, 7, 100, 0))
            (d / 'workers.json').write_text(json.dumps([worker(A)]))
            (d / 'brokers.log').write_text(opened(A, '2026-11-25T00:00:00.250Z') + '\n')
            out = subprocess.run([sys.executable, 'scripts/bench_workers.py', str(d), '1000', '0'],
                                 capture_output=True, text=True)
            self.assertEqual((out.returncode, out.stdout), (0, '1000 20.0 9 250\n'), out.stderr)
            (d / 'workers.json').write_text(json.dumps([worker(A, running=False, code=1)]))
            out = subprocess.run([sys.executable, 'scripts/bench_workers.py', str(d), '1000', '0'],
                                 capture_output=True, text=True)
            self.assertEqual(out.returncode, 1)
            self.assertIn('worker-1 exited during the run', out.stderr)


if __name__ == '__main__':
    unittest.main()
