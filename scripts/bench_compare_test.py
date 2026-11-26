"""Checks for scripts/bench_compare.py; run with python3 scripts/bench_compare_test.py."""
import csv
import io
from contextlib import redirect_stdout
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_compare  # noqa: E402


def write(path, rows):
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['variant', 'brokers', 'sync_commit', 'throughput', 'enqueue_p99_ms', 'shards'])
        w.writeheader()
        w.writerows(rows)


def rows(variant, brokers, values, metric='throughput', sync_commit='on', shards='1'):
    return [{'variant': variant, 'brokers': brokers, 'sync_commit': sync_commit,
             'throughput': '', 'enqueue_p99_ms': '', 'shards': shards, metric: v}
            for v in values]


class VerdictTest(unittest.TestCase):
    def test_helps_needs_ten_percent_and_separate_ranges(self):
        self.assertEqual(bench_compare.verdict([100, 101, 102], [120, 121, 122])[1], 'helps')

    def test_a_large_gain_with_overlapping_ranges_is_no_measurable_effect(self):
        self.assertEqual(bench_compare.verdict([100, 101, 130], [120, 125, 140])[1], 'no measurable effect')

    def test_more_than_five_percent_worse_is_worse(self):
        self.assertEqual(bench_compare.verdict([100, 100, 100], [90, 94, 99])[1], 'worse')

    def test_lower_is_better_flips_the_direction(self):
        gain, word = bench_compare.verdict([20, 21, 22], [10, 11, 12], lower_is_better=True)
        self.assertEqual(word, 'helps')
        self.assertAlmostEqual(gain, 21 / 11)


class MainTest(unittest.TestCase):
    def test_reports_each_shared_broker_count_and_ignores_other_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'r.csv'
            write(p, rows('check', 1, [100, 101, 102]) + rows('skip', 1, [120, 121, 122]) +
                     rows('check', 4, [50, 51, 52]) + rows('skip', 4, [50, 51, 53]) +
                     rows('other', 1, [999]) + rows('skip', 2, [1, 2, 3]))
            out = io.StringIO()
            with redirect_stdout(out):
                bench_compare.main([str(p), 'check', 'skip'])
            lines = out.getvalue().splitlines()
            self.assertEqual(len(lines), 2, lines)
            self.assertTrue(lines[0].startswith('b1 throughput: skip 121 (120-122) vs check 101 (100-102)'), lines[0])
            self.assertTrue(lines[0].endswith('helps'), lines[0])
            self.assertTrue(lines[1].endswith('no measurable effect'), lines[1])

    def test_blank_metric_cells_are_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'r.csv'
            write(p, rows('on', 4, [30, 31, 32], 'enqueue_p99_ms') + rows('off', 4, [20, 21, 22], 'enqueue_p99_ms'))
            out = io.StringIO()
            with redirect_stdout(out):
                bench_compare.main([str(p), 'on', 'off', '--metric', 'enqueue_p99_ms', '--lower-is-better'])
            self.assertIn('helps', out.getvalue())

    def test_where_keeps_only_matching_rows(self):
        # H2's async reference shares the control's variant name; only sync_commit tells them apart.
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'r.csv'
            write(p, rows('nobatch', 1, [100, 101, 102]) + rows('nobatch', 1, [500, 501, 502], sync_commit='off') +
                     rows('batch', 1, [120, 121, 122]))
            out = io.StringIO()
            with redirect_stdout(out):
                bench_compare.main([str(p), 'nobatch', 'batch', '--where', 'sync_commit=on'])
            self.assertIn('vs nobatch 101 (100-102)', out.getvalue())
            self.assertTrue(out.getvalue().rstrip().endswith('helps'), out.getvalue())

    def test_column_picks_control_and_treatment_by_another_column(self):
        # One variant per layout; the shard count is the treatment.
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'r.csv'
            write(p, rows('w8c16', 2, [100, 101, 102]) + rows('w8c16', 2, [150, 151, 152], shards='2') +
                     rows('w2c256', 2, [999, 999, 999], shards='2'))
            out = io.StringIO()
            with redirect_stdout(out):
                bench_compare.main([str(p), '1', '2', '--column', 'shards', '--where', 'variant=w8c16'])
            line = out.getvalue().strip()
            self.assertTrue(line.startswith('b2 throughput: 2 151 (150-152) vs 1 101 (100-102)'), line)
            self.assertTrue(line.endswith('helps'), line)

    def test_no_shared_broker_count_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'r.csv'
            write(p, rows('check', 1, [100]) + rows('skip', 4, [100]))
            with self.assertRaises(SystemExit):
                bench_compare.main([str(p), 'check', 'skip'])


if __name__ == '__main__':
    unittest.main()
