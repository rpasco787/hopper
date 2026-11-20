"""Checks for scripts/bench_chart.py; run with python3 scripts/bench_chart_test.py."""
import csv
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_chart  # noqa: E402


def write(path, rows):
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['scenario', 'variant', 'brokers', 'throughput'])
        w.writeheader()
        w.writerows(rows)


class ChartTest(unittest.TestCase):
    def test_medians_take_one_variant_and_ignore_steady_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'r.csv'
            write(p, [dict(scenario='drain', variant='after', brokers=1, throughput=t) for t in (10, 20, 30)] +
                     [dict(scenario='drain', variant='before', brokers=1, throughput=999),
                      dict(scenario='steady', variant='after', brokers=1, throughput=999)])
            self.assertEqual(bench_chart.medians(p, 'after'), {1: 20.0})

    def test_series_argument_names_a_file_and_a_variant(self):
        self.assertEqual(bench_chart.parse_series('after (abc)=r.csv#after'), ('after (abc)', 'r.csv', 'after'))
        self.assertEqual(bench_chart.parse_series('base=r.csv'), ('base', 'r.csv', ''))

    def test_chart_has_a_line_and_a_legend_per_series(self):
        svg = bench_chart.chart([('before', {1: 5000, 2: 4800, 4: 4500}), ('after', {1: 9000, 2: 8800, 4: 8000})])
        self.assertEqual(svg.count('<polyline'), 2)
        self.assertIn('>before<', svg)
        self.assertIn('>after<', svg)
        self.assertIn('9,000', svg)


if __name__ == '__main__':
    unittest.main()
