"""Checks for scripts/soak_chart.py; run with python3 -m unittest discover -s scripts -p '*_test.py'."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent))
import soak_chart  # noqa: E402

# Three samples from two shards, the second with no pickup p99 (no Prometheus).
TIMELINE = """t_s,offered_per_s,acked_per_s,behind,completed_per_s,enqueue_p99_ms,pickup_p99_ms,shard0_completed,shard0_table_bytes,shard0_dead_tup,shard1_completed,shard1_table_bytes,shard1_dead_tup
30,200.0,199.5,0,190.2,8.1,41.0,2900,1048576,120,2950,1100000,130
60,200.0,200.1,0,201.7,9.4,,5950,2097152,300,6000,2200000,280
90,200.0,198.9,3,199.0,12.6,55.5,8930,3145728,90,8990,3300000,100
"""


def titles(svg):
    return [t.text for t in ET.fromstring(svg).iter('{http://www.w3.org/2000/svg}title')]


class SoakChartTest(unittest.TestCase):
    def test_a_three_row_timeline_draws_three_charts(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / 'timeline.csv').write_text(TIMELINE)
            written = soak_chart.draw(Path(tmp))
            self.assertEqual(sorted(p.name for p in written), ['pickup-p99.svg', 'table.svg', 'throughput.svg'])
            expect = {
                'throughput.svg': 'Completed jobs/s',
                'pickup-p99.svg': 'p99 latency (ms)',
                'table.svg': 'jobs table per shard',
            }
            for name, title in expect.items():
                svg = (Path(tmp) / name).read_text()
                self.assertEqual(titles(svg)[0], title, name)  # also proves the SVG parses

    def test_throughput_plots_every_sample(self):
        rows = soak_chart.read(TIMELINE.splitlines())
        svg = soak_chart.throughput(rows)
        self.assertEqual(svg.count('<circle'), 3)
        self.assertIn('201.7', svg)

    def test_a_blank_pickup_sample_is_skipped_not_drawn_as_zero(self):
        rows = soak_chart.read(TIMELINE.splitlines())
        series = dict(soak_chart.latency_series(rows))
        self.assertEqual(series['pickup p99'], [(30.0, 41.0), (90.0, 55.5)])
        self.assertEqual(len(series['enqueue p99']), 3)

    def test_no_pickup_at_all_says_so(self):
        rows = soak_chart.read([l.replace(',41.0,', ',,').replace(',55.5,', ',,') for l in TIMELINE.splitlines()])
        self.assertIn('no pickup samples', soak_chart.latency(rows))

    def test_the_table_chart_has_a_line_per_shard_in_each_panel(self):
        rows = soak_chart.read(TIMELINE.splitlines())
        self.assertEqual(soak_chart.shards(rows), [0, 1])
        svg = soak_chart.table(rows)
        self.assertEqual(svg.count('<polyline'), 4, 'size and dead tuples, two shards each')
        self.assertIn('>shard 1<', svg)

    def test_the_script_takes_the_evidence_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / 'timeline.csv').write_text(TIMELINE)
            subprocess.run([sys.executable, str(Path(soak_chart.__file__)), tmp], check=True)
            self.assertTrue((Path(tmp) / 'table.svg').exists())


if __name__ == '__main__':
    unittest.main()
