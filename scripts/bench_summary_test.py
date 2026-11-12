"""Checks for scripts/bench_summary.py; run with python3 scripts/bench_summary_test.py."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_summary  # noqa: E402


def row(**kw):
    r = dict(ref='abc1234', scenario='drain', workers='8', concurrency='16', vm_cpus='8',
             pinned='0', brokers='1', throughput='9000', postgres_cpu='100', broker_cpu='50',
             worker_cpu='100', on_cpu_pct='50.0', lock_pct='1.0', lwlock_pct='4.0', io_pct='20.0',
             lease_mean_ms='0.400', lease_blks_per_call='10.0')
    r.update({k: str(v) for k, v in kw.items()})
    return r


def runs(brokers, throughputs, **kw):
    return [row(brokers=brokers, throughput=t, **kw) for t in throughputs]


class SummaryTest(unittest.TestCase):
    def test_separated_ranges_say_two_brokers_slower(self):
        out = bench_summary.summarize(runs(1, [9000, 9100, 9200]) + runs(2, [5000, 5100, 5200]))
        self.assertIn('**Verdict:** 2 brokers slower', out)
        self.assertIn('0.56×', out)
        self.assertIn('9,100 (9,000–9,200)', out)

    def test_overlapping_ranges_are_inconclusive(self):
        out = bench_summary.summarize(runs(1, [9000, 9100, 9200]) + runs(2, [8900, 9050, 9300]))
        self.assertIn('inconclusive (ranges overlap)', out)

    def test_pinned_postgres_at_its_two_cores_is_saturated(self):
        out = bench_summary.summarize(runs(1, [9000, 9100, 9200], pinned=1, postgres_cpu=190))
        self.assertIn('| postgres |', out)

    def test_unpinned_vm_near_its_cpus_is_saturated(self):
        out = bench_summary.summarize(runs(1, [9000, 9100, 9200], vm_cpus=4, postgres_cpu=150,
                                           broker_cpu=150, worker_cpu=80))
        self.assertIn('| VM |', out)

    def test_cpu_per_broker_divides_by_broker_count(self):
        out = bench_summary.summarize(runs(2, [5000, 5100, 5200], broker_cpu=180))
        self.assertIn('| 90 |', out)

    def test_short_groups_are_flagged(self):
        out = bench_summary.summarize(runs(1, [9000, 9100]))
        self.assertIn('2 (want 3)', out)

    def test_each_setup_is_its_own_section(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], vm_cpus=4) + runs(1, [1, 2, 3], vm_cpus=8))
        self.assertEqual(out.count('\n## '), 1)  # the second heading; the first starts the text
        self.assertTrue(out.startswith('## '))

    def test_dirty_refs_are_marked(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], ref='abc1234-dirty'))
        self.assertIn('dirty tree', out)


if __name__ == '__main__':
    unittest.main()
