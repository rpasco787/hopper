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
    def test_batch_size_and_fallback_share_show_per_kind(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], ack_batch='10.0', heartbeat_batch='8.0',
                                           ack_fallback_pct='0.5', heartbeat_fallback_pct='0.0'))
        self.assertIn('| 10.0/8.0 | 0.5/0.0 |', out)

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
        self.assertIn('2 (want 5)', out)

    def test_each_setup_is_its_own_section(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], vm_cpus=4) + runs(1, [1, 2, 3], vm_cpus=8))
        self.assertEqual(out.count('\n## '), 1)  # the second heading; the first starts the text
        self.assertTrue(out.startswith('## '))

    def test_dirty_refs_are_marked(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], ref='abc1234-dirty'))
        self.assertIn('dirty tree', out)

    def test_four_brokers_get_their_own_verdict(self):
        out = bench_summary.summarize(runs(1, [5000, 5100, 5200]) + runs(4, [6000, 6100, 6200]))
        self.assertIn('**Verdict:** 4 brokers faster; the 4-broker median is 1.20× the 1-broker median.', out)

    def test_variants_are_separate_sections(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], variant='batch') + runs(1, [1, 2, 3], variant='nobatch'))
        self.assertIn('variant=batch', out)
        self.assertIn('variant=nobatch', out)

    def test_commit_and_latency_columns_show_medians(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], commits_per_job='2.20', ack_p99_ms='1.50'))
        self.assertIn('| 2.20 |', out)
        self.assertIn('| 1.50 |', out)

    def test_columns_an_old_file_lacks_are_dashes(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3]))
        self.assertIn('| – |', out)

    def test_four_pinned_brokers_share_two_cores(self):
        def row4(out):
            return next(l for l in out.splitlines() if l.startswith('| 4 |'))
        # Four brokers on CPUs 2-3 saturate at 90% of two cores: 180 in total.
        self.assertTrue(row4(bench_summary.summarize(runs(4, [1, 2, 3], pinned=1, broker_cpu=170))).endswith('| none |'))
        self.assertTrue(row4(bench_summary.summarize(runs(4, [1, 2, 3], pinned=1, broker_cpu=185))).endswith('| brokers |'))

    def test_one_pinned_broker_has_both_cores(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], pinned=1, broker_cpu=170))
        row1 = next(l for l in out.splitlines() if l.startswith('| 1 |'))
        self.assertTrue(row1.endswith('| none |'), row1)

    def test_ranges_apart_but_medians_close_are_no_measurable_difference(self):
        # The evidence rule wants 10%; these medians are 2% apart.
        out = bench_summary.summarize(runs(1, [5000, 5010, 5020]) + runs(2, [5100, 5110, 5120]))
        self.assertIn('**Verdict:** no measurable difference', out)

    def test_pool_size_and_harness_are_part_of_the_setup(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], db_max_conns='16') + runs(1, [1, 2, 3], db_max_conns='32'))
        self.assertEqual(out.count('\n## '), 1)
        out = bench_summary.summarize(runs(1, [1, 2, 3], harness='abc1234') + runs(1, [1, 2, 3], harness='def5678'))
        self.assertEqual(out.count('\n## '), 1)

    def test_a_dirty_harness_is_marked(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], harness='abc1234-dirty'))
        self.assertIn('uncommitted harness', out)

    def test_setup_splits_by_worker_sdk_and_memory_limit(self):
        out = bench_summary.summarize(runs(1, [1, 2, 3], worker_sdk='go', worker_mem='384m')
                                      + runs(1, [4, 5, 6], worker_sdk='java', worker_mem='384m'))
        self.assertIn('worker_sdk=go', out)
        self.assertIn('worker_sdk=java', out)
        self.assertIn('worker_mem=384m', out)

    def test_worker_columns_show_in_their_own_table(self):
        out = bench_summary.summarize(runs(1, [9000, 9100, 9200], worker_sdk='java', warm_throughput=9500,
                                           max_leased=128, worker_cpu_ms_per_1k=1500, worker_peak_rss_mib=110.5,
                                           worker_threads=31, startup_ms=640))
        self.assertIn('| 1 | 3 | 9,500 (9,500–9,500) | 128 | 1,500 | 110.5 | 31 | 640 |', out)

    def test_rows_without_worker_columns_get_no_worker_table(self):
        self.assertNotIn('worker CPU ms per 1k jobs', bench_summary.summarize(runs(1, [1, 2, 3])))

    def test_values_bench_workers_could_not_compute_are_left_out_of_medians(self):
        # scripts/bench_workers.py writes '-' for a startup no broker logged.
        rs = runs(1, [9000, 9100], worker_cpu_ms_per_1k=1500, worker_peak_rss_mib=110.5,
                  worker_threads=31, startup_ms=640)
        rs.append(row(throughput=9200, worker_cpu_ms_per_1k='-', worker_peak_rss_mib='120.5',
                      worker_threads='31', startup_ms='-'))
        out = bench_summary.summarize(rs)
        self.assertIn('| 1 | 3 | – | – | 1,500 | 110.5 | 31 | 640 |', out)


if __name__ == '__main__':
    unittest.main()
