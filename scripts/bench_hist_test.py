"""Checks for scripts/bench_hist.py; run with python3 scripts/bench_hist_test.py."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_hist  # noqa: E402

B = 'grpc_server_handling_seconds_bucket{grpc_method="%s",grpc_service="hopper.v1.HopperService",grpc_type="unary",le="%s"} %d'


def snapshot(phase, port, method, counts):
    lines = [f'# {phase} :{port}']
    lines += [B % (method, le, n) for le, n in counts]
    return '\n'.join(lines)


class HistTest(unittest.TestCase):
    def test_interpolates_inside_the_bucket(self):
        text = '\n'.join([
            snapshot('before', 1, 'Ack', [('0.001', 0), ('0.0025', 0), ('+Inf', 0)]),
            snapshot('after', 1, 'Ack', [('0.001', 90), ('0.0025', 100), ('+Inf', 100)]),
        ])
        ms, calls = bench_hist.p99(text, 'Ack')
        self.assertAlmostEqual(ms, 2.35, places=6)
        self.assertEqual(calls, 100)

    def test_pairs_each_broker_then_sums(self):
        text = '\n'.join([
            snapshot('before', 1, 'Ack', [('0.001', 50), ('+Inf', 50)]),
            snapshot('before', 2, 'Ack', [('0.001', 50), ('+Inf', 50)]),
            snapshot('after', 1, 'Ack', [('0.001', 100), ('+Inf', 100)]),
            snapshot('after', 2, 'Ack', [('0.001', 150), ('+Inf', 150)]),
        ])
        # 150 new calls, all at or under 1 ms.
        ms, calls = bench_hist.p99(text, 'Ack')
        self.assertAlmostEqual(ms, 0.99, places=6)
        self.assertEqual(calls, 150)

    def test_a_method_with_no_calls_has_no_p99(self):
        text = '\n'.join([snapshot('before', 1, 'Heartbeat', [('0.001', 0), ('+Inf', 0)]),
                          snapshot('after', 1, 'Heartbeat', [('0.001', 5), ('+Inf', 5)])])
        self.assertEqual(bench_hist.p99(text, 'Ack'), (None, 0))
        self.assertEqual(bench_hist.p99('', 'Ack'), (None, 0))

    def test_a_broker_missing_a_snapshot_is_incomplete(self):
        text = '\n'.join([
            snapshot('before', 1, 'Ack', [('+Inf', 0)]),
            snapshot('before', 2, 'Ack', [('+Inf', 0)]),
            snapshot('after', 1, 'Ack', [('+Inf', 100)]),
        ])
        with self.assertRaisesRegex(bench_hist.Incomplete, ':2'):
            bench_hist.p99(text, 'Ack')

    def test_a_failed_scrape_is_incomplete(self):
        text = '\n'.join([snapshot('before', 1, 'Ack', [('+Inf', 0)]), '# after :1', '# scrape failed'])
        with self.assertRaisesRegex(bench_hist.Incomplete, ':1 after scrape failed'):
            bench_hist.p99(text, 'Ack')

    def test_counts_that_go_down_are_incomplete(self):
        # A broker that restarts mid-run starts its counters again from zero.
        text = '\n'.join([snapshot('before', 1, 'Ack', [('0.001', 90), ('+Inf', 100)]),
                          snapshot('after', 1, 'Ack', [('0.001', 30), ('+Inf', 40)])])
        with self.assertRaisesRegex(bench_hist.Incomplete, 'went down'):
            bench_hist.p99(text, 'Ack')


if __name__ == '__main__':
    unittest.main()
