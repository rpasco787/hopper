#!/usr/bin/env python3
"""Medians, ranges, saturation and broker-count verdicts for scripts/bench.sh.

    python3 scripts/bench_summary.py docs/bench/lite/e1/results.csv > docs/bench/lite/e1/summary.md

Rows are grouped by everything that must match for runs to be comparable;
the broker count is the only thing that varies inside a group. Columns an
older results.csv lacks show as a dash.
"""
import csv
import statistics
import sys
from collections import defaultdict

# Everything that defines a configuration. Settings with no column of their
# own (BROKER_ENV, WORKER_ENV, PG_ARGS, BENCH_SQL, EXPLAIN) are folded into
# variant: scripts/bench.sh refuses them without one.
SETUP = ('ref', 'harness', 'scenario', 'variant', 'payload_bytes', 'sync_commit', 'db_max_conns',
         'workers', 'concurrency', 'vm_cpus', 'pinned', 'worker_sdk', 'worker_mem')
WANT = 5
# Under PIN=1, a role is saturated at 90% of the cores scripts/bench.sh gives
# it, in percent of one core. Every broker may use CPUs 2 and 3, so the
# brokers together saturate at 180 however many there are.
PINNED_LIMITS = (('postgres', 'postgres_cpu', 180), ('workers', 'worker_cpu', 270))
BROKERS_LIMIT = 180
# The evidence rule: medians at least 10% apart, and ranges that don't overlap.
MEASURABLE = 0.10
HEADER = ('| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU '
          '| on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job '
          '| WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |')
RULE = '|---:|---:|---|' + '---:|' * 16 + '---|'
WORKER_HEADER = ('| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs '
                 '| peak RSS MiB per worker | threads per worker | startup ms |')
WORKER_RULE = '|---:|---:|---|---:|---:|---:|---:|---:|'
# What scripts/bench_workers.py writes for a value it could not compute.
UNKNOWN = '-'


def fmt(x, digits=0):
    return '–' if x is None else f'{x:,.{digits}f}'


def pair(a, b, digits=1):
    return '–' if a is None and b is None else f'{fmt(a, digits)}/{fmt(b, digits)}'


def median_of(rs, key):
    vals = [float(r[key]) for r in rs if r.get(key) not in (None, '', UNKNOWN)]
    return statistics.median(vals) if vals else None


def verdict(one, other, n):
    ratio = statistics.median(other) / statistics.median(one)
    if not (max(other) < min(one) or min(other) > max(one)):
        return 'inconclusive (ranges overlap)'
    if ratio >= 1 + MEASURABLE:
        return f'{n} brokers faster'
    if ratio <= 1 - MEASURABLE:
        return f'{n} brokers slower'
    return 'no measurable difference (ranges apart, medians under 10% apart)'


def saturation(cpu, brokers, vm_cpus, pinned):
    if pinned:
        hot = [name for name, key, limit in PINNED_LIMITS if cpu[key] >= limit]
        if cpu['per_broker_cpu'] * brokers >= BROKERS_LIMIT:
            hot.append('brokers')
        return ', '.join(hot) or 'none'
    total = cpu['postgres_cpu'] + cpu['per_broker_cpu'] * brokers + cpu['worker_cpu']
    return 'VM' if total >= 0.9 * 100 * vm_cpus else 'none'


def summarize(rows):
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        groups[tuple(r.get(k, '') for k in SETUP)][int(r['brokers'])].append(r)
    lines = []
    for setup in sorted(groups):
        fields = dict(zip(SETUP, setup))
        lines.append('## ' + ', '.join(f'{k}={v}' for k, v in fields.items() if v != ''))
        lines.append('')
        if fields['ref'].endswith('-dirty'):
            lines += ['> Built from a dirty tree: not a result.', '']
        if fields['harness'].endswith('-dirty'):
            lines += ['> Measured with an uncommitted harness: not a result.', '']
        lines += [HEADER, RULE]
        throughputs = {}
        for brokers in sorted(groups[setup]):
            rs = groups[setup][brokers]

            def med(key):
                return median_of(rs, key)

            ts = [float(r['throughput']) for r in rs]
            throughputs[brokers] = ts
            cpu = {'postgres_cpu': med('postgres_cpu'), 'per_broker_cpu': med('broker_cpu') / brokers,
                   'worker_cpu': med('worker_cpu')}
            lock = None if med('lock_pct') is None else med('lock_pct') + med('lwlock_pct')
            cells = [
                str(brokers),
                str(len(rs)) if len(rs) >= WANT else f'{len(rs)} (want {WANT})',
                f'{fmt(statistics.median(ts))} ({fmt(min(ts))}–{fmt(max(ts))})',
                fmt(cpu['postgres_cpu']), fmt(cpu['per_broker_cpu']), fmt(cpu['worker_cpu']),
                fmt(med('on_cpu_pct'), 1), fmt(lock, 1), fmt(med('io_pct'), 1),
                fmt(med('lease_mean_ms'), 3), fmt(med('lease_blks_per_call'), 1),
                fmt(med('commits_per_job'), 2), fmt(med('wal_syncs_per_job'), 3),
                fmt(med('wal_kb_per_job'), 2), fmt(med('hot_update_pct'), 1),
                fmt(med('ack_p99_ms'), 2), fmt(med('heartbeat_p99_ms'), 2),
                pair(med('ack_batch'), med('heartbeat_batch')),
                pair(med('ack_fallback_pct'), med('heartbeat_fallback_pct')),
                saturation(cpu, brokers, int(fields['vm_cpus']), fields['pinned'] == '1'),
            ]
            lines.append('| ' + ' | '.join(cells) + ' |')
        lines.append('')
        if any(r.get('worker_cpu_ms_per_1k') for b in groups[setup] for r in groups[setup][b]):
            lines += [WORKER_HEADER, WORKER_RULE]
            for brokers in sorted(groups[setup]):
                rs = groups[setup][brokers]
                warm = [float(r['warm_throughput']) for r in rs if r.get('warm_throughput') not in (None, '', UNKNOWN)]
                cells = [str(brokers), str(len(rs)),
                         f'{fmt(statistics.median(warm))} ({fmt(min(warm))}–{fmt(max(warm))})' if warm else '–',
                         fmt(median_of(rs, 'max_leased')), fmt(median_of(rs, 'worker_cpu_ms_per_1k')),
                         fmt(median_of(rs, 'worker_peak_rss_mib'), 1), fmt(median_of(rs, 'worker_threads')),
                         fmt(median_of(rs, 'startup_ms'))]
                lines.append('| ' + ' | '.join(cells) + ' |')
            lines.append('')
        if 1 in throughputs and len(throughputs) > 1:
            for n in sorted(throughputs):
                if n == 1:
                    continue
                ratio = statistics.median(throughputs[n]) / statistics.median(throughputs[1])
                lines.append(f'**Verdict:** {verdict(throughputs[1], throughputs[n], n)}; '
                             f'the {n}-broker median is {ratio:.2f}× the 1-broker median.')
            lines.append('')
    return '\n'.join(lines)


def main():
    if len(sys.argv) != 2:
        sys.exit('usage: scripts/bench_summary.py results.csv')
    with open(sys.argv[1]) as f:
        print(summarize(list(csv.DictReader(f))))


if __name__ == '__main__':
    main()
