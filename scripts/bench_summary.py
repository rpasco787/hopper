#!/usr/bin/env python3
"""Medians, ranges, saturation and a 1-vs-2-broker verdict for scripts/bench.sh.

    python3 scripts/bench_summary.py docs/bench/lite/e1/results.csv > docs/bench/lite/e1/summary.md

Rows are grouped by everything that must match for runs to be comparable;
the broker count is the only thing that varies inside a group.
"""
import csv
import statistics
import sys
from collections import defaultdict

SETUP = ('ref', 'scenario', 'workers', 'concurrency', 'vm_cpus', 'pinned')
# Under PIN=1, a role is saturated at 90% of the cores scripts/bench.sh gives
# it, in percent of one core.
PINNED_LIMITS = (('postgres', 'postgres_cpu', 180), ('broker', 'per_broker_cpu', 90),
                 ('workers', 'worker_cpu', 270))
HEADER = ('| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU '
          '| on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | saturated |')
RULE = '|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|'


def fmt(x, digits=0):
    return f'{x:,.{digits}f}'


def verdict(one, two):
    if max(two) < min(one):
        return '2 brokers slower'
    if min(two) > max(one):
        return '2 brokers faster'
    return 'inconclusive (ranges overlap)'


def saturation(cpu, brokers, vm_cpus, pinned):
    if pinned:
        return ', '.join(name for name, key, limit in PINNED_LIMITS if cpu[key] >= limit) or 'none'
    total = cpu['postgres_cpu'] + cpu['per_broker_cpu'] * brokers + cpu['worker_cpu']
    return 'VM' if total >= 0.9 * 100 * vm_cpus else 'none'


def summarize(rows):
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        groups[tuple(r[k] for k in SETUP)][int(r['brokers'])].append(r)
    lines = []
    for setup in sorted(groups):
        fields = dict(zip(SETUP, setup))
        lines.append('## ' + ', '.join(f'{k}={v}' for k, v in fields.items()))
        lines.append('')
        if fields['ref'].endswith('-dirty'):
            lines += ['> Built from a dirty tree: not a result.', '']
        lines += [HEADER, RULE]
        throughputs = {}
        for brokers in sorted(groups[setup]):
            rs = groups[setup][brokers]

            def med(key):
                return statistics.median(float(r[key]) for r in rs)

            ts = [float(r['throughput']) for r in rs]
            throughputs[brokers] = ts
            cpu = {'postgres_cpu': med('postgres_cpu'), 'per_broker_cpu': med('broker_cpu') / brokers,
                   'worker_cpu': med('worker_cpu')}
            cells = [
                str(brokers),
                str(len(rs)) if len(rs) == 3 else f'{len(rs)} (want 3)',
                f'{fmt(statistics.median(ts))} ({fmt(min(ts))}–{fmt(max(ts))})',
                fmt(cpu['postgres_cpu']), fmt(cpu['per_broker_cpu']), fmt(cpu['worker_cpu']),
                fmt(med('on_cpu_pct'), 1), fmt(med('lock_pct') + med('lwlock_pct'), 1),
                fmt(med('io_pct'), 1), fmt(med('lease_mean_ms'), 3), fmt(med('lease_blks_per_call'), 1),
                saturation(cpu, brokers, int(fields['vm_cpus']), fields['pinned'] == '1'),
            ]
            lines.append('| ' + ' | '.join(cells) + ' |')
        lines.append('')
        if 1 in throughputs and 2 in throughputs:
            ratio = statistics.median(throughputs[2]) / statistics.median(throughputs[1])
            lines += [f'**Verdict:** {verdict(throughputs[1], throughputs[2])}; '
                      f'the 2-broker median is {ratio:.2f}× the 1-broker median.', '']
    return '\n'.join(lines)


def main():
    if len(sys.argv) != 2:
        sys.exit('usage: scripts/bench_summary.py results.csv')
    with open(sys.argv[1]) as f:
        print(summarize(list(csv.DictReader(f))))


if __name__ == '__main__':
    main()
