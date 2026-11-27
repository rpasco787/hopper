#!/usr/bin/env python3
"""Compare a treatment variant with a control, per broker count, by the evidence rule.

    python3 scripts/bench_compare.py docs/bench/1b/h4/results.csv check skip
    python3 scripts/bench_compare.py docs/bench/1b/h5/results.csv wakeups-on wakeups-off \\
        --metric enqueue_p99_ms --lower-is-better

At each broker count both variants ran, the treatment *helps* when its median is
at least 10% better than the control's and the two ranges don't overlap. It is
*worse* when its median is more than 5% worse. Anything else is *no measurable
effect*. Both variants must come from the same interleaved loop
(docs/scaling.md). --where KEY=VALUE keeps only rows with that column value,
for loops where a variant name alone doesn't identify a configuration.
--column KEY picks control and treatment by another column than variant, as
for one shard against two:

    python3 scripts/bench_compare.py docs/bench/4/results.csv 1 2 --column shards --where variant=w8c16
"""
import argparse
import csv
import statistics
import sys
from collections import defaultdict


def values(path, variant, metric, where=(), column='variant'):
    by = defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            if any(r.get(k, '') != v for k, v in where):
                continue
            if r.get(column, '') == variant and r.get(metric, '') != '':
                by[int(r['brokers'])].append(float(r[metric]))
    return by


def verdict(control, treatment, lower_is_better=False):
    mc, mt = statistics.median(control), statistics.median(treatment)
    if lower_is_better:
        gain = mc / mt if mt else float('inf')
        apart = max(treatment) < min(control)
    else:
        gain = mt / mc if mc else float('inf')
        apart = min(treatment) > max(control)
    if gain >= 1.10 and apart:
        return gain, 'helps'
    if gain < 0.95:
        return gain, 'worse'
    return gain, 'no measurable effect'


def fmt(vs):
    return f'{statistics.median(vs):,.0f} ({min(vs):,.0f}-{max(vs):,.0f})' if max(vs) >= 100 else \
        f'{statistics.median(vs):.2f} ({min(vs):.2f}-{max(vs):.2f})'


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('results')
    p.add_argument('control')
    p.add_argument('treatment')
    p.add_argument('--metric', default='throughput')
    p.add_argument('--lower-is-better', action='store_true')
    p.add_argument('--where', action='append', default=[], metavar='KEY=VALUE')
    p.add_argument('--column', default='variant', help='the column control and treatment are values of')
    a = p.parse_args(argv)
    where = []
    for w in a.where:
        k, sep, v = w.partition('=')
        if not sep:
            sys.exit(f'--where needs KEY=VALUE, got {w!r}')
        where.append((k, v))
    ctl = values(a.results, a.control, a.metric, where, a.column)
    trt = values(a.results, a.treatment, a.metric, where, a.column)
    counts = sorted(set(ctl) & set(trt))
    if not counts:
        sys.exit(f'no broker count has both {a.control!r} and {a.treatment!r} rows with {a.metric}')
    for b in counts:
        gain, word = verdict(ctl[b], trt[b], a.lower_is_better)
        print(f'b{b} {a.metric}: {a.treatment} {fmt(trt[b])} vs {a.control} {fmt(ctl[b])}  '
              f'n={len(trt[b])}/{len(ctl[b])}  {gain:.3f}x  {word}')


if __name__ == '__main__':
    main()
