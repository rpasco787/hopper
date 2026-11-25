#!/usr/bin/env python3
"""Phase 3d pilot: how many jobs a drain takes to reach its pace (docs/bench/3d/README.txt).

    python3 docs/bench/3d/pilot/warmup.py <rate.psv> [<rate.psv> ...]

For each drain it reads completions per second from rate.psv (tenths of a
second from the first lease). Its pace is the median of the seconds in the
second half, leaving out the last second (the tail, as streams run dry). It is
warm from the first second after which every second up to the last runs at 90%
of that pace or more. The script prints each drain's warm second and the jobs
completed before it, then WARM_JOBS: the largest of those, rounded up to the
next 10,000, and at least 10,000.
"""
import math
import statistics
import sys
from collections import Counter


def per_second(path):
    per = Counter()
    for line in open(path):
        if line.startswith('#') or not line.strip():
            continue
        tenths, n = line.strip().split('|')
        per[int(tenths) // 10] += int(n)
    return [per.get(s, 0) for s in range(max(per) + 1)]


def warm_point(per):
    body = per[:-1]
    pace = statistics.median(body[len(body) // 2:])
    for s in range(len(body)):
        if all(x >= 0.9 * pace for x in body[s:]):
            return s, sum(per[:s]), pace
    return len(body), sum(body), pace


def main():
    worst = 0
    for path in sys.argv[1:]:
        s, before, pace = warm_point(per_second(path))
        print(f'{path}: pace {pace:,.0f} jobs/s, warm from second {s}, {before:,} jobs before it')
        worst = max(worst, before)
    print(f'WARM_JOBS={max(10000, math.ceil(worst / 10000) * 10000)}')


if __name__ == '__main__':
    main()
