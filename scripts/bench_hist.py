#!/usr/bin/env python3
"""p99 handling time of one gRPC method across brokers, from scripts/bench.sh's rpc.txt.

    python3 scripts/bench_hist.py rpc.txt Ack    # prints "CALLS P99_MS", or "0" with no calls

rpc.txt holds a '# before :PORT' and an '# after :PORT' section per broker of
grpc_server_handling_seconds_bucket lines, or '# scrape failed' in place of
the lines. Each broker's after is paired with its own before. A broker missing
either, a failed scrape, or a count that went down (a restart resets them)
makes the measurement incomplete: exit status 1 with the reason. The answer
interpolates inside the bucket, as PromQL's histogram_quantile does, so it is
only as fine as internal/metrics.RPCBuckets.
"""
import re
import sys
from collections import defaultdict

HEAD = re.compile(r'# (?P<phase>before|after) (?P<port>:\d+)$')
LINE = re.compile(r'grpc_server_handling_seconds_bucket\{(?P<labels>[^}]*)\} (?P<value>\S+)$')


class Incomplete(Exception):
    """The snapshots don't cover the whole run for every broker."""


def sections(text, method):
    """{(phase, port): {upper bound: cumulative count}} for one method."""
    out, key = {}, None
    for line in text.splitlines():
        head = HEAD.match(line)
        if head:
            key = (head['phase'], head['port'])
            out[key] = {}
            continue
        if line == '# scrape failed':
            raise Incomplete(f'{key[1]} {key[0]} scrape failed' if key else 'scrape failed')
        m = LINE.match(line)
        if not m or key is None:
            continue
        labels = dict(kv.split('=', 1) for kv in m['labels'].split(','))
        if labels.get('grpc_method') != f'"{method}"':
            continue
        le = labels['le'].strip('"')
        out[key][float('inf') if le == '+Inf' else float(le)] = float(m['value'])
    return out


def buckets(text, method):
    """(upper bound, cumulative count) pairs over the run: each broker's after
    minus its own before, then summed over brokers."""
    secs = sections(text, method)
    total = defaultdict(float)
    for port in sorted({port for _, port in secs}):
        if ('before', port) not in secs or ('after', port) not in secs:
            raise Incomplete(f'{port} lacks a before or an after snapshot')
        before, after = secs[('before', port)], secs[('after', port)]
        for le in set(before) | set(after):
            d = after.get(le, 0.0) - before.get(le, 0.0)
            if d < 0:
                raise Incomplete(f'{port} counts went down; did the broker restart?')
            total[le] += d
    return sorted(total.items())


def p99(text, method):
    """(milliseconds, calls) over the run; milliseconds is None with no calls."""
    bs = buckets(text, method)
    calls = bs[-1][1] if bs else 0.0
    if calls <= 0:
        return None, 0
    rank = 0.99 * calls
    lo_le, lo_n = 0.0, 0.0
    for le, n in bs:
        if n >= rank:
            if le == float('inf'):
                return lo_le * 1000, calls
            if n == lo_n:
                return le * 1000, calls
            return (lo_le + (le - lo_le) * (rank - lo_n) / (n - lo_n)) * 1000, calls
        lo_le, lo_n = le, n
    return None, calls


def main():
    if len(sys.argv) != 3:
        sys.exit('usage: scripts/bench_hist.py rpc.txt METHOD')
    with open(sys.argv[1]) as f:
        text = f.read()
    try:
        ms, calls = p99(text, sys.argv[2])
    except Incomplete as e:
        sys.exit(f'{sys.argv[2]} histograms: {e}')
    print(f'{calls:.0f}' + ('' if ms is None else f' {ms:.2f}'))


if __name__ == '__main__':
    main()
