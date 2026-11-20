#!/usr/bin/env python3
"""Evaluate the H2 written rule; fallback gating uses exact retained counts."""
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parent
repo = root.parents[3]
sys.path.insert(0, str(repo / 'scripts'))
import bench_hist

rows = list(csv.DictReader((root / 'results.csv').open()))
assert len(rows) == 60, f'Expected 60 accepted runs, got {len(rows)}'
groups = defaultdict(list)
for r in rows:
    assert r['ref'] == r['harness'] == '3aaf0c1', (r['ref'], r['harness'])
    assert (r['workers'], r['concurrency'], r['payload_bytes'], r['pinned'], r['retried']) == ('8', '16', '300', '1', '0')
    v = 'async' if r['sync_commit'] == 'off' else r['variant']
    evidence = repo / r['evidence']
    counts = {}
    for line in (evidence / 'confirmations.txt').read_text().splitlines()[1:]:
        kind, calls, applied = line.split('|')
        counts[kind] = (int(calls), int(applied))
    for kind, rpc in [('ack', 'Ack'), ('heartbeat', 'Heartbeat')]:
        _, calls = bench_hist.p99((evidence / 'rpc.txt').read_text(), rpc)
        r[kind + '_rpc_calls'] = calls
        batched = counts.get(kind + '_batch', (0, 0))[0]
        singles = counts.get(kind + '_one', (0, 0))[0]
        r[kind + '_exact_fallback_pct'] = 100 * singles / calls if calls and batched else None
        if v.startswith('batch'):
            assert batched > 0 and calls > 0, ('NO BATCH STATEMENT', evidence, kind)
            assert abs(float(r[kind + '_fallback_pct']) - r[kind + '_exact_fallback_pct']) <= 0.051
        r[kind + '_statements_per_job'] = (batched + singles) / 40000
    r['confirmation_statements_per_job'] = sum(c[0] for c in counts.values()) / 40000
    r['flush_wait_pct'] = sum(float(parts[4]) for line in (evidence / 'waits.txt').read_text().splitlines()
                              if (parts := line.split('|'))[0] == 'event' and parts[2] in ('WALWrite', 'WALSync'))
    groups[v, int(r['brokers'])].append(r)
assert set(groups) == {(v, b) for v in ('nobatch', 'batch', 'batch-flights1', 'async') for b in (1, 2, 4)}
assert all(len(rs) == 5 for rs in groups.values())

def vals(rs, k):
    return [float(r[k]) for r in rs if r.get(k) not in ('', None)]

def med(rs, k):
    vs = vals(rs, k)
    return statistics.median(vs) if vs else None

verdicts, decisions = [], {}
for v in ('batch', 'batch-flights1', 'async'):
    helps_somewhere = worse_somewhere = too_many_fallbacks = False
    for b in (1, 2, 4):
        ctl, trt = groups['nobatch', b], groups[v, b]
        tc, tt = vals(ctl, 'throughput'), vals(trt, 'throughput')
        ratio = statistics.median(tt) / statistics.median(tc)
        helps = ratio >= 1.10 and min(tt) > max(tc)
        p99 = {k: med(trt, k) / med(ctl, k) for k in ('ack_p99_ms', 'heartbeat_p99_ms') if med(trt, k) is not None and med(ctl, k)}
        worse = ratio < 0.95 or any(x > 1.5 for x in p99.values())
        fb = {k: med(trt, k) for k in ('ack_exact_fallback_pct', 'heartbeat_exact_fallback_pct') if med(trt, k) is not None}
        helps_somewhere |= helps
        worse_somewhere |= worse
        too_many_fallbacks |= any(x > 10 for x in fb.values())
        verdicts.append(f'{v:15} b{b}: {statistics.median(tt):6,.0f} ({min(tt):,.0f}-{max(tt):,.0f}) '
                        f'vs {statistics.median(tc):6,.0f} ({min(tc):,.0f}-{max(tc):,.0f})  {ratio:.2f}x  '
                        f'{"helps" if helps else "WORSE" if worse else "no measurable gain"}  p99x {p99}  exact fallback% {fb}')
    decisions[v] = dict(helps_somewhere=helps_somewhere, worse_somewhere=worse_somewhere,
                        too_many_fallbacks=too_many_fallbacks,
                        qualifies=helps_somewhere and not worse_somewhere and not too_many_fallbacks)
    if v != 'async':
        verdicts.append(f'  -> {v} qualifies: {decisions[v]["qualifies"]}')
(root / 'decision.txt').write_text('\n'.join(verdicts) + '\n')
(root / 'decision.json').write_text(json.dumps(decisions, indent=2) + '\n')
print('\n'.join(verdicts))

lines = ['# H2 comparison', '', 'Five-run medians; ranges in parentheses. Each variant uses code and harness `3aaf0c1`.',
         'Confirmation statements/job includes Ack and Heartbeat batch and one-row statements. Fallback shares use exact counts.',
         'RPC p99 values interpolate within fixed histogram buckets: 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000 ms.', '']
for b in (1, 2, 4):
    lines += [f'## {b} broker(s)', '',
              '| Variant | Jobs/s (range) | Commits/job | WAL syncs/job | Confirmation statements/job | WALWrite+WALSync % | Active backends | Ack/HB p99 ms | Submitted batch Ack/HB | Exact fallback % Ack/HB |',
              '|---|---|---:|---:|---:|---:|---:|---|---|---|']
    for v in ('nobatch', 'batch', 'batch-flights1', 'async'):
        rs = groups[v, b]
        ts = vals(rs, 'throughput')
        batch = '/'.join(f'{med(rs, k):.1f}' for k in ('ack_batch', 'heartbeat_batch')) if v.startswith('batch') else '–'
        fb = '/'.join(f'{med(rs, k):.3f}' for k in ('ack_exact_fallback_pct', 'heartbeat_exact_fallback_pct')) if v.startswith('batch') else '–'
        lines.append(f'| {v} | {statistics.median(ts):,.0f} ({min(ts):,.0f}–{max(ts):,.0f}) | {med(rs,"commits_per_job"):.2f} | '
                     f'{med(rs,"wal_syncs_per_job"):.3f} | {med(rs,"confirmation_statements_per_job"):.3f} | '
                     f'{med(rs,"flush_wait_pct"):.1f} | {med(rs,"active_backends"):.2f} | '
                     f'{med(rs,"ack_p99_ms"):.2f}/{med(rs,"heartbeat_p99_ms"):.2f} | {batch} | {fb} |')
    lines.append('')
(root / 'comparison.md').write_text('\n'.join(lines) + '\n')
