#!/usr/bin/env python3
"""Drain throughput against broker count (or another column), one line per series, as SVG.

    python3 scripts/bench_chart.py "before (e61ae1c)=docs/bench/1b/final/results.csv#before" \\
        "after (abc1234)=docs/bench/1b/final/results.csv#after" > docs/img/scaling-laptop.svg

A series is NAME=RESULTS.csv#VARIANT: the median drain throughput per broker
count over that variant's rows (no #VARIANT means rows with no variant). Put
every series in one interleaved loop; docs/scaling.md says why. --x COLUMN
puts another column on the x axis, such as shards:

    python3 scripts/bench_chart.py --x shards "8 x 16=docs/bench/4/results.csv#w8c16" > docs/img/shards-laptop.svg
"""
import csv
import statistics
import sys
from collections import defaultdict
from xml.sax.saxutils import escape

W, H, LEFT, RIGHT, TOP, BOTTOM = 600, 320, 72, 130, 28, 52
COLORS = ('#8a8f98', '#1f6feb', '#d97706', '#16a34a')


def parse_series(arg):
    name, _, rest = arg.partition('=')
    path, _, variant = rest.partition('#')
    if not name or not path:
        sys.exit('usage: scripts/bench_chart.py NAME=results.csv[#variant] ...')
    return name, path, variant


def medians(path, variant='', x='brokers'):
    by = defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            if r['scenario'] == 'drain' and r.get('variant', '') == variant:
                by[int(r[x])].append(float(r['throughput']))
    return {b: statistics.median(v) for b, v in sorted(by.items())}


def chart(series, x='brokers'):
    axis = x
    xs = sorted({b for _, m in series for b in m})
    top = max(v for _, m in series for v in m.values()) * 1.15

    def x(b):
        return LEFT + (W - LEFT - RIGHT) * xs.index(b) / max(1, len(xs) - 1)

    def y(v):
        return H - BOTTOM - (H - TOP - BOTTOM) * v / top

    mid_y = (TOP + H - BOTTOM) / 2
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
           f'font-family="system-ui, sans-serif" font-size="12">',
           f'<rect width="{W}" height="{H}" fill="#ffffff"/>']
    for i in range(5):
        v = top * i / 4
        out.append(f'<line x1="{LEFT}" x2="{W - RIGHT}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="#e5e7eb"/>')
        out.append(f'<text x="{LEFT - 8}" y="{y(v) + 4:.1f}" text-anchor="end" fill="#374151">{v:,.0f}</text>')
    for b in xs:
        out.append(f'<text x="{x(b):.1f}" y="{H - BOTTOM + 18}" text-anchor="middle" fill="#374151">{b}</text>')
    out.append(f'<text x="{(LEFT + W - RIGHT) / 2:.1f}" y="{H - 12}" text-anchor="middle" fill="#111827">{escape(axis)}</text>')
    out.append(f'<text x="18" y="{mid_y:.1f}" text-anchor="middle" fill="#111827" '
               f'transform="rotate(-90 18 {mid_y:.1f})">jobs/s, drain median</text>')
    for i, (name, m) in enumerate(series):
        color = COLORS[i % len(COLORS)]
        points = ' '.join(f'{x(b):.1f},{y(v):.1f}' for b, v in m.items())
        out.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        for b, v in m.items():
            out.append(f'<circle cx="{x(b):.1f}" cy="{y(v):.1f}" r="4" fill="{color}"/>')
            out.append(f'<text x="{x(b):.1f}" y="{y(v) - 9:.1f}" text-anchor="middle" fill="{color}">{v:,.0f}</text>')
        ly = TOP + 20 * i
        out.append(f'<line x1="{W - RIGHT + 16}" x2="{W - RIGHT + 36}" y1="{ly}" y2="{ly}" stroke="{color}" stroke-width="2.5"/>')
        out.append(f'<text x="{W - RIGHT + 42}" y="{ly + 4}" fill="#111827">{escape(name)}</text>')
    out.append('</svg>')
    return '\n'.join(out)


def main():
    args = sys.argv[1:]
    x = 'brokers'
    if args[:1] == ['--x']:
        if len(args) < 2:
            sys.exit('--x needs a column')
        x, args = args[1], args[2:]
    series = []
    for arg in args:
        name, path, variant = parse_series(arg)
        series.append((name, medians(path, variant, x)))
    if not series or not all(m for _, m in series):
        sys.exit('every series needs drain rows for its variant')
    print(chart(series, x))


if __name__ == '__main__':
    main()
