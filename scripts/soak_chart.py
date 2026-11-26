#!/usr/bin/env python3
"""Draws a soak's timeline as three SVGs, with no third-party packages.

    python3 scripts/soak_chart.py docs/sharding/soak-kind/<run>

reads <dir>/timeline.csv, which test/k8s TestSoak writes every 30 s, and
writes into the same directory:

    throughput.svg  completed jobs/s across every shard
    pickup-p99.svg  pickup p99 (Prometheus, blank without it) and enqueue p99
    table.svg       each shard's jobs table size and dead tuples
"""
import csv
import sys
from pathlib import Path
from xml.sax.saxutils import escape

W, H, LEFT, RIGHT, TOP, BOTTOM = 640, 300, 72, 120, 36, 48
COLORS = ('#1f6feb', '#d97706', '#16a34a', '#8a8f98', '#9333ea', '#dc2626', '#0891b2', '#65a30d')


def read(lines):
    return list(csv.DictReader(lines))


def number(v):
    return float(v) if v not in (None, '') else None


def points(rows, column):
    """(t_s, value) for every row with a value in column."""
    out = []
    for r in rows:
        v = number(r.get(column))
        if v is not None:
            out.append((float(r['t_s']), v))
    return out


def shards(rows):
    if not rows:
        return []
    found = set()
    for k in rows[0]:
        if k.startswith('shard') and k.endswith('_table_bytes'):
            found.add(int(k[len('shard'):-len('_table_bytes')]))
    return sorted(found)


def panel(title, ylabel, series, top=0, height=H, unit=''):
    """One chart: an <svg> fragment of lines over time, at vertical offset top."""
    out = [f'<g transform="translate(0,{top})">',
           f'<text x="{LEFT}" y="20" font-size="14" fill="#111827">{escape(title)}</text>']
    every = [p for _, ps in series for p in ps]
    if not every:
        out.append(f'<text x="{(LEFT + W - RIGHT) / 2:.1f}" y="{height / 2:.1f}" text-anchor="middle" '
                   f'fill="#6b7280">no {escape(ylabel)} samples</text>')
        out.append('</g>')
        return out
    t_max = max(t for t, _ in every) or 1.0
    t_min = min(t for t, _ in every)
    v_top = max(v for _, v in every) * 1.15 or 1.0
    span = max(t_max - t_min, 1.0)

    def x(t):
        return LEFT + (W - LEFT - RIGHT) * (t - t_min) / span

    def y(v):
        return height - BOTTOM - (height - TOP - BOTTOM) * v / v_top

    for i in range(5):
        v = v_top * i / 4
        out.append(f'<line x1="{LEFT}" x2="{W - RIGHT}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="#e5e7eb"/>')
        out.append(f'<text x="{LEFT - 8}" y="{y(v) + 4:.1f}" text-anchor="end" fill="#374151">{v:,.{1 if v_top < 10 else 0}f}{unit}</text>')
    for i in range(5):
        t = t_min + span * i / 4
        out.append(f'<text x="{x(t):.1f}" y="{height - BOTTOM + 18}" text-anchor="middle" fill="#374151">{t / 60:.0f}m</text>')
    mid = (TOP + height - BOTTOM) / 2
    out.append(f'<text x="18" y="{mid:.1f}" text-anchor="middle" fill="#111827" '
               f'transform="rotate(-90 18 {mid:.1f})">{escape(ylabel)}</text>')
    for i, (name, ps) in enumerate(series):
        color = COLORS[i % len(COLORS)]
        if ps:
            line = ' '.join(f'{x(t):.1f},{y(v):.1f}' for t, v in ps)
            out.append(f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="2"/>')
        ly = TOP + 20 * i
        out.append(f'<line x1="{W - RIGHT + 14}" x2="{W - RIGHT + 32}" y1="{ly}" y2="{ly}" stroke="{color}" stroke-width="2"/>')
        out.append(f'<text x="{W - RIGHT + 38}" y="{ly + 4}" fill="#111827">{escape(name)}</text>')
    out.append('</g>')
    return out


def svg(title, body, height=H):
    return '\n'.join([f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" '
                      f'viewBox="0 0 {W} {height}" font-family="system-ui, sans-serif" font-size="12">',
                      f'<title>{escape(title)}</title>',
                      f'<rect width="{W}" height="{height}" fill="#ffffff"/>'] + body + ['</svg>'])


def throughput(rows):
    title = 'Completed jobs/s'
    ps = points(rows, 'completed_per_s')
    body = panel(title, 'jobs/s', [('completed', ps), ('offered', points(rows, 'offered_per_s'))])
    # A dot per sample, so a gap in sampling shows.
    xs = [p for p in ps]
    if xs:
        t_min, t_max = min(t for t, _ in xs), max(t for t, _ in xs)
        every = xs + points(rows, 'offered_per_s')
        v_top = max(v for _, v in every) * 1.15 or 1.0
        span = max(t_max - t_min, 1.0)
        for t, v in xs:
            cx = LEFT + (W - LEFT - RIGHT) * (t - t_min) / span
            cy = H - BOTTOM - (H - TOP - BOTTOM) * v / v_top
            body.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="2.5" fill="{COLORS[0]}"><title>{t:.0f}s: {v}</title></circle>')
    return svg(title, body)


def latency_series(rows):
    return [('pickup p99', points(rows, 'pickup_p99_ms')), ('enqueue p99', points(rows, 'enqueue_p99_ms'))]


def latency(rows):
    title = 'p99 latency (ms)'
    series = latency_series(rows)
    body = panel(title, 'ms', series)
    if not series[0][1]:
        body.append(f'<text x="{LEFT}" y="{H - 8}" fill="#6b7280">no pickup samples: Prometheus was not running</text>')
    return svg(title, body)


def table(rows):
    title = 'jobs table per shard'
    ids = shards(rows)
    size = [(f'shard {i}', [(t, v / 1048576) for t, v in points(rows, f'shard{i}_table_bytes')]) for i in ids]
    dead = [(f'shard {i}', points(rows, f'shard{i}_dead_tup')) for i in ids]
    body = panel('jobs table size (MiB, pg_total_relation_size)', 'MiB', size)
    body += panel('jobs n_dead_tup', 'dead tuples', dead, top=H)
    return svg(title, body, height=2 * H)


def draw(directory):
    directory = Path(directory)
    with open(directory / 'timeline.csv', newline='') as f:
        rows = read(f)
    written = []
    for name, render in (('throughput.svg', throughput), ('pickup-p99.svg', latency), ('table.svg', table)):
        path = directory / name
        path.write_text(render(rows) + '\n')
        written.append(path)
    return written


def main():
    if len(sys.argv) != 2:
        sys.exit('usage: scripts/soak_chart.py <evidence dir holding timeline.csv>')
    for path in draw(sys.argv[1]):
        print(path)


if __name__ == '__main__':
    main()
