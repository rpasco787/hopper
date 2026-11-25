#!/usr/bin/env python3
"""The worker columns of a scripts/bench.sh row, from one run's evidence.

    python3 scripts/bench_workers.py <evidence dir> <jobs completed> <memory limit in bytes, or 0>

Prints "cpu_ms_per_1k peak_rss_mib threads startup_ms", with a dash for any it
can't compute, or exits 1 with the reason the run can't count. It reads:
- proc.txt: each worker process's /proc/<pid>/stat and status, as
  scripts/bench.sh's proc_snapshot writes them. A drain's one snapshot, "end",
  counts CPU since the process started; a steady run's "before" and "after"
  count CPU between them. stat's utime and stime cover every thread.
- workers.json: the workers' docker inspect, trimmed. Every worker must still
  be running, never OOM-killed, and limited to the memory asked for.
- brokers.log: the brokers' JSON lines. A worker's startup runs from its
  container's start to the first "lease stream opened" a broker logged for its
  id, <hostname>-<pid>: the broker's line is readiness
  (docs/design/conformance.md, "The worker under test").
Peak memory (VmHWM) and threads are medians across the workers.
"""
import json
import re
import statistics
import sys
from datetime import datetime
from pathlib import Path

PREFIX = 'hopper-bench-'
TIME = re.compile(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)')


def parse_time(text):
    """RFC 3339 with up to nanosecond digits, as Docker and slog write it, in epoch seconds."""
    m = TIME.fullmatch(text)
    if not m:
        raise ValueError(f'not an RFC 3339 time: {text!r}')
    whole, frac, zone = m.groups()
    t = datetime.fromisoformat(whole + ('+00:00' if zone == 'Z' else zone)).timestamp()
    return t + (int(frac[:9].ljust(9, '0')) / 1e9 if frac else 0.0)


def short(name):
    """/hopper-bench-worker-b-1 -> worker-b-1, as scripts/bench.sh names its files."""
    name = name.lstrip('/')
    return name[len(PREFIX):] if name.startswith(PREFIX) else name


def parse_proc(text):
    """({(label, container id): {'ticks', 'VmHWM', 'VmRSS', 'Threads'}}, clock ticks per second)."""
    snaps, clk, cur, pid = {}, 100, None, None
    for line in text.splitlines():
        if line.startswith('clk_tck '):
            clk = int(line.split()[1])
        elif line.startswith('== '):
            _, label, container, pid = line.split()
            cur = snaps[(label, container)] = {}
        elif cur is None:
            continue
        elif line.startswith(pid + ' ('):
            # The command name sits in parentheses and may hold spaces; utime
            # and stime are the 12th and 13th fields after it (proc(5)).
            rest = line[line.rindex(')') + 2:].split()
            cur['ticks'] = int(rest[11]) + int(rest[12])
        else:
            key, sep, value = line.partition(':')
            if sep and key in ('VmHWM', 'VmRSS', 'Threads'):
                cur[key] = int(value.split()[0])
    return snaps, clk


def startups(workers, broker_lines):
    """Milliseconds from each worker container's start to its first accepted lease stream, or None."""
    opened = {}
    for line in broker_lines:
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if not isinstance(rec, dict) or rec.get('msg') != 'lease stream opened':
            continue
        if 'time' not in rec or not rec.get('worker_id'):
            continue
        host = str(rec['worker_id']).rpartition('-')[0]
        t = parse_time(rec['time'])
        opened[host] = min(t, opened.get(host, t))
    out = []
    for w in workers:
        host = w['Config']['Hostname']
        if host not in opened:
            return None
        out.append((opened[host] - parse_time(w['State']['StartedAt'])) * 1000)
    return out


def columns(proc_text, workers, broker_lines, jobs, memory):
    """(cpu_ms_per_1k, peak_rss_mib, threads, startup_ms) as text; raises ValueError if the run can't count."""
    if not workers:
        raise ValueError('no worker containers')
    for w in workers:
        st, name = w['State'], short(w['Name'])
        if not st['Running'] or st['OOMKilled']:
            raise ValueError(f"{name} exited during the run (exit {st['ExitCode']}, "
                             f"OOM-killed {str(st['OOMKilled']).lower()})")
        if memory and w['HostConfig']['Memory'] != memory:
            raise ValueError(f"{name} ran with memory limit {w['HostConfig']['Memory']}, not {memory}")
    snaps, clk = parse_proc(proc_text)
    labels = {label for label, _ in snaps}
    final = 'after' if 'after' in labels else 'end'
    if final not in labels:
        raise ValueError('no worker process statistics')
    ticks, hwm, threads = 0, [], []
    for w in workers:
        name = short(w['Name'])
        snap = snaps.get((final, w['Id']), {})
        if not {'ticks', 'VmHWM', 'Threads'} <= snap.keys():
            raise ValueError(f'no /proc statistics for {name} at "{final}"')
        t = snap['ticks']
        if final == 'after':
            before = snaps.get(('before', w['Id']), {})
            if 'ticks' not in before:
                raise ValueError(f'no /proc statistics for {name} at "before"')
            t -= before['ticks']
        ticks += t
        hwm.append(snap['VmHWM'])
        threads.append(snap['Threads'])
    cpu_ms = ticks * 1000 / clk
    starts = startups(workers, broker_lines)
    return (f'{cpu_ms * 1000 / jobs:.0f}' if jobs > 0 else '-',
            f'{statistics.median(hwm) / 1024:.1f}',
            f'{statistics.median(threads):.0f}',
            f'{statistics.median(starts):.0f}' if starts else '-')


def main(argv):
    if len(argv) != 4:
        sys.exit('usage: scripts/bench_workers.py <evidence dir> <jobs completed> <memory limit bytes or 0>')
    d = Path(argv[1])
    proc = d / 'proc.txt'
    brokers = d / 'brokers.log'
    try:
        workers = json.loads((d / 'workers.json').read_text())
        out = columns(proc.read_text() if proc.exists() else '', workers,
                      brokers.read_text().splitlines() if brokers.exists() else [],
                      int(argv[2]), int(argv[3]))
    except KeyError as e:
        sys.exit(f'workers.json lacks {e}')
    except (ValueError, OSError) as e:
        sys.exit(str(e))
    print(' '.join(out))


if __name__ == '__main__':
    main(sys.argv)
