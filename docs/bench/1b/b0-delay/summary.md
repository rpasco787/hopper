## ref=e61ae1c, harness=b762273, scenario=drain, variant=commit_delay, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 3,768 (3,642–3,985) | 94 | 49 | 65 | 13.3 | 82.6 | 2.7 | 0.209 | 142.9 | 2.25 | 0.234 | 1.32 | 0.0 | 24.89 | 24.92 | none |
| 2 | 5 | 5,087 (4,836–5,373) | 120 | 35 | 100 | 8.6 | 88.3 | 1.2 | 0.433 | 170.9 | 2.24 | 0.125 | 1.42 | 0.0 | 24.35 | 24.21 | none |
| 4 | 5 | 4,638 (4,281–4,682) | 128 | 18 | 95 | 3.8 | 95.7 | 0.8 | 1.198 | 233.2 | 2.24 | 0.092 | 1.70 | 0.0 | 24.92 | 28.65 | none |

**Verdict:** 2 brokers faster; the 2-broker median is 1.35× the 1-broker median.
**Verdict:** 4 brokers faster; the 4-broker median is 1.23× the 1-broker median.

