## ref=e61ae1c, harness=b762273, scenario=drain, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 4,889 (4,102–5,003) | 118 | 59 | 86 | 8.0 | 85.1 | 6.9 | 0.249 | 150.0 | 2.24 | 0.433 | 1.34 | 0.0 | 24.26 | 23.85 | none |
| 2 | 5 | 4,746 (4,602–5,013) | 122 | 34 | 95 | 4.8 | 91.2 | 3.8 | 0.527 | 177.9 | 2.23 | 0.256 | 1.46 | 0.0 | 24.50 | 24.42 | none |
| 4 | 5 | 4,369 (4,062–4,494) | 130 | 18 | 92 | 3.5 | 95.8 | 1.9 | 1.213 | 233.9 | 2.23 | 0.163 | 1.70 | 0.0 | 31.04 | 34.26 | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 0.97× the 1-broker median.
**Verdict:** inconclusive (ranges overlap); the 4-broker median is 0.89× the 1-broker median.

