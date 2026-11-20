## ref=654c140, harness=654c140, scenario=drain, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 5,170 (4,021–5,311) | 108 | 62 | 93 | 8.6 | 82.6 | 7.6 | 0.249 | 154.8 | 2.24 | 0.426 | 1.31 | 0.0 | 23.51 | 23.03 | none |
| 4 | 5 | 4,933 (4,274–5,026) | 129 | 21 | 113 | 2.8 | 92.2 | 2.0 | 1.126 | 233.7 | 2.22 | 0.176 | 1.69 | 0.0 | 24.30 | 27.11 | none |

**Verdict:** inconclusive (ranges overlap); the 4-broker median is 0.95× the 1-broker median.

## ref=e61ae1c, harness=654c140, scenario=drain, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 4,864 (4,381–4,950) | 107 | 57 | 82 | 9.7 | 80.6 | 7.3 | 0.253 | 149.9 | 2.24 | 0.430 | 1.34 | 0.0 | 23.90 | 23.55 | none |
| 4 | 5 | 4,149 (3,831–4,345) | 130 | 18 | 94 | 2.6 | 92.5 | 2.0 | 1.208 | 232.3 | 2.24 | 0.163 | 1.71 | 0.0 | 31.47 | 35.01 | none |

**Verdict:** 4 brokers slower; the 4-broker median is 0.85× the 1-broker median.

