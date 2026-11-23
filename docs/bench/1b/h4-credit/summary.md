## ref=170c385, harness=170c385, scenario=drain, variant=after, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 8,937 (8,543–10,143) | 50 | 27 | 77 | 15.0 | 67.6 | 13.3 | 0.425 | 214.4 | 0.29 | 0.092 | 1.20 | 0.0 | 33.91 | – | 15.5/– | 4.2/– | none |
| 4 | 5 | 6,031 (5,109–7,515) | 98 | 14 | 78 | 9.3 | 83.7 | 5.4 | 0.755 | 229.2 | 0.51 | 0.089 | 1.30 | 0.0 | 64.16 | – | 4.8/– | 10.3/– | none |

**Verdict:** 4 brokers slower; the 4-broker median is 0.67× the 1-broker median.

## ref=770fdb5, harness=170c385, scenario=drain, variant=before, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 6,099 (5,717–6,879) | 74 | 43 | 81 | 17.1 | 63.7 | 12.1 | 0.404 | 184.7 | 0.45 | 0.141 | 1.55 | 0.0 | 28.09 | 29.39 | 11.1/10.6 | 0.0/3.9 | none |
| 4 | 5 | 4,060 (3,956–4,297) | 119 | 17 | 87 | 9.2 | 83.7 | 5.8 | 0.720 | 187.0 | 0.92 | 0.127 | 1.66 | 0.0 | 46.92 | 49.75 | 3.6/3.6 | 0.2/8.1 | none |

**Verdict:** 4 brokers slower; the 4-broker median is 0.67× the 1-broker median.

