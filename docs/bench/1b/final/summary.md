## ref=4c4e37d, harness=4c4e37d, scenario=drain, variant=after, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 6,581 (5,012–7,436) | 85 | 49 | 88 | 20.0 | 51.4 | 14.3 | 0.392 | 185.5 | 0.45 | 0.143 | 1.55 | 0.0 | 29.76 | 29.48 | 11.0/10.5 | 0.0/3.9 | none |
| 2 | 5 | 6,055 (5,760–6,469) | 102 | 30 | 91 | 12.3 | 80.2 | 8.6 | 0.495 | 189.7 | 0.63 | 0.130 | 1.58 | 0.0 | 34.25 | 37.19 | 6.4/6.1 | 0.0/6.6 | none |
| 4 | 5 | 4,236 (3,817–4,303) | 123 | 18 | 85 | 8.3 | 86.8 | 5.2 | 0.754 | 218.7 | 0.93 | 0.127 | 1.67 | 0.0 | 45.76 | 48.43 | 3.6/3.5 | 0.2/9.1 | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 0.92× the 1-broker median.
**Verdict:** 4 brokers slower; the 4-broker median is 0.64× the 1-broker median.

## ref=e61ae1c, harness=4c4e37d, scenario=drain, variant=before, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 3,773 (3,501–3,908) | 109 | 57 | 79 | 11.9 | 80.5 | 6.6 | 0.303 | 146.6 | 2.24 | 0.427 | 1.35 | 0.0 | 46.74 | 44.16 | – | – | none |
| 2 | 5 | 3,617 (3,572–3,793) | 124 | 34 | 89 | 7.7 | 85.5 | 3.5 | 0.631 | 175.6 | 2.24 | 0.249 | 1.47 | 0.0 | 46.52 | 45.22 | – | – | none |
| 4 | 5 | 2,923 (2,848–3,404) | 143 | 20 | 94 | 4.2 | 91.1 | 2.2 | 1.671 | 220.7 | 2.24 | 0.171 | 1.70 | 0.0 | 59.81 | 65.85 | – | – | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 0.96× the 1-broker median.
**Verdict:** 4 brokers slower; the 4-broker median is 0.77× the 1-broker median.

