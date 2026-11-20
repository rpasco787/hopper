## ref=7b77052, harness=7b77052, scenario=drain, variant=check, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 7,880 (4,432–8,376) | 77 | 49 | 91 | 25.0 | 64.3 | 12.5 | 0.355 | 185.4 | 0.44 | 0.143 | 1.55 | 0.0 | 22.12 | 23.53 | 11.0/10.4 | 0.0/3.8 | none |
| 4 | 5 | 4,743 (2,726–5,039) | 130 | 18 | 87 | 6.1 | 81.0 | 4.4 | 0.699 | 190.2 | 0.93 | 0.127 | 1.64 | 0.0 | 47.34 | 45.96 | 3.5/3.5 | 0.2/9.4 | none |

**Verdict:** inconclusive (ranges overlap); the 4-broker median is 0.60× the 1-broker median.

## ref=7b77052, harness=7b77052, scenario=drain, variant=skip, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 10,708 (10,188–11,082) | 68 | 38 | 83 | 16.7 | 66.7 | 12.5 | 0.413 | 213.1 | 0.28 | 0.094 | 1.19 | 0.0 | 24.83 | – | 15.4/– | 3.8/– | none |
| 4 | 5 | 7,658 (3,372–7,969) | 96 | 13 | 70 | 10.2 | 83.1 | 7.3 | 0.684 | 238.2 | 0.52 | 0.090 | 1.31 | 0.0 | 46.89 | – | 4.7/– | 10.8/– | none |

**Verdict:** 4 brokers slower; the 4-broker median is 0.72× the 1-broker median.

