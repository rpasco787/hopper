## ref=3aaf0c1, harness=3aaf0c1, scenario=drain, variant=batch, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 6,446 (5,304–8,170) | 82 | 50 | 95 | 17.6 | 67.6 | 14.3 | 0.393 | 184.9 | 0.45 | 0.142 | 1.55 | 0.0 | 28.25 | 33.72 | 11.0/10.6 | 0.0/4.0 | none |
| 2 | 5 | 4,645 (4,168–6,599) | 107 | 30 | 90 | 10.2 | 81.2 | 9.9 | 0.566 | 190.7 | 0.62 | 0.131 | 1.61 | 0.0 | 47.21 | 46.81 | 6.3/6.2 | 0.1/6.4 | none |
| 4 | 5 | 3,800 (2,094–5,087) | 129 | 18 | 86 | 11.0 | 84.3 | 4.2 | 0.793 | 190.2 | 0.93 | 0.128 | 1.67 | 0.0 | 61.79 | 69.52 | 3.6/3.5 | 0.3/9.1 | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 0.72× the 1-broker median.
**Verdict:** 4 brokers slower; the 4-broker median is 0.59× the 1-broker median.

## ref=3aaf0c1, harness=3aaf0c1, scenario=drain, variant=batch-flights1, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 7,251 (6,832–8,895) | 67 | 46 | 87 | 20.0 | 53.3 | 22.2 | 0.353 | 188.5 | 0.32 | 0.137 | 1.51 | 0.0 | 24.60 | 23.82 | 19.9/19.3 | 0.0/1.5 | none |
| 2 | 5 | 4,232 (2,573–7,826) | 94 | 34 | 106 | 13.8 | 64.1 | 12.5 | 0.599 | 190.2 | 0.43 | 0.135 | 1.56 | 0.0 | 75.40 | 78.18 | 11.0/10.8 | 0.0/3.2 | none |
| 4 | 5 | 3,546 (3,130–5,077) | 121 | 18 | 88 | 11.8 | 77.6 | 9.2 | 0.757 | 190.0 | 0.62 | 0.131 | 1.60 | 0.0 | 49.91 | 70.53 | 6.1/6.1 | 0.1/4.2 | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 0.58× the 1-broker median.
**Verdict:** 4 brokers slower; the 4-broker median is 0.49× the 1-broker median.

## ref=3aaf0c1, harness=3aaf0c1, scenario=drain, variant=nobatch, payload_bytes=300, sync_commit=off, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 5,277 (4,281–6,701) | 111 | 69 | 123 | 75.0 | 12.5 | 0.0 | 0.570 | 178.1 | 2.24 | 0.002 | 1.44 | 0.0 | 37.83 | 35.25 | – | – | none |
| 2 | 5 | 6,185 (3,272–6,372) | 124 | 42 | 131 | 30.0 | 53.3 | 0.0 | 1.151 | 276.6 | 2.23 | 0.002 | 1.64 | 0.0 | 23.24 | 23.98 | – | – | none |
| 4 | 5 | 3,676 (3,010–5,227) | 144 | 24 | 119 | 13.6 | 70.6 | 0.0 | 3.020 | 459.3 | 2.21 | 0.002 | 1.80 | 0.0 | 48.24 | 50.00 | – | – | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 1.17× the 1-broker median.
**Verdict:** inconclusive (ranges overlap); the 4-broker median is 0.70× the 1-broker median.

## ref=3aaf0c1, harness=3aaf0c1, scenario=drain, variant=nobatch, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 3,369 (2,039–4,725) | 111 | 62 | 84 | 8.4 | 77.2 | 7.6 | 0.335 | 142.9 | 2.24 | 0.425 | 1.31 | 0.0 | 49.59 | 48.19 | – | – | none |
| 2 | 5 | 3,406 (2,753–4,793) | 120 | 35 | 98 | 4.1 | 86.7 | 4.2 | 0.736 | 183.0 | 2.23 | 0.259 | 1.46 | 0.0 | 59.17 | 54.32 | – | – | none |
| 4 | 5 | 3,845 (1,457–4,147) | 147 | 22 | 103 | 4.7 | 88.2 | 2.1 | 1.472 | 221.3 | 2.23 | 0.180 | 1.69 | 0.0 | 48.84 | 48.57 | – | – | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 1.01× the 1-broker median.
**Verdict:** inconclusive (ranges overlap); the 4-broker median is 1.14× the 1-broker median.

