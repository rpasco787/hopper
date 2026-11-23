## ref=1f128c5, harness=1f128c5, scenario=drain, variant=after, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 5,685 (4,577–6,244) | 82 | 49 | 92 | 13.0 | 69.2 | 11.8 | 0.452 | 185.2 | 0.44 | 0.141 | 1.56 | 0.0 | 30.02 | 37.60 | 11.1/10.6 | 0.0/3.9 | none |

## ref=a3fcbda, harness=1f128c5, scenario=drain, variant=before, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 5,602 (4,692–6,399) | 79 | 44 | 89 | 17.1 | 62.2 | 11.4 | 0.443 | 184.6 | 0.45 | 0.142 | 1.56 | 0.0 | 35.42 | 30.46 | 10.9/10.6 | 0.0/4.2 | none |

