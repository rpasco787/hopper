## ref=7b77052, harness=7b77052, scenario=drain, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=16, concurrency=8, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 5,342 (2,978–5,839) | 85 | 50 | 110 | 17.8 | 73.0 | 10.0 | 0.410 | 158.4 | 0.59 | 0.154 | 1.46 | 0.0 | 36.14 | 34.21 | 10.8/10.3 | 0.0/4.8 | none |

## ref=7b77052, harness=7b77052, scenario=drain, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=4, concurrency=32, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 10,385 (3,706–10,545) | 75 | 55 | 79 | 25.0 | 53.8 | 20.0 | 0.329 | 218.2 | 0.35 | 0.143 | 1.65 | 0.0 | 20.35 | 19.76 | 10.7/10.5 | 0.0/2.0 | none |

## ref=7b77052, harness=7b77052, scenario=drain, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 7,787 (6,208–8,355) | 77 | 46 | 88 | 17.1 | 72.2 | 8.6 | 0.342 | 184.4 | 0.44 | 0.142 | 1.54 | 0.0 | 23.06 | 24.79 | 11.0/10.5 | 0.0/3.7 | none |

