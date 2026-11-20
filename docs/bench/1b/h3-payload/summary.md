## ref=cf2d835, harness=cf2d835, scenario=drain, payload_bytes=0, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 8,589 (7,410–8,709) | 76 | 50 | 87 | 26.1 | 60.9 | 10.0 | 0.320 | 181.1 | 0.45 | 0.143 | 1.16 | 0.0 | 20.78 | 20.80 | 10.9/10.6 | 0.0/4.0 | none |

## ref=cf2d835, harness=cf2d835, scenario=drain, payload_bytes=1500, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 5,622 (5,243–6,467) | 94 | 54 | 83 | 8.0 | 68.1 | 13.9 | 0.667 | 243.4 | 0.45 | 0.146 | 5.23 | 0.0 | 37.48 | 41.16 | 11.2/10.5 | 0.0/4.6 | none |

## ref=cf2d835, harness=cf2d835, scenario=drain, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 8,252 (7,389–8,730) | 79 | 50 | 87 | 12.9 | 69.0 | 11.1 | 0.351 | 183.0 | 0.44 | 0.143 | 1.54 | 0.0 | 23.60 | 23.51 | 10.9/10.5 | 0.0/3.7 | none |

