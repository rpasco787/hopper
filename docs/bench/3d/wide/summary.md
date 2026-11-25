## ref=14e87ee, harness=88a8271, scenario=drain, variant=go-proof, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=10000, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 643 (643–643) | 11 | 6 | 10 | 0.0 | 0.0 | 0.0 | 0.494 | 394.7 | 0.09 | 0.063 | 1.24 | 0.0 | 98.61 | – | 32.4/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | 10,000 | 126 | 215.8 | 9 | 162 |

## ref=14e87ee, harness=88a8271, scenario=drain, variant=go-wide, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=10000, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 627 (627–638) | 14 | 5 | 9 | 0.0 | 0.0 | 0.0 | 0.602 | 395.2 | 0.13 | 0.101 | 1.25 | 0.0 | 23.16 | – | 14.5/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | – | 10,000 | 144 | 210.1 | 9 | 226 |

## ref=14e87ee, harness=88a8271, scenario=drain, variant=java-proof, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=10000, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 608 (608–608) | 12 | 6 | 41 | 0.0 | 0.0 | 0.0 | 0.488 | 321.1 | 0.15 | 0.104 | 1.25 | 0.0 | 49.14 | – | 14.2/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | 10,000 | 770 | 260.6 | 29 | 899 |

## ref=14e87ee, harness=88a8271, scenario=drain, variant=java-wide, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=10000, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 609 (602–620) | 12 | 5 | 38 | 100.0 | 0.0 | 0.0 | 0.589 | 389.6 | 0.09 | 0.060 | 1.28 | 0.0 | 89.94 | – | 36.0/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | – | 10,000 | 737 | 249.4 | 25 | 855 |

