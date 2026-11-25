## ref=14e87ee, harness=0acfa0a-dirty, scenario=drain, variant=go-long, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

> Measured with an uncommitted harness: not a result.

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 7,893 (7,893–7,893) | 138 | 61 | 87 | 12.5 | 76.2 | 10.2 | 0.849 | 591.1 | 0.32 | 0.093 | 1.23 | 0.0 | 42.20 | – | 16.0/– | 9.1/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | 101 | 112 | 21.4 | 9 | 133 |

## ref=14e87ee, harness=0acfa0a-dirty, scenario=drain, variant=go-wide, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=10000, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=1g

> Measured with an uncommitted harness: not a result.

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 634 (634–634) | 12 | 5 | 10 | 100.0 | 0.0 | 0.0 | 0.607 | 395.8 | 0.15 | 0.121 | 1.24 | 0.0 | 21.89 | – | 11.2/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | 10,000 | 124 | 206.3 | 9 | 213 |

## ref=14e87ee, harness=0acfa0a-dirty, scenario=drain, variant=java-long, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=384m

> Measured with an uncommitted harness: not a result.

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 2,318 (2,318–2,318) | 147 | 61 | 294 | 18.2 | 66.2 | 15.1 | 1.172 | 356.6 | 0.40 | 0.161 | 1.19 | 0.0 | 87.04 | – | 8.2/– | 2.7/– | workers |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | 117 | 1,222 | 128.6 | 23 | 4,161 |

## ref=14e87ee, harness=0acfa0a-dirty, scenario=drain, variant=java-wide, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=10000, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=1g

> Measured with an uncommitted harness: not a result.

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 603 (603–603) | 11 | 3 | 33 | 0.0 | 0.0 | 0.0 | 0.468 | 389.8 | 0.11 | 0.085 | 1.26 | 0.0 | 24.53 | – | 19.0/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | 10,000 | 646 | 275.2 | 25 | 632 |

## ref=14e87ee, harness=0acfa0a-dirty, scenario=steady, variant=go-steady, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

> Measured with an uncommitted harness: not a result.

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 1,001 (1,001–1,001) | 63 | 45 | 30 | 16.4 | 54.8 | 19.2 | 0.093 | 20.3 | 2.96 | 2.435 | 1.55 | 0.0 | 20.39 | 49.75 | 1.1/3.5 | 1.1/0.8 | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | – | 309 | 21.2 | 9 | 179 |

## ref=14e87ee, harness=0acfa0a-dirty, scenario=steady, variant=java-steady, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=384m

> Measured with an uncommitted harness: not a result.

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 (want 5) | 1,000 (1,000–1,000) | 86 | 50 | 91 | 20.0 | 50.8 | 13.3 | 0.123 | 22.8 | 2.96 | 1.790 | 1.55 | 0.0 | 36.55 | 89.33 | 1.3/2.7 | 1.2/0.0 | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | – | – | 895 | 127.8 | 23 | 2,667 |

