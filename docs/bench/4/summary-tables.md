## ref=64ac8a9, harness=64ac8a9, scenario=drain, variant=w2c256, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=256, vm_cpus=8, pinned=1, worker_sdk=go, shards=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 5 | 15,808 (12,583–17,538) | 38 | 16 | 50 | 0.0 | 80.0 | 16.7 | 0.844 | 580.9 | 0.12 | 0.045 | 1.36 | 0.0 | 32.43 | – | 15.0/– | 0.8/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 2 | 5 | – | 135 | 60 | 23.1 | 10 | 149 |

## ref=64ac8a9, harness=64ac8a9, scenario=drain, variant=w2c256, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=256, vm_cpus=8, pinned=1, worker_sdk=go, shards=2

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 5 | 16,301 (13,442–16,709) | 44 | 16 | 43 | 11.1 | 60.0 | 25.0 | 0.692 | 449.5 | 0.14 | 0.083 | 1.30 | 0.0 | 37.59 | – | 10.3/– | 0.1/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 2 | 5 | – | 155 | 68 | 24.3 | 9 | 182 |

## ref=64ac8a9, harness=64ac8a9, scenario=drain, variant=w8c16, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, shards=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 5 | 6,727 (6,209–7,277) | 58 | 16 | 53 | 3.6 | 85.5 | 9.1 | 0.588 | 267.1 | 0.34 | 0.068 | 1.27 | 0.0 | 45.81 | – | 9.2/– | 5.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 2 | 5 | – | 89 | 105 | 21.3 | 9 | 176 |

## ref=64ac8a9, harness=64ac8a9, scenario=drain, variant=w8c16, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, shards=2

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 5 | 7,715 (6,700–8,120) | 75 | 18 | 61 | 10.4 | 68.6 | 15.2 | 0.418 | 137.3 | 0.44 | 0.145 | 1.14 | 0.0 | 39.67 | – | 4.8/– | 2.1/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 2 | 5 | – | 117 | 133 | 20.8 | 9 | 186 |

## Shards: ref=64ac8a9, harness=64ac8a9, scenario=drain, variant=w2c256, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=256, vm_cpus=8, pinned=1, worker_sdk=go

**Shard verdict** at 2 brokers: inconclusive (ranges overlap); the 2-shard median is 1.03× the 1-shard median (16,301 against 15,808).

## Shards: ref=64ac8a9, harness=64ac8a9, scenario=drain, variant=w8c16, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go

**Shard verdict** at 2 brokers: inconclusive (ranges overlap); the 2-shard median is 1.15× the 1-shard median (7,715 against 6,727).

