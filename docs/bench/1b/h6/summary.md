## ref=14e87ee, harness=edac070, scenario=drain, variant=go-w1c128, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=128, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 19,798 (19,398–21,088) | 76 | 57 | 70 | 53.8 | 11.1 | 30.8 | 0.463 | 695.9 | 0.11 | 0.096 | 1.19 | 0.0 | 9.09 | – | 12.9/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 20,216 (19,849–21,720) | 94 | 46 | 23.5 | 9 | 115 |

## ref=14e87ee, harness=edac070, scenario=drain, variant=go-w2c256, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=256, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 24,330 (23,604–24,769) | 88 | 70 | 108 | 23.5 | 47.1 | 26.7 | 0.691 | 892.7 | 0.07 | 0.047 | 1.35 | 0.0 | 21.80 | – | 28.4/– | 0.4/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 26,560 (23,649–26,936) | 192 | 55 | 24.4 | 10 | 129 |

## ref=14e87ee, harness=edac070, scenario=drain, variant=go-w2c64, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=64, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 18,803 (18,564–20,233) | 93 | 64 | 86 | 38.9 | 33.3 | 20.0 | 0.452 | 563.2 | 0.14 | 0.089 | 1.26 | 0.0 | 13.92 | – | 14.9/– | 0.8/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 19,502 (18,447–20,790) | 124 | 53 | 22.2 | 10 | 127 |

## ref=14e87ee, harness=edac070, scenario=drain, variant=go-w2c64-lb6, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=64, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 10,682 (10,360–10,813) | 110 | 54 | 65 | 28.6 | 39.3 | 31.4 | 0.269 | 326.3 | 0.36 | 0.218 | 1.03 | 0.0 | 6.83 | – | 5.8/– | 1.9/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 9,821 (9,626–10,136) | 29 | 70 | 23.2 | 10 | 127 |

## ref=14e87ee, harness=edac070, scenario=drain, variant=go-w4c32, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=4, concurrency=32, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 16,095 (15,485–16,856) | 102 | 62 | 89 | 17.8 | 67.5 | 16.7 | 0.447 | 460.0 | 0.19 | 0.087 | 1.26 | 0.0 | 17.62 | – | 15.7/– | 2.8/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 14,856 (14,761–16,860) | 111 | 65 | 22.3 | 9 | 92 |

## ref=14e87ee, harness=edac070, scenario=drain, variant=go-w8c16, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 11,348 (10,438–13,082) | 118 | 62 | 98 | 11.4 | 65.5 | 12.7 | 0.559 | 475.0 | 0.30 | 0.091 | 1.21 | 0.0 | 23.56 | – | 16.1/– | 6.5/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 10,491 (10,380–12,169) | 102 | 90 | 21.4 | 9 | 102 |

## ref=14e87ee, harness=edac070, scenario=drain, variant=go-w8c64, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=64, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 19,797 (19,374–22,210) | 95 | 65 | 115 | 14.3 | 64.3 | 13.5 | 0.960 | 787.5 | 0.09 | 0.032 | 1.44 | 0.0 | 47.17 | – | 54.0/– | 1.1/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 22,163 (20,897–23,819) | 385 | 68 | 21.4 | 9 | 109 |

