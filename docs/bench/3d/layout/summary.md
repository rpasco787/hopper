## ref=14e87ee, harness=035628b, scenario=drain, variant=go-w1c128, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=128, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 14,153 (13,461–14,692) | 88 | 58 | 72 | 40.0 | 26.7 | 26.7 | 0.676 | 749.3 | 0.11 | 0.096 | 1.18 | 0.0 | 22.40 | – | 12.8/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | 15,831 (14,857–17,347) | 97 | 58 | 21.5 | 9 | 192 |

## ref=14e87ee, harness=035628b, scenario=drain, variant=go-w2c64, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=64, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 12,946 (12,945–13,014) | 98 | 59 | 73 | 23.1 | 46.2 | 25.0 | 0.667 | 613.6 | 0.14 | 0.087 | 1.25 | 0.0 | 24.66 | – | 15.1/– | 1.2/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | 13,941 (13,715–15,609) | 124 | 69 | 22.4 | 10 | 173 |

## ref=14e87ee, harness=035628b, scenario=drain, variant=go-w8c16, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 7,356 (6,472–7,474) | 129 | 61 | 92 | 18.7 | 65.4 | 13.0 | 0.795 | 463.6 | 0.31 | 0.090 | 1.21 | 0.0 | 48.62 | – | 16.2/– | 7.3/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | 7,094 (6,583–7,892) | 120 | 127 | 21.3 | 9 | 227 |

## ref=14e87ee, harness=035628b, scenario=drain, variant=java-w1c128, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=1, concurrency=128, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 8,475 (8,303–8,800) | 90 | 61 | 158 | 39.4 | 25.9 | 35.7 | 0.553 | 566.1 | 0.20 | 0.158 | 1.13 | 0.0 | 24.32 | – | 6.9/– | 0.0/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | 11,506 (9,879–11,710) | 126 | 205 | 161.1 | 24 | 807 |

## ref=14e87ee, harness=035628b, scenario=drain, variant=java-w2c64, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=2, concurrency=64, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=1g

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 7,817 (7,552–7,983) | 102 | 59 | 210 | 24.3 | 45.9 | 24.2 | 0.671 | 514.1 | 0.17 | 0.110 | 1.20 | 0.0 | 36.21 | – | 11.0/– | 0.6/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | 10,418 (9,802–10,650) | 126 | 308 | 134.3 | 24 | 1,207 |

## ref=14e87ee, harness=035628b, scenario=drain, variant=java-w8c16, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 (want 5) | 2,262 (1,795–2,573) | 140 | 62 | 324 | 19.9 | 56.6 | 15.6 | 0.997 | 321.7 | 0.44 | 0.200 | 1.14 | 0.0 | 88.29 | – | 6.5/– | 1.6/– | workers |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | 2,892 (2,373–3,309) | 119 | 1,378 | 127.7 | 23 | 4,069 |

