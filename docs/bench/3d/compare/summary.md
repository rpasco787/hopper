## ref=14e87ee, harness=8726d9d, scenario=drain, variant=go, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 7,699 (7,240–8,508) | 123 | 57 | 86 | 13.7 | 66.7 | 10.3 | 0.747 | 465.0 | 0.30 | 0.090 | 1.21 | 0.0 | 41.89 | – | 16.2/– | 7.3/– | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 8,088 (6,751–9,442) | 121 | 119 | 21.4 | 9 | 174 |

## ref=14e87ee, harness=8726d9d, scenario=drain, variant=java, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 2,045 (1,939–2,296) | 134 | 59 | 315 | 17.6 | 62.7 | 15.0 | 1.025 | 292.0 | 0.44 | 0.195 | 1.15 | 0.0 | 90.62 | 9.20 | 6.6/2.7 | 1.4/0.0 | workers |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | 2,872 (2,161–3,416) | 116 | 1,491 | 127.7 | 23 | 4,119 |

## ref=14e87ee, harness=8726d9d, scenario=steady, variant=go, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=go, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 1,001 (1,000–1,004) | 67 | 40 | 25 | 24.2 | 48.5 | 18.6 | 0.074 | 20.5 | 2.99 | 2.160 | 1.55 | 0.0 | 19.98 | 94.18 | 1.1/3.4 | 1.3/2.3 | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | – | – | 249 | 21.2 | 9 | 107 |

## ref=14e87ee, harness=8726d9d, scenario=steady, variant=java, payload_bytes=300, sync_commit=on, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1, worker_sdk=java, worker_mem=384m

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | batch ack/hb | fallback % ack/hb | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 1,001 (997–1,002) | 86 | 48 | 88 | 16.4 | 60.0 | 15.0 | 0.136 | 27.4 | 2.94 | 1.777 | 1.56 | 0.0 | 30.54 | 78.67 | 1.3/3.9 | 1.3/0.0 | none |

| brokers | runs | warm jobs/s median (range) | max leased | worker CPU ms per 1k jobs | peak RSS MiB per worker | threads per worker | startup ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 5 | – | – | 915 | 128.0 | 23 | 2,634 |

