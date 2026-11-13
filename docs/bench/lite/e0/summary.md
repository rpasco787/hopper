## ref=ab6a6f1, scenario=drain, workers=16, concurrency=16, vm_cpus=8, pinned=0

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 3 | 3,961 (3,678–4,470) | 160 | 40 | 104 | 14.7 | 79.3 | 3.7 | 0.821 | 181.0 | none |

## ref=ab6a6f1, scenario=drain, workers=4, concurrency=16, vm_cpus=8, pinned=0

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 | 4,899 (4,345–5,219) | 161 | 89 | 93 | 9.5 | 73.3 | 8.5 | 0.298 | 151.2 | none |

