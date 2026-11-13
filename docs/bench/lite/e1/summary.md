## ref=ab6a6f1, scenario=drain, workers=8, concurrency=16, vm_cpus=8, pinned=0

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 | 5,088 (5,068–5,185) | 155 | 81 | 102 | 16.2 | 65.7 | 8.4 | 0.275 | 152.0 | none |
| 2 | 3 | 5,444 (5,147–5,472) | 152 | 42 | 98 | 10.9 | 80.6 | 3.6 | 0.707 | 181.0 | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 1.07× the 1-broker median.

