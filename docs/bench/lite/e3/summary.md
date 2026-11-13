## ref=ab6a6f1, scenario=drain, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 | 5,274 (5,170–5,307) | 103 | 44 | 78 | 7.4 | 82.7 | 8.6 | 0.240 | 152.0 | none |
| 2 | 3 | 5,191 (5,087–5,224) | 114 | 32 | 93 | 4.8 | 90.5 | 3.2 | 0.527 | 181.2 | none |

**Verdict:** inconclusive (ranges overlap); the 2-broker median is 0.98× the 1-broker median.

