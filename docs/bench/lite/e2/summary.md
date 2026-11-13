## ref=ab6a6f1, scenario=drain, workers=8, concurrency=16, vm_cpus=4, pinned=0

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 3 | 4,838 (4,711–4,882) | 107 | 49 | 59 | 10.1 | 78.2 | 6.9 | 0.403 | 157.6 | none |
| 2 | 3 | 4,620 (4,611–4,625) | 99 | 24 | 52 | 11.3 | 84.1 | 3.6 | 1.138 | 199.4 | none |

**Verdict:** 2 brokers slower; the 2-broker median is 0.95× the 1-broker median.

