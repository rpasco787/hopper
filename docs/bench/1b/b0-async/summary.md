## ref=e61ae1c, harness=b762273, scenario=drain, payload_bytes=300, sync_commit=off, db_max_conns=default, workers=8, concurrency=16, vm_cpus=8, pinned=1

| brokers | runs | jobs/s median (range) | postgres CPU | CPU per broker | worker CPU | on-CPU % | Lock+LWLock % | IO % | lease ms | lease blocks | commits/job | WAL syncs/job | WAL KB/job | HOT % | ack p99 ms | hb p99 ms | saturated |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 6,472 (5,961–6,723) | 117 | 67 | 118 | 66.7 | 16.7 | 0.0 | 0.471 | 166.2 | 2.24 | 0.002 | 1.44 | 0.0 | 18.36 | 18.31 | none |
| 2 | 5 | 5,421 (4,816–5,440) | 123 | 39 | 113 | 34.3 | 60.0 | 0.0 | 1.207 | 245.7 | 2.23 | 0.002 | 1.70 | 0.0 | 24.42 | 24.39 | none |
| 4 | 5 | 3,921 (3,024–4,108) | 150 | 21 | 104 | 15.2 | 83.0 | 0.0 | 2.810 | 385.3 | 2.24 | 0.002 | 1.81 | 0.0 | 48.51 | 57.06 | none |

**Verdict:** 2 brokers slower; the 2-broker median is 0.84× the 1-broker median.
**Verdict:** 4 brokers slower; the 4-broker median is 0.61× the 1-broker median.

