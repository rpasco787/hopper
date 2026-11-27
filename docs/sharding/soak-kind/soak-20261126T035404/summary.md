# Soak 63f1639

Ref 63f1639, broker image `ghcr.io/rpasco787/hopper-broker:dev-1795663844`, context `kind-hopper`: 2 shards (bundled), 2 brokers, 3 workers; 200 jobs/s offered open loop (at most 256 in flight), 50 ms jobs, chunks of 100000.

| | |
|---|---|
| jobs | 187403 |
| duration | 15m0s planned; produced for 937 s, drained in 5 s |
| completed/s | mean 200.0, p99 200.7 (30 s samples while producing) |
| enqueue p99 | 15.1 ms |
| behind | 0 of 187403 offered calls could not start |
| failed enqueue calls | map[] |
| faults | broker-restart 3, worker-kill 5; 0 errors |
| skipped | failover: bundled mode: a shard has no standby to promote; run with postgres.mode=cloudnativepg |
| worker container restarts | 5 |
| lost | 0 |
| unexplained | 1 |
| rows on the wrong shard | 0 |
| redelivery rate | 0.000117 ((Σ attempts − jobs) / jobs = (187425 − 187403) / 187403) |
| repeated starts by cause | map[killed:20] |
| harness max RSS | 289 MiB before reconciling, 320 MiB after reconciling every chunk against 157 MiB of worker logs |

| chunk | jobs | by shard | acknowledged | lost | unexplained | attempts | max RSS after |
|---|---|---|---|---|---|---|---|
| 0 | 100000 | [50000 50000] | 100000 | 0 | 1 | 100009 | 320 MiB |
| 1 | 87403 | [43702 43701] | 87403 | 0 | 0 | 87416 | 320 MiB |

events.csv lists every fault; timeline.csv every 30 s sample (throughput.svg, pickup-p99.svg, table.svg).
