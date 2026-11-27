# Soak 70d3665

Ref 70d3665, broker image `ghcr.io/rpasco787/hopper-broker:dev-1795663844`, context `kind-hopper`: 2 shards (bundled), 2 brokers, 3 workers; 200 jobs/s offered open loop (at most 256 in flight), 50 ms jobs, chunks of 100000.

| | |
|---|---|
| jobs | 184292 |
| duration | 15m0s planned; produced for 921 s, drained in 10 s |
| completed/s | mean 200.0, p99 200.4 (30 s samples while producing) |
| enqueue p99 | 17.5 ms |
| behind | 0 of 184292 offered calls could not start |
| failed enqueue calls | map[] |
| faults | broker-restart 3, worker-kill 5; 0 errors |
| skipped | failover: bundled mode: a shard has no standby to promote; run with postgres.mode=cloudnativepg |
| worker container restarts | 5 |
| lost | 0 |
| unexplained | 0 |
| rows on the wrong shard | 0 |
| redelivery rate | 0.0000 ((Σ attempts − jobs) / jobs = (184301 − 184292) / 184292) |
| repeated starts by cause | map[killed:7] |
| harness max RSS | 306 MiB before parsing logs, 331 MiB after parsing 151 MiB of them |

| chunk | jobs | by shard | acknowledged | lost | unexplained | attempts | max RSS after |
|---|---|---|---|---|---|---|---|
| 0 | 100000 | [50000 50000] | 100000 | 0 | 0 | 100005 | 339 MiB |
| 1 | 84292 | [42146 42146] | 84290 | 0 | 0 | 84296 | 348 MiB |

events.csv lists every fault; timeline.csv every 30 s sample (throughput.svg, pickup-p99.svg, table.svg).
