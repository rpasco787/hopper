# Soak d78ac4b

Ref d78ac4b, broker image `ghcr.io/rpasco787/hopper-broker:dev-1795667566`, context `kind-hopper`: 2 shards (cloudnativepg), 2 brokers, 3 workers; 200 jobs/s offered open loop (at most 256 in flight), 50 ms jobs, chunks of 100000.

| | |
|---|---|
| jobs | 58241 |
| duration | 5m0s planned; produced for 314 s, drained in 6 s |
| completed/s | mean 196.0, p99 200.2 (30 s samples while producing) |
| enqueue p99 | 865.2 ms |
| behind | 4426 of 62835 offered calls could not start |
| failed enqueue calls | map[DeadlineExceeded:2627 Unavailable:351] |
| faults | broker-restart 1, failover 1, worker-kill 1; 0 errors |
| worker container restarts | 1 |
| lost | 0 |
| unexplained | 0 |
| rows on the wrong shard | 0 |
| redelivery rate | 0.000155 ((Σ attempts − jobs) / jobs = (58250 − 58241) / 58241) |
| repeated starts by cause | map[killed:2] |
| harness max RSS | 62 MiB before reconciling, 137 MiB after reconciling every chunk against 47 MiB of worker logs |

| chunk | jobs | by shard | acknowledged | lost | unexplained | attempts | max RSS after |
|---|---|---|---|---|---|---|---|
| 0 | 58241 | [29114 29127] | 58203 | 0 | 0 | 58250 | 137 MiB |

events.csv lists every fault; timeline.csv every 30 s sample (throughput.svg, pickup-p99.svg, table.svg).
