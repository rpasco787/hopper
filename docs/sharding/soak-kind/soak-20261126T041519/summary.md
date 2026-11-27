# Soak 85bf556

Ref 85bf556, broker image `ghcr.io/rpasco787/hopper-broker:dev-1795666476`, context `kind-hopper`: 2 shards (bundled), 2 brokers, 3 workers; 200 jobs/s offered open loop (at most 256 in flight), 50 ms jobs, chunks of 100000.

| | |
|---|---|
| jobs | 192509 |
| duration | 15m0s planned; produced for 963 s, drained in 5 s |
| completed/s | mean 200.0, p99 203.2 (30 s samples while producing) |
| enqueue p99 | 44.1 ms |
| behind | 0 of 192509 offered calls could not start |
| failed enqueue calls | map[] |
| faults | broker-restart 3, worker-kill 5; 0 errors |
| skipped | failover: bundled mode: a shard has no standby to promote; run with postgres.mode=cloudnativepg |
| worker container restarts | 5 |
| lost | 0 |
| unexplained | 0 |
| rows on the wrong shard | 0 |
| redelivery rate | 0.000031 ((Σ attempts − jobs) / jobs = (192515 − 192509) / 192509) |
| repeated starts by cause | map[killed:3] |
| harness max RSS | 457 MiB before reconciling, 457 MiB after reconciling every chunk against 154 MiB of worker logs |

| chunk | jobs | by shard | acknowledged | lost | unexplained | attempts | max RSS after |
|---|---|---|---|---|---|---|---|
| 0 | 100000 | [50000 50000] | 100000 | 0 | 0 | 100005 | 457 MiB |
| 1 | 92509 | [46254 46255] | 92508 | 0 | 0 | 92510 | 457 MiB |

events.csv lists every fault; timeline.csv every 30 s sample (throughput.svg, pickup-p99.svg, table.svg).
