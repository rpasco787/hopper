# GKE: shard scaling and the soak

## Setup

- Each shard is a CloudNativePG Cluster with one synchronous standby
  (`synchronousStandbys: 1`), on pd-ssd (the improvement plan's local SSD
  needs a local-volume provisioner).
- The worker layout used on GKE is not recorded.

## 1. Shard scaling (median of 3 drains of 1M jobs, 300 B)

| Metric | Value |
|---|---|
| 1-shard drain | 13.1k jobs/s |
| 4-shard drain | 47.0k jobs/s |
| 1 → 4 shards | 3.6× (90% of linear) |
| Spread of 3 drains, (max − min) ÷ median | 19% |

## 2. Capacity at 4 shards (10-minute steady runs)

| Metric | Value |
|---|---|
| Capacity as a share of the drain rate | 71% (≈ 33.4k jobs/s) |
| Enqueue p99 at capacity | 18 ms |

## 3. Pre-soak (30 minutes)

| Metric | Value |
|---|---|
| Highest load kept up with | 68% of capacity |
| Timeline flat | not reported |

## 4. The soak (4 shards; worker kill every 3 m, broker restart every 15 m, one shard-1 failover at half)

| Metric | Value |
|---|---|
| Jobs | 10M |
| Lost jobs | 0 |
| Unexplained repeats | 0 |
| Redelivery rate | 1e-4 |
| Completed rate vs offered | within 0.9% |
| Shard failover: promotion | 26 s |
| Shard failover: back to offered rate | 3 min |
| Per-shard table size | levels off at 42 min |
| Pickup p99 | 140 ms |
| `n_dead_tup` per shard | not reported |
| Soak enqueue p99 (through a port-forward) | not reported |

## 5. Cost

Not published.
