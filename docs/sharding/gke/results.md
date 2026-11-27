# GKE: shard scaling and the soak (Task 20)

These results come from the Task 20 sitting, which was run and reduced by
hand. The numbers below are the ones reported from that sitting. Its raw
files are **not in the tree**: `scaling.csv`, `docs/sharding/gke/soak/`
(`summary.md`, `report.json`, `timeline.csv`, `events.csv`),
`scripts/gke-soak.sh` and `deploy/gke/soak-values.yaml`. Until they are
committed, this file is the record, and no number here can be re-derived
from the tree. The day's spend was not published.

The bands (okay / good / great) and the predictions were written before the
numbers came in. Only the 1 → 4 targets (2.5–3× good, 3.5×+ great) and the
plan's pass criteria predate this sitting. Everything else was set during the
same session and was not committed ahead of the run, so treat those bands as
a yardstick, not a pre-registration.

The predictions came in two versions:
- "as planned" assumed the plan's 8 workers × 16 slots, which H6 found
  starved of credit;
- "slot-rich" assumed a layout with more slots per worker.

## Pass criteria (plan, Task 20 "Done when" and the improvement plan)

| Criterion | Result |
|---|---|
| Throughput measured at 1, 2 and 4 shards | ✅ |
| Soak ≥ 10M jobs | ✅ 10M |
| Zero lost | ✅ 0 |
| Zero unexplained | ✅ 0 |
| Enqueue p99 < 25 ms at capacity | ✅ 18 ms |
| 1 → 4 throughput in the "good" band (2.5–3×) | ✅ 2.7× |
| Cluster deleted | not recorded here |

## 1. Shard scaling (median of 3 drains of 1M jobs, 300 B)

| Metric | Okay | Good | Great | **Actual** | **Verdict** | Prediction | Hit? |
|---|---|---|---|---|---|---|---|
| 1 → 4 shards | ≥ 2.0× | 2.5–3.0× | ≥ 3.5× | **2.7×** | **Good** | 1.3–2.0× as planned; 2.5–3.2× slot-rich | ✅ slot-rich |
| 1 → 2 shards | ≥ 1.4× | ≥ 1.7× | ≥ 1.9× | **1.8×** | **Good** | 1.2–1.5× / 1.6–1.9× | ✅ slot-rich |
| 1-shard drain | ≥ 5k/s | ≥ 10k/s | ≥ 15k/s | **11.0k/s** | **Good** | 6–12k/s | ✅ |
| 4-shard drain | ≥ 15k/s | ≥ 30k/s | ≥ 50k/s | **29.7k/s** (11k × 2.7) | **Okay** (0.3k/s short of good) | 10–20k/s / 20–35k/s | ✅ slot-rich |
| Spread of 3 drains, (max − min) ÷ median | ≤ 25% | ≤ 15% | ≤ 10% | **19%** | **Okay** | 10–20% | ✅ |

Gains shrink as shards are added:
- 1 → 2 gives 90% of linear (1.8 of 2).
- 2 → 4 gives 75% (2.7 ÷ 1.8 = 1.5 of 2).

At 4 shards something that is not sharded starts to bind: brokers, worker
slots or the load generator. Find it before trying 8 shards.

One GKE shard (11.0k/s) is slower than the laptop's best single shard
(15.8k/s, `docs/bench/4/medians.txt`). That is consistent with every commit
waiting on the synchronous standby (`synchronousStandbys: 1`) and on pd-ssd's
network flush. It is the per-shard price of a failover that loses nothing.

On the laptop a second shard gave 1.03–1.15× (Task 17). Each shard there
shared one disk, and the drain was bound by WAL-flush latency. On GKE each
shard has its own disk and node, and the ratio follows. This supports Task
17's hypothesis.

## 2. Capacity at 4 shards (10-minute steady runs)

| Metric | Okay | Good | Great | **Actual** | **Verdict** | Prediction | Hit? |
|---|---|---|---|---|---|---|---|
| Capacity as a share of the drain rate | ≥ 50% | ≥ 70% | ≥ 85% | **71%** (≈ 21.1k/s) | **Good** | 60–80% | ✅ |
| Enqueue p99 at capacity | < 25 ms | < 15 ms | < 10 ms | **18 ms** | **Okay** | 10–20 ms | ✅ |

## 3. Pre-soak (30 minutes)

| Okay | Good | Great | **Actual** | **Verdict** | Prediction | Hit? |
|---|---|---|---|---|---|---|
| keeps up at 50% | keeps up at 70% | 70%, flat timeline | **kept up at 68% of capacity** | **Okay**, just under good | ~60% chance of holding 70% | ≈ |

Whether the timeline stayed flat was not reported.

## 4. The soak (worker kill every 3 m, broker restart every 15 m, one shard-1 failover at half)

| Metric | Okay | Good | Great | **Actual** | **Verdict** | Prediction | Hit? |
|---|---|---|---|---|---|---|---|
| Lost jobs | 0 (pass/fail) | — | — | **0** | **Pass** | 0 | ✅ |
| Unexplained repeats | 0 (pass/fail) | — | — | **0** | **Pass** | 0 | ✅ |
| Jobs | ≥ 10M | ≥ 10M, every fault fired | ≥ 25M or ≥ 4 h | **10M** | **Okay** (good if every scheduled fault fired) | — | — |
| Redelivery rate | < 1e-3 | < 1e-4 | every repeat traced to a kill | **1e-4** | **Okay**, on the good line | 1e-4 – 5e-4 | ✅ low end |
| Completed rate vs offered | within 2% | within 1% | within 1%, no drift | **within 0.9%** | **Good** | within 1–2% | ✅ better |
| Shard failover: promotion | < 60 s | < 30 s | < 20 s | **26 s** | **Good** | 15–30 s | ✅ |
| Shard failover: back to offered rate | < 5 min | < 2 min | < 1 min | **3 min** | **Okay** | 1–2 min | ❌ slower |
| Per-shard table size | grows sublinearly | levels off within 1 h | flat after warm-up | **levels off at 42 min** | **Good** | levels off | ✅ |
| Pickup p99 | < 1 s | < 250 ms | < 100 ms | **140 ms** | **Good** | 50–200 ms | ✅ |
| `n_dead_tup` per shard | — | — | — | not reported | not measured | — | — |
| Soak enqueue p99 (through a port-forward) | — | — | — | not reported | not measured | 30–60 ms | — |

### Recovery after the failover was the one miss

The soak ran near 70% of capacity, which leaves roughly 40% spare. A 26 s
stall of the whole system would drain in about a minute. Only one shard in
four was down, and unkeyed enqueues moved off it. A 3-minute return to the
offered rate therefore points to something slow after promotion, not to the
backlog itself. The candidates:
- a cold cache on the new primary;
- broker pools reconnecting with backoff;
- the replacement standby catching up while synchronous commits wait on it.

The shard-1 rows of `timeline.csv` around the failover would tell them apart.
A slow ramp suggests the cache; a flat gap then a jump suggests the
reconnects. Kind's CloudNativePG rehearsal promoted in 18.6 s
(`docs/sharding/soak-kind/`).

## 5. Cost

Not published.

## Tally

- 9 good, 7 okay, 0 great.
- Both pass/fail gates passed.
- Not measured: `n_dead_tup`, soak enqueue p99 and cost.
- Predictions: 14 of 15 scored rows fell in range (pre-soak counted as a
  near-hit), all in the slot-rich ranges. The miss was failover recovery.

## Deviations from the plan

- pd-ssd replaces the improvement plan's local SSD. CloudNativePG on local
  SSD needs a local-volume provisioner, which is a project of its own.
- The raw evidence, the GKE script and the values file are not committed (see
  the top of this file).
- The worker layout used on GKE is not recorded here.
