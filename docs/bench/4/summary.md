# Task 17: one shard against two, one CPU each (laptop)

- **Ref:** `64ac8a9`, harness `64ac8a9`. The broker and worker code is
  unchanged since `12a0b92`; the commits after it touch only the bench
  harness.
- **Setup:** 2 brokers on CPUs 2-3, Go workers on CPUs 4-6, `PIN=1`, 40,000
  jobs of 300 B per drain, `synchronous_commit=on`. Each PostgreSQL shard
  gets one CPU: shard 0 on CPU 0, shard 1 on CPU 1.
- **Arms:** two layouts, w8c16 (8 × 16, the plan's) and w2c256 (2 × 256,
  credit-rich). Each ran at `SHARDS` 1 and 2, five runs per configuration,
  interleaved and rotated per repetition.
- **When:** 2026-11-26, 02:53:17-03:08:55 UTC, on AC power under
  `caffeinate -dimsu`.
- **Health of the loop:** `sleeps.txt` is empty. There were 0 failed or
  rejected runs and 0 retried jobs, and every configuration has 5 rows.
- **The prediction** (`prediction.md`) was committed in `4ee25e4`, before
  the first run of any loop.
- **An earlier loop was discarded.** Four runs into it, leftover harness
  processes were loading the laptop (`docs/sharding.md` → Decisions). Its rows
  are not results.
- **Evidence:** `results.csv`, `loop.log`, `loop-settings.txt`,
  `medians.txt`, `decision.txt`, `summary-tables.md`, and the 20 run
  directories.
- **Chart:** [`docs/img/shards-laptop.svg`](../../img/shards-laptop.svg).

## Results

| layout | shards | jobs/s median (range) | ratio | commits/job | WAL syncs/job | WAL syncs/s | jobs per WAL sync | ack batch | postgres CPU (sum) | broker CPU (sum) |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| w8c16 | 1 | 6,727 (6,209-7,277) | | 0.34 | 0.068 | 468 | 14.7 | 9.2 | 58 | 33 |
| w8c16 | 2 | 7,715 (6,700-8,120) | **1.15×** | 0.44 | 0.145 | 1,112 | 6.9 | 4.8 | 75 (39 + 36) | 37 |
| w2c256 | 1 | 15,808 (12,583-17,538) | | 0.12 | 0.045 | 709 | 22.2 | 15.0 | 38 | 31 |
| w2c256 | 2 | 16,301 (13,442-16,709) | **1.03×** | 0.14 | 0.083 | 1,320 | 12.0 | 10.3 | 44 (22 + 22) | 31 |

- **Verdicts by the evidence rule** (`decision.txt`, `bench_compare.py
  --column shards`): no measurable effect in either arm. The 1- and 2-shard
  ranges overlap in both, and `bench_summary.py` says "inconclusive (ranges
  overlap)".
- **Layout:** w2c256 drains 2.35× faster than w8c16 at one shard, and 2.11×
  at two. Both "help" by the rule.
- **Wait events** (active backends, all samples): `LWLock:WALWrite` is the
  largest share in every configuration, followed by `IO:WALSync`.
  PostgreSQL spends its time waiting for the WAL to be written and flushed,
  not on CPU.

## Against the prediction

1. **Plan's claim: two shards drain 1.6-2.0× one shard. Wrong in both
   arms** (1.15× and 1.03×).
2. **Plan's claim: commits per job within 10%. Wrong.** They rose 29% at
   w8c16 (0.34 to 0.44) and 17% at w2c256 (0.12 to 0.14). My prediction said
   the claim would fail, at 10-40% more commits per job, and gave the
   mechanism: one ack batcher per shard. That held. The median ack batch fell
   from 9.2 to 4.8 at w8c16 and from 15.0 to 10.3 at w2c256.
3. **Plan's claim: a ratio under 1.3× means the broker CPUs are the ceiling.
   Wrong here.** The ratio is under 1.3× in both arms, but `broker_cpu` is
   31-37% summed over both brokers, against a ceiling of 180. The brokers are
   nowhere near it.
4. **My w8c16 numbers:** I predicted about 8,000 jobs/s at one shard
   (measured 6,727, inside my 6,500-10,000) and a ratio of about 1.35×
   (1.2-1.6). Measured 1.15×: **wrong, below my range.** My reason, a
   credit-starved layout, is also not what this loop shows (point 7).
5. **My w2c256 numbers:** I predicted about 16,000 jobs/s at one shard
   (measured 15,808: right) and a ratio of about 1.6× (1.4-1.9). Measured
   1.03×: **wrong.**
6. **"The credit-rich arm shows a larger ratio": wrong.** Its ratio (1.03×)
   is smaller than w8c16's (1.15×), and neither difference is measurable.
7. **What I named as a possible cap for w2c256 at two shards:** if the
   ratio fell under 1.4× with `broker_cpu` under 150 and neither shard's
   `postgres_cpu` near 90, I would name the shared disk. All three conditions
   hold: brokers at 31, shards at 22 and 22. The run directories support a
   more specific version of that, though this loop doesn't prove it:
   - Two shards flush the WAL more than twice as often as one: 1,320 against
     709 syncs a second at w2c256, and 1,112 against 468 at w8c16. The VM's
     disk isn't out of flushes per second.
   - But each flush now carries about half as many jobs: 12.0 against 22.2,
     and 6.9 against 14.7.
   - A drain is a closed loop. At most the workers' slots are in flight (512
     or 128), and splitting them across two shards halves each shard's group
     commit.
   - So throughput is bound by how long a commit waits for its flush, times
     a fixed number of jobs in flight, and adding a database doesn't shorten
     that wait.
   - This also explains why PostgreSQL CPU is low (22-39% a shard) while
     `LWLock:WALWrite` dominates the waits.

   Untested. The test would be the same loop with more slots (or `WORK_MS` so
   that more jobs are in flight), or with one disk per shard.
8. **"No run is rejected or retried":** right.

## What this shows and doesn't

- **On this laptop, sharding doesn't raise drain throughput.** That holds at
  either layout, with each shard on its own CPU and both shards on one
  virtual disk. The database CPU was never the ceiling (PostgreSQL ran at
  22-61% of a CPU a shard), so the one-CPU-per-shard comparison tested the
  wrong limit for this workload. Task 20's GKE run, with a disk per shard and
  more clients, is where throughput against shard count can be measured.
- **The cost of sharding a closed-loop drain is visible:** more commits per
  job (+17-29%) and about twice the WAL syncs per job. Both follow from one
  confirmation batcher per shard, which the plan chose so that a dead shard
  can't hold up another's acks.
- **The layout matters more than the shard count here.** At one shard,
  2 × 256 drains 2.35× faster than 8 × 16, which agrees with H6's direction.
