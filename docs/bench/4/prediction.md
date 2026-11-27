# Task 17 prediction: one shard against two, one CPU each

Written 2026-11-25, before the first run of `docs/bench/4/loop.sh`, and
committed with the harness that runs it.

## The loop

- 2 brokers (CPUs 2-3 between them), Go workers on CPUs 4-6, `PIN=1`,
  40,000 jobs of 300 B per drain, `synchronous_commit=on`.
- Each PostgreSQL shard gets one CPU: `SHARDS=1` runs `postgres` on CPU 0
  alone, `SHARDS=2` adds `postgres-1` on CPU 1.
- Two layouts, five runs of each of the four configurations, interleaved and
  rotated per repetition:
  - **w8c16**, 8 workers × 16 slots: the plan's arm, the 1b loops' layout.
  - **w2c256**, 2 workers × 256 slots: the credit-rich layout. H6
    (`docs/scaling.md`, `docs/bench/1b/h6/`) found that 8 × 16 gets 0.47× of
    one broker's best (11,348 jobs/s against 24,330 at 2 × 256), and its
    follow-up says shard baselines should also be measured at a credit-rich
    layout. With 2 brokers, each broker serves one 256-slot stream.

## The plan's claims (for the w8c16 arm, and I hold w2c256 to them too)

1. If throughput follows database capacity, two shards drain between 1.6×
   and 2.0× one shard. Brokers, workers and the loadgen are unchanged, and
   the brokers' two CPUs may become the ceiling.
2. Commits per job stay within 10% of one shard's, since sharding moves
   commits and doesn't add any.
3. If the ratio is under 1.3×, the broker CPUs are the likely ceiling: check
   `broker_cpu` near 200% (the summary calls the brokers saturated at 180).

## My predictions

Numbers are medians; every one is a guess with wide error bars, written so
the result can prove it wrong.

- **w8c16, one shard:** about 8,000 jobs/s (6,500-10,000). PostgreSQL on one
  CPU is the ceiling: `postgres_cpu` at 85 or more of its 90.
- **w8c16, two shards:** about 11,000 jobs/s, a ratio of **about 1.35×
  (1.2-1.6)**: *under* the plan's 1.6-2.0. The reason I expect is the
  layout, not the brokers. At 8 × 16 a stream leases only the credit that came
  back since its last lease (about 6 rows a call in H6), so worker-side
  credit, not database capacity, holds the drain once PostgreSQL has a second
  CPU. `broker_cpu` stays well under 180 (about 100-130 summed over both
  brokers), so if the ratio is under 1.3× the plan's diagnosis (broker CPUs)
  will **not** hold; the layout will.
- **w2c256, one shard:** about 16,000 jobs/s (12,000-20,000), PostgreSQL
  saturated on its one CPU. In H6, at 24,330 jobs/s on two CPUs, PostgreSQL
  used 88%, which is about all of one CPU, but with one broker and 1b-era
  statistics. It is well above w8c16's one-shard number, because a 256-slot
  stream takes close to its 32-row cap a call.
- **w2c256, two shards:** about 26,000 jobs/s, a ratio of **about 1.6×
  (1.4-1.9)**.
- **Does the credit-rich arm show a larger ratio? Yes.** w2c256's ratio
  exceeds w8c16's by at least 0.15. A layout that isn't held back by credit
  has more to gain from a second database.
- **What may cap w2c256 at two shards:** the brokers (`broker_cpu` 130-170
  summed) or the disk. Both shards' WAL sits on the same Docker VM disk, so a
  commit-flush ceiling there would not double. That is why the plan defers
  real disks to Task 20. If the ratio is under 1.4× with `broker_cpu` under
  150 and neither shard's `postgres_cpu` near 90, I will name the shared disk
  as the likely ceiling. That stays a hypothesis: this loop doesn't measure it.
- **Commits per job: the plan's claim 2 fails, I think.** Two shards will
  run 10-40% more commits per job than one, in both arms. The broker keeps
  one ack batcher (and one heartbeat batcher) per shard, so each batch
  collects only the acks for its own shard's jobs. At about the same per-batch
  latency, batches are smaller and there are more of them per job (`ack_batch`
  falls). Lease calls per job stay within 10%: with both shards full, a
  lease fills from its first shard and never moves on.
- **WAL syncs per job** rise with commits per job: two servers each flush
  their own WAL.
- **No run is rejected or retried.** Every drain finishes with `retried = 0`.

## What would void the loop

- `sleeps.txt` is not empty (the laptop slept).
- Any configuration ends with fewer than 5 rows.
- The harness column shows `-dirty`.
