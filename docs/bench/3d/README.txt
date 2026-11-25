Phase 3d: the Go worker against the Java worker. Task 19 writes the rest of
this file.

Decisions taken while running
- Task 2's test expected '1200', a plan arithmetic error: the data give 60 + 80
  = 140 ticks, so '1400'. The code is unchanged and the test equally strict.
- Tasks 4 and 5: the plan's tests needed BENCH_TEST_ACKS=JOBS added, because
  the fake curl reports 100 Acks by default and the drain's Ack check would
  reject the run. Their assertions are unchanged.
- Task 6: the JFR variant check sits after EXPLAIN's, in the variant block.
- Task 7: median_of and the warm-throughput list skip '-', bench_workers.py's
  can't-compute marker, which float('-') would otherwise crash on. A test
  covers it.
- common.sh's preflight also excludes .gitignore from its "tree differs from
  HEAD outside docs/" check. The user keeps an unstaged '.claude' line there,
  which would otherwise refuse every loop, and no build reads it: every image
  builds from REF through git archive.
- The rate.psv fix, 0acfa0a, found by the pilot's first attempt. The query
  joined jobs to its own min(leased_at); planned on the pre-drain statistics
  (no completed rows) it became a nested loop that rescanned jobs per
  completed job, and the 200,000-job go-long drain's query was still running
  after 12 minutes. A scalar subquery is an InitPlan, run once (89 ms on
  200,000 rows); bench_test.py has a test for it. The attempt was stopped, its
  partial evidence discarded, and the pilot rerun whole. It doesn't move
  ref.txt: it changed only scripts/, and the harness is always the working
  tree's, while REF (14e87ee) builds the brokers and workers.
- A stray process: before the first pilot attempt an orphaned
  `bash scripts/bench.sh drain` subshell (parent 1, from an earlier
  bench_test.py run cut off by its timeout, its fake docker gone) was
  spinning on the host. It was killed. Check `ps -ef | grep scripts/bench.sh`
  before each loop.
- Pilot R1: warmup.py gave WARM_JOBS=200000 for both SDKs, so the rule's cap
  applies: WARM_JOBS=100000, JOBS=140000 (pilot/decision.txt has the
  per-second curves; Go's late-drain slowdown, not a ramp, set its number).
- Pilot R2: go-steady kept up at 1,000 jobs/s, so RATE=1000.
- Pilot R3: both wide runs reached max_leased 10,000 under 80% of 1g (Go
  206.3 MiB, Java 275.2 MiB), so WIDE_MEM=1g, WIDE_MS=15000, WIDE_LEASE=60s.
- Pilot R4: WIDE_JFR = 10000/603 + 15, rounded up to a multiple of 5 = 35.
