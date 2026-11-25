Phase 3d: the Go worker against the Java worker. The write-up is
docs/bench/java.md; this file indexes the evidence and records every decision
taken while running.

The measured ref: 14e87ee (ref.txt). Every loop except profile/ builds the
brokers and workers from it with git archive; the harness (scripts/) is the
working tree's, and each row's harness column records its commit. profile/
builds from d9370e1, the HEAD that added cmd/worker -debug-listen (D16).

What each directory holds, and what made it
- common.sh: shared by every loop: the measured ref, the preflight checks
  (tree at HEAD outside docs/, no other container, 8 CPUs, AC power), the
  sleep check for a loop's window, and row lookups.
- prediction.txt: written before any pilot or loop run; java.md's "Against
  the prediction" checks each line.
- pilot/ (pilot/run.sh, warmup.py): six runs that set the loops' settings by
  the plan's rules R1-R4: decision.txt says why, settings.env holds them,
  warmup.txt is warmup.py's output. Not results.
- compare/ (compare/loop.sh): the comparison, drain and steady x Go and Java
  x 5, interleaved and rotated. results.csv, summary.md, decision.txt
  (bench_compare.py, go control, java treatment), loop-settings.txt,
  loop.log, sleeps.txt, and one evidence directory per run.
- wide/ (wide/loop.sh): 10,000 handlers on one worker, 3 runs per SDK at warn
  and one proof run each at info. peaks.txt holds the pinned-event count and
  TestPeaks' output; the java-proof run's directory (...T223415) holds the
  JFR text extracts (worker-1.jfr-*.txt). The loop also reruns
  pinning/control.sh on the measured image.
- pinning/ (pinning/control.sh, PinControl.java): the positive control for
  the pinned-event count; control.txt is its result in the measured image.
- profile/ (profile/loop.sh): Part E's diagnostics at d9370e1: a Go and a
  Java drain with profiles, and a Go 10,000-handler run with heap and
  goroutine profiles. worker-cpu-top.txt, worker-memstats.txt,
  worker-goroutines.txt, worker-goroutine.txt, worker-heap.txt and the Java
  run's worker-1.jfr-*.txt are the committed extracts.
- layout/ (layout/loop.sh): a follow-up after Task 21, at the measured ref:
  the same 128 slots on the same three worker CPUs as 8x16, 2x64 and 1x128,
  for both SDKs, 3 drains each, rotated. results.csv, summary.md,
  decision.txt (each layout's java against go, and java at 2x64 and 1x128
  against java at 8x16), loop-settings.txt, loop.log, sleeps.txt.

What stays local (gitignored; .gitignore, plan section 6 and D15)
- raw worker logs (*.stdout, *.stderr): per-run logs of up to 140,000 jobs,
  large, and summarized by the committed rows and extracts;
- JFR recordings (*.jfr) and Go CPU profiles (worker-cpu.pprof): binary and
  large; their text extracts are committed beside them;
- logs.txt and broker-cpu.pprof, as before Phase 3d.
They stay in each evidence directory on the machine that ran it.

Reading a .jfr
  "$(/usr/libexec/java_home -v 25)/bin/jfr" summary <file>
  "$(/usr/libexec/java_home -v 25)/bin/jfr" view --width 200 pinned-threads <file>
  "$(/usr/libexec/java_home -v 25)/bin/jfr" print --events jdk.VirtualThreadPinned <file>
The host JDK's bin/jfr is not on PATH; common.sh's JFR_TOOL points at it.
Go profiles: go tool pprof -top <dir>/worker-cpu.pprof.

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
- The pilot's rows carry harness=0acfa0a-dirty only because of the user's
  uncommitted '.claude' line in .gitignore (bench.sh's harness check looks at
  everything outside docs/, .gitignore included). No build reads .gitignore:
  every image builds from REF through git archive. Before Task 13 the user
  moved that line to .git/info/exclude, so the tree matches HEAD outside
  docs/ and the comparison's rows carry a clean harness.
- Task 13: before the loop, `ps -ef | grep "[b]ash scripts/bench.sh"` found
  no stray process, `docker ps -q` was empty and the laptop was on AC power.
  shellcheck isn't installed on this host, so loop.sh was checked with
  `bash -n` only.
- Task 13 ran 2026-11-25 05:47:13-06:25:34 UTC (38 min): 20 rows, harness
  8726d9d (clean), no FAILED line, no rejected.txt, sleeps.txt empty, so D17
  needed no rerun. Every drain's max_leased was at most 126 (of 128); every
  Java jvm-flags.txt shows UseSerialGC {command line} and a 288 MiB
  MaxHeapSize. The loop's first run is "java drain", as the plan's rotation
  ((i + k) % 4 from i=1) gives; not reordered.
- Prediction misses, for java.md (prediction.txt's own "measurement wrong"
  tests): Go's drain median, 7,699 jobs/s (7,240-8,508), sits below the
  H4-credit range (8,543-10,143). Not taken as a broken measurement: those
  were 40,000-job drains, these are 140,000, and every Go drain ended with
  280,000 dead tuples (Java's 30,902-82,549), the late-drain slowdown the
  pilot's rate curve showed (pilot/decision.txt). One Go drain's warm window
  (6,751) is 10.2% below its cold rate (7,519), on the same cause; the
  medians are warm 8,088 above cold 7,699. Java's startup (drain 4,119 ms,
  steady 2,634) is far above the predicted 1,500 ms; its warm steady CPU
  (915 ms per 1k) is below its cold drain CPU (1,491), as predicted.
- Task 14: before the loop, no stray `bash scripts/bench.sh` process,
  `docker ps -q` empty, on AC power. wide/loop.sh is the plan's text
  verbatim, checked with `bash -n` only (no shellcheck on this host). It ran
  2026-11-25 06:27:44-06:35:33 UTC (8 min): 8 rows, harness 88a8271 (clean),
  no FAILED line, no rejected.txt, sleeps.txt empty, so D17 needed no rerun.
  Every row has max_leased 10,000 and retried 0. peaks.txt: both proof
  processes held 10,000 deliveries open at once, PASS, go test exit 0.
  pinning/control.txt was rerun by the loop on the measured image
  (hopper-bench/worker-java:ref-14e87ee, sha256:ddd75cf7...): 1 event at
  0 ms, 0 at 20 ms, PASS. It replaces the smoke's uncommitted control.txt
  (run on hopper-example-worker:dev).
- Task 14, the pins (for java.md; a finding, not fixed here, §1): 72
  jdk.VirtualThreadPinned events in the java-proof recording, not ~50 and not
  all in the first second as predicted. Every one is a first use of a class:
  58 "Contended monitor enter" (56 "Freeze or preempt failed (2)" in
  BuiltinClassLoader.loadClass, e.g. from Execution.run:46; 2 "Native or VM
  frame on stack" in ReferencedKeyMap.intern) and 14 "Object.wait", each
  "Waited for initialization of <class> by another thread" (Jackson
  JsonEncoding and UTF8StreamJsonParser, AckRequest, HeartbeatRequest,
  Confirmer$Outcome, HandlerStatus, grpc ClientCalls$GrpcFuture). They come
  in two bursts: 24 at 06:34:39 (the first deliveries, about 1 s after the
  JVM started) and 48 at 06:34:54 (the first completions, 15 s later, when
  the ack path's classes load for the first time). None while the handlers
  sleep. Longest 38.0 ms (twice, Jackson ByteSourceJsonBootstrapper
  .constructParser, at 06:34:39.284); the rest under 8 ms. Follow-up:
  preload or initialize those classes before the first delivery (or an AOT
  cache, D18), so the first bursts don't pin carriers.
- Task 14, the java-proof JFR views (for java.md): Serial collector
  (UseSerialGC from the command line), heap 16 MB initial, 768 MB max; 51 GC
  pauses, 312 ms in all, median 3.47 ms, largest 107 ms (the one old
  collection, GC 51 at 06:34:54, as the completions began); heap after GC at
  its highest on the plateau 73.1 MB (GC 48, 06:34:40, after which no GC ran
  until 06:34:54), 80.7 MB at its highest overall (GC 54, during the
  completions); native memory committed at its maximum: Java heap 171.8 MB,
  Tracing (JFR) 21.3, Code 20.0, Metaspace 19.0, Shared class space 12.7,
  Arena 12.6, Other 12.2, Symbol 4.1, Class 2.9, NMT 2.2, Thread Stack 0.4
  (virtual threads' stacks are heap StackChunks); Java threads peak 21 (OS
  threads from /proc: 29 in this run, 25 in every java-wide run).
- Task 14, prediction misses (for java.md): Go's peak RSS 210 MiB
  (208-212), not 110; Java's 249 MiB (220-251), under the predicted 300.
  Threads Go 9 (predicted 12), Java 25 (predicted 30). The pin count and
  timing above.
- Part E ran, at d9370e1 (HEAD after Task 17, per D16), not ref.txt's
  14e87ee. The loop's three runs were accepted first time; sleeps.txt is
  empty. Their throughputs (go-profile 5,162, java-profile 1,710 jobs/s at
  200,000 jobs) are diagnostics, not results: profiling costs CPU and the
  drain is longer than Task 13's.
- Task 18, the memstats extract: the plan's grep named StackInuse and
  StackSys, but Go's heap?debug=1 text prints one line, "# Stack = inuse /
  sys", so the extract had no stack line. loop.sh greps for "Stack" instead
  (same anchors, one line per run, as strict), and the extract was re-made
  from the saved worker-heap.txt with that regex; no run was repeated. The
  stack is the wide run's largest item (121 MiB of 204 MiB Sys).
- Task 18, the Java recording covers JVM start plus ~21 s of a ~117 s drain
  (JFR=25 as planned), so its hot-methods view is the early, JIT-heavy part of
  the drain, not a steady state: compiler-statistics reports 21.5 s of
  compilation time and gc-pauses 1.22 s over that window.
- Task 19, java.md: the ramp paragraph sums each compare drain's rate.psv
  into seconds; no extract was added. The slog.With attribution (11.1%) is
  `go tool pprof -peek 'slog.\(\*Logger\).With$'` on the local
  worker-cpu.pprof of profile/...T224141; the committed worker-cpu-top.txt
  shows the same cost as appendJSONMarshal, 10.2% cumulative. The verdicts
  were re-run with scripts/bench_compare.py (go control, java treatment) and
  match compare/decision.txt and wide/decision.txt.
- Task 19, the 3d target: "within 10% of Go" (D14) is not met (0.266x cold,
  0.355x warm), and "zero pinning events" is not met (72, all class loading
  and initialization). Both are reported as measured; nothing was retuned to
  move them.
- After Task 21: layout/ asked whether the comparison's Java gap is the
  layout (eight JVMs on three CPUs) or Java's per-job cost. Both SDKs ran at
  each layout so every ratio is within one loop. 8x16 workers get 384m as in
  compare/; 2x64 and 1x128 get 1g as in wide/, so one JVM is not short of
  heap. The answer is mostly the layout: one JVM drains 3.7x faster than
  eight, and Java reaches 0.60x Go cold and 0.73x warm. Go nearly doubled
  too, so java.md and the README no longer say a drain at one broker is bound
  by the broker and PostgreSQL; the 8x16 figures stay, as that layout's.
  The 3d target ("within 10%") is still not met.
