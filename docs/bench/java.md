# The Go and Java workers, measured on one stack (Phase 3d)

`cmd/worker` (the Go SDK) against `hopper-example-worker` (the Java SDK), on
the benchmark stack, 2026-11-24. Every number below names the file it comes
from; paths are under `docs/bench/3d/` unless they say otherwise. The evidence
index, and every decision taken while running, are in
[3d/README.txt](3d/README.txt). The plan is
`docs/plans/2026-11-24-phase-3d-go-vs-java-benchmark.md` (gitignored, local).

## Summary

- **Drain throughput: Java is not within 10% of Go, at any layout measured.**
  On one worker with 128 slots, Java drained at **0.60×** Go's rate cold
  (8,475 jobs/s, 8,303–8,800, against 14,153, 13,461–14,692) and **0.73×**
  warm (11,506, 9,879–11,710, against 15,831, 14,857–17,347)
  (`layout/decision.txt`). On the comparison's layout, the same 128 slots as
  eight workers × 16, it drained at **0.266×** cold (2,045 jobs/s,
  1,939–2,296, against 7,699, 7,240–8,508) and **0.355×** warm (2,872,
  2,161–3,416, against 8,088, 6,751–9,442) (`compare/decision.txt`).
- **Most of that layout's gap was the layout.** Eight JVMs on three CPUs each
  load and compile the same code, so Java's workers were CPU-bound. One JVM on
  the same CPUs drained 3.7× faster at 6.7× less CPU per job, and was not
  CPU-bound. The Go worker nearly doubled as well (7,356 to 14,153 jobs/s in
  the same loop), so the comparison's layout held back both SDKs, not only
  Java. See [Layout](#layout-the-same-slots-on-fewer-workers).
- **CPU per 1,000 jobs:** Java 1,491 ms (1,342–1,566), Go 119 (110–130), in a
  drain, counted since process start, so JVM start-up and JIT are in Java's
  figure: **12.5×**. In a steady run, after a 60 s warm-up: Java 915 ms
  (852–1,023), Go 249 (246–287): **3.7×** (`compare/decision.txt`).
- **Peak resident memory per worker:** Java 127.7 MiB (127.1–128.8), Go
  21.4 MiB (21.2–21.5), of a 384 MiB limit each: **6.0×**. Threads: 23
  against 9 (`compare/summary.md`, `compare/results.csv`).
- **Start to an accepted lease stream:** Java 4,119 ms (3,923–4,864), Go 174
  (136–210), with eight workers starting at once on three CPUs
  (`compare/decision.txt`).
- **10,000 handlers open at once on one worker:** Go 210 MiB (208–212), Java
  249 MiB (220–251) peak RSS, at the same throughput (627 against 609 jobs/s,
  sleep-bound). The Java recording holds **72 `jdk.VirtualThreadPinned`
  events**, every one a first use of a class, in two bursts, none while the
  handlers slept. The positive control saw 1 and 0, as it must
  (`wide/decision.txt`, `wide/peaks.txt`, `pinning/control.txt`).

## Setup

- **Hardware and layout.** One laptop: Apple M2, 8 cores, 8 GiB; Docker
  Desktop 29.1.3, a VM with 8 vCPUs and 3.83 GiB; Compose v2.40.3;
  `postgres:16-alpine` with `synchronous_commit` on. `PIN=1`:
  PostgreSQL on CPUs 0–1, brokers 2–3, workers 4–6, loadgen 7
  (`cpuset.yaml` in every run). One broker, 8 workers × 16 slots, 300-byte
  jobs, `sleep` handlers of 0 ms: the 1b loops' settings (D1).
- **The measured ref is `14e87ee`** (`3d/ref.txt`); every broker and worker
  image was built from it with `git archive`. The images, from each run's
  `workers.json`: Go `sha256:066f2a6f8e65…`, Java
  `sha256:ddd75cf76a4e…` (`hopper-bench/worker-java:ref-14e87ee`, JRE Temurin
  25.0.5+8). The harness was `8726d9d` for the comparison and `88a8271` for
  the 10,000-handler loop, both clean; neither touches the measured code.
- **The JVM** (`jvm-flags.txt` in each Java run): `UseSerialGC` from the
  command line, `MaxHeapSize` 301,989,888 bytes (288 MiB, 75% of 384 MiB by
  the image's `MaxRAMPercentage=75`), initial heap 8 MiB, and
  `ActiveProcessorCount` left at -1, so the JVM sizes itself from the
  container's cpuset, three CPUs. The 10,000-handler runs, at 1 GiB, have a
  768 MiB maximum heap (`wide/…T223415/jvm-flags.txt`).
- **One memory limit for both** (D3): every worker container gets
  `mem_limit` and `memswap_limit` 384m (`workers-mem.yaml`), so neither SDK
  can use memory the other can't, and the JVM's heap follows from the limit
  instead of from the whole VM. The collector is pinned so a larger limit
  can't switch it to G1.
- **Worker metrics off on both** (D2, `HOPPER_METRICS_LISTEN=`): the Java
  example worker serves none, and the Go worker would otherwise update
  Prometheus per job. Both log at warn and run the same handlers.
- **Every number is a five-run median** (range in brackets) from one
  interleaved loop, `compare/loop.sh`: drain and steady × Go and Java × 5,
  rotated each repetition, run 2026-11-25 05:47–06:25 UTC, on AC power,
  with an empty `sleeps.txt`, no rejected run and no rerun. The
  10,000-handler figures are three-run medians from `wide/loop.sh`. Verdicts
  are `scripts/bench_compare.py` with Go as control and Java as treatment
  (D14).
- **The drain's size came from the pilot** (`pilot/decision.txt`):
  `JOBS=140000`, with the warm window over the last 40,000
  (`WARM_JOBS=100000`). The pilot's rule asked for 200,000 for both SDKs, and
  its 100,000 cap applied. For Java that is real: it had not reached its pace
  by then. For Go it is an artifact. Go's ramp was about 5 s (~33,000 jobs);
  its late-drain slowdown, not a ramp, set its number. So Go's "warm" window
  is simply its last 40,000 jobs.

## Drain

140,000 preloaded jobs, `compare/summary.md` and `compare/decision.txt`:

| | Go | Java | Java / Go |
|---|---:|---:|---:|
| Cold, jobs/s | 7,699 (7,240–8,508) | 2,045 (1,939–2,296) | 0.266× |
| Warm (after job 100,000), jobs/s | 8,088 (6,751–9,442) | 2,872 (2,161–3,416) | 0.355× |
| Worker CPU per 1,000 jobs, ms (since start) | 119 (110–130) | 1,491 (1,342–1,566) | 12.5× |
| Peak RSS per worker, MiB (`VmHWM`) | 21.4 (21.2–21.5) | 127.7 (127.1–128.8) | 6.0× |
| OS threads per worker | 9 | 23 | |
| Start to an accepted lease stream, ms | 174 (136–210) | 4,119 (3,923–4,864) | 23.7× |
| Workers' CPU, % (3 CPUs = 300) | 86 | 315 | |
| Saturated | none | **workers** | |
| Rows leased at once, max (of 128) | 121 | 116 | |

**How fast each reached its pace** (`rate.psv` in each run, summed to
seconds). Go was past 8,000 jobs/s by its third or fourth second in every
run and then varied between about 5,000 and 11,600 until its last second;
its drains lasted 17–20 s. Java started at 23–95 jobs/s in its first second,
reached about 1,000 by second 8–13, about 2,000 by second 15–28, and from
about second 35 varied between roughly 700 and 5,500, with no clean
plateau, to the end of 61–73 s drains. Java spends its first half-minute
interpreting and compiling: the profile run's recording (below) holds 21.5 s
of JIT compilation in its first 25 s.

**What explains the gap.** Go's drain on this layout is not worker-bound: its
workers used 86% of their 300, PostgreSQL's backends spent 66.7% of their
time on locks, and nothing was saturated. That bound belongs to the layout,
not to one broker: on fewer, wider workers the same broker and database
drained nearly twice as fast ([Layout](#layout-the-same-slots-on-fewer-workers)). Java's workers were saturated instead, at 315%
of three CPUs. Its CPU per job accounts for that exactly: 1,491 ms per 1,000
jobs × 2,045 jobs/s is 3.05 CPU-seconds per second, the three CPUs. At Go's
119 ms per 1,000 the same drain needs under one CPU. So on this layout the
Java worker's CPU per job, not the broker, sets its drain rate; with more CPU
per worker the gap would narrow toward the broker's bound, which this loop
doesn't measure.

**Go's drain sits below the H4-credit range.** 7,699 jobs/s (7,240–8,508) is
under the 8,543–10,143 that the H4-credit loop measured on the same layout
([scaling.md](../scaling.md#h4-the-ownership-heartbeat-before-every-handler)),
which `prediction.txt` named as a sign of a broken measurement. It is taken as
a longer drain, not a broken one: those were 40,000-job drains, these are
140,000, and every Go drain ended with 280,000 dead tuples (Java's: 30,902 to
82,549, `compare/results.csv`), the late-drain slowdown the pilot's rate
curve showed. One Go drain's warm window (6,751) fell 10.2% below its own
cold rate (7,519) on the same cause; the medians have warm above cold.

## Steady

1,000 jobs/s for 60 s of warm-up under its own run id, then 30 s measured
(D8; `compare/decision.txt`, `compare/results.csv`):

| | Go | Java |
|---|---:|---:|
| Completed, jobs/s | 1,001 (1,000–1,004) | 1,001 (997–1,002) |
| `kept_up` | yes, 5 of 5 | yes, 5 of 5 |
| Pickup p99, ms | 10.0 (7.6–11.5) | 15.7 (13.6–17.3) |
| End-to-end p99, ms | 29.8 (26.0–35.0) | 59.0 (48.9–75.8) |
| Max producer lag, ms | 170 (106–285) | 392 (275–842) |
| Worker CPU per 1,000 jobs, ms (warm) | 249 (246–287) | 915 (852–1,023) |
| Peak RSS per worker, MiB | 21.2 (21.1–21.3) | 128.0 (126.9–129.2) |
| Start to an accepted lease stream, ms | 107 (86–158) | 2,634 (2,143–2,859) |

Both kept up. Java's p99s are 1.6× (pickup) and 2.0× (end to end) Go's. Warm,
Java's CPU per job is 3.7× Go's, against 12.5× in the cold drain: most of the
drain's difference is start-up and compilation. Go's per-job CPU is higher
here than in the drain (249 against 119 ms), most likely because at
1,000 jobs/s its fixed costs (the runtime, the lease stream, its timers) are
spread over fewer jobs; this loop doesn't profile it. These are
30 s runs; longer steady runs at one broker have not kept up
([scaling.md](../scaling.md)), and this doesn't change that.

## 10,000 handlers at once

**The shape** (D11, `wide/loop.sh`): one broker, one worker at concurrency
10,000 with a 1 GiB limit, 10,000 preloaded `sleep` jobs of 15 s. The worker
asks for a 60 s lease, so renewal (G/3) is at 20 s and no handler renews: the
run measures 10,000 open handlers, not 10,000 renewals against one broker.
The freshness window (G/10 = 6 s) covers the ramp. Three runs per SDK at
warn, then one proof run each at info.

**How 10,000 is shown:**
- rows: every run's `max_leased` is 10,000 (`wide/decision.txt`);
- handlers: in both proof runs' info logs one process held 10,000 deliveries
  open at once, of concurrency 10,000; `TestPeaks` passed
  (`wide/peaks.txt`);
- goroutines: Go's goroutine profile in Part E counts 30,016, three per
  handler (the handler, its heartbeat monitor and its renewal loop) plus 16
  (`profile/…T224634/worker-goroutines.txt`, `worker-goroutine.txt`).

| | Go | Java |
|---|---:|---:|
| Peak RSS, MiB | 210.1 (208.0–212.0) | 249.4 (219.9–250.8) |
| OS threads | 9 | 25 |
| Worker CPU per 1,000 jobs, ms | 144 (117–145) | 737 (586–750) |
| Start to an accepted lease stream, ms | 226 (197–264) | 855 (735–1,230) |
| Throughput, jobs/s | 627 (627–638) | 609 (602–620) |

Throughput has no measurable difference (0.971×): 10,000 jobs of 15 s each
are bound by the sleep. Java uses 1.19× Go's memory here, against 6.0× at 16
handlers. The gap shrinks from 106 MiB to 39 MiB because Java's cost is
mostly fixed (the JVM, its code and metaspace), while Go's grows with the
handlers: from the drain's 16 handlers per worker to 10,000, Go's peak rose
by about 19 KiB per handler (mostly three goroutine stacks) and Java's by
about 12 KiB. The limits differ (384 MiB against 1 GiB), so the JVM's heap
sizing does too; treat the per-handler figures as rough.

**From the Java proof run's recording** (`wide/…T223415/worker-1.jfr-*.txt`,
35 s from JVM start, with native memory tracking):
- Collector Serial, heap 16 MB initial and 768 MB maximum
  (`heap-configuration`).
- Heap after GC at its highest on the plateau, while the handlers slept:
  73.1 MB (GC 48, after which none ran until the completions began); 80.7 MB
  at its highest overall, during the completions (`gc`).
- 51 GC pauses, 312 ms in all, median 3.47 ms; the largest 107 ms, the one
  old collection, as the completions began (`gc-pauses`).
- Native memory committed at its maximum (`native-memory-committed`): Java
  heap 171.8 MB, Tracing (JFR itself) 21.3, Code 20.0, Metaspace 19.0, Shared
  class space 12.7, Arena 12.6, Other 12.2, Symbol 4.1, Class 2.9, NMT 2.2,
  Thread Stack 0.4. Thread stacks are tiny because a virtual thread's stack
  lives in the heap, as stack chunks.
- Java threads peaked at 21 (`thread-count`); the process had 29 OS threads
  in this run and 25 in every warn run (`/proc`, `wide/results.csv`).

**From Go's MemStats** (Part E, `profile/…T224634/worker-memstats.txt`, at
`d9370e1` with 10,000 handlers open): `Sys` 203.5 MiB, of which goroutine
stacks are the largest item at 121.4 MiB in use; heap in use 67.6 MiB
(`HeapAlloc` 63.9 MiB); 6 GCs. That is about 4 KiB of stack per goroutine,
12 KiB per handler with its three goroutines.

## Pinning

**72 `jdk.VirtualThreadPinned` events** in the Java proof run, recorded at a
0 ms threshold (`wide/peaks.txt`, `worker-1.jfr-summary.txt`). The improvement
plan expected none.

**The positive control** (`pinning/control.txt`, `pinning/control.sh`, run by
the wide loop in the measured image `sha256:ddd75cf7…`): a virtual thread
that parks 5 ms inside a class initializer gave **1 event at 0 ms and 0 at
JFR's default 20 ms**, PASS. The 1 shows the counter counts in this image; the
0 shows the default threshold would have hidden a short pin, so the 0 ms
setting took effect and a short pin in the worker would have been counted.

**What pinned** (`wide/…T223415/worker-1.jfr-pinned.txt`,
`worker-1.jfr-pinned-threads.txt`). Every event is a first use of a class:
- 58 "Contended monitor enter": 56 "Freeze or preempt failed (2)" in
  `BuiltinClassLoader.loadClass` (for example from `Execution.run` line 46),
  and 2 "Native or VM frame on stack" in `ReferencedKeyMap.intern`;
- 14 `Object.wait`, each "Waited for initialization of <class> by another
  thread": Jackson's `JsonEncoding` and `UTF8StreamJsonParser`,
  `AckRequest`, `HeartbeatRequest`, `Confirmer$Outcome`, `HandlerStatus`,
  grpc's `ClientCalls$GrpcFuture`.

They come in two bursts: 24 at 06:34:39 UTC, the first deliveries, about 1 s
after the JVM started; and 48 at 06:34:54, the first completions 15 s later,
when the ack path's classes load for the first time. None while the 10,000
handlers slept. The longest was 38.0 ms (twice, Jackson's
`ByteSourceJsonBootstrapper.constructParser`); the rest were under 8 ms, so
at JFR's default threshold the recording would have shown 2.

So the virtual threads don't pin while they work or sleep: JEP 491 removed the
`synchronized` pin, and the SDK holds no lock across a blocking call. What
pins is class loading and class initialization, which a carrier can't
unmount from, when many virtual threads reach a class first at once. In the
profile run (Part E, eight JVMs on three CPUs) the same causes pinned 40 times
in 25 s, and the longest was 149 ms
(`profile/…T224329/worker-1.jfr-pinned-threads.txt`): contention makes the
same pins longer. **Follow-up, not done here:** load and initialize those
classes before the first delivery (or an AOT cache, D18), so the first
bursts don't pin carriers.

## Where the CPU goes

Part E ran, at **`d9370e1`, not the measured `14e87ee`** (D16): it needed
`cmd/worker -debug-listen`, which those commits add. Its runs are diagnostics,
not results: profiling costs CPU, and its drains were 200,000 jobs (Go 5,162
jobs/s, Java 1,710, `profile/results.csv`).

**Go** (`profile/…T224141/worker-cpu-top.txt`, 10 s of one worker's CPU
during a drain, 1.08 s of samples): system calls 17.6% flat
(`Syscall6`, the socket reads and writes) and `futex` 7.4%; grpc's stream
receive path (`clientStream.withRetry` 13.0% cumulative, `readFrame` 13.0%);
protobuf unmarshalling; and **`log/slog` at 10.2% cumulative**
(`appendJSONMarshal`). `go tool pprof -peek` on the local profile puts that
under `slog.Logger.With` (11.1%), called from `worker.(*Worker).execute`
(`worker/execute.go:27-28`): `With` formats its attributes eagerly, so the
per-job logger costs about a tenth of the Go worker's CPU even at warn, where
it writes nothing. **A finding, not fixed here:** a lazily formatted logger,
or a level check before `With`, would remove it. In the 10,000-handler run the
Go worker was idle: 10 ms of samples in 5 s, all in the timer code
(`…T224634/worker-cpu-top.txt`).

**Java** (`profile/…T224329/worker-1.jfr-*.txt`, one of eight workers,
`JFR=25`): the recording covers JVM start plus about 21 s of a 117 s drain,
so it shows the early, JIT-heavy part, not a steady state.
`compiler-statistics` reports 5,046 methods compiled and **21.5 s of
compilation time** in that window, and `gc-cpu-time` 0.36 s of GC CPU over
32 collections. `hot-methods` is flat: no method has more than 1% of the
samples, and the top ones are grpc-java and Netty stream setup
(`NettyClientTransport.newStream`, `RetriableStream.<init>`,
`CallOptions.withStreamTracerFactory`), class loading
(`Resource.getBytes`, `ZipFile`), and protobuf writing. So early on, Java's
CPU goes to compiling and loading its own code and to per-call grpc-java
machinery (each Ack and Heartbeat is a unary call with its own stream),
where Go's goes to the socket.

What this says about the gap: the cold drain's 12.5× CPU per job is mostly
start-up and compilation (3.7× warm, steady), and what remains warm is
grpc-java's per-call cost against Go's. Neither side has one hotspot to fix;
Go has one cheap win (the logger).

## Against the prediction

`prediction.txt` was written before any pilot or loop run.

| Predicted | Measured | |
|---|---|---|
| Java/Go cold drain 0.25 | 0.266 | met |
| Java/Go warm drain 0.40 | 0.355 | close: Java's warm pace never settled (rate.psv) |
| CPU per 1k, cold: Go 130, Java 1,800 ms | 119, 1,491 | met for Go; Java 17% under |
| Peak RSS: Go 22, Java 140 MiB | 21.4, 127.7 | met |
| Threads: Go 9, Java 26 | 9, 23 | met for Go; Java 3 fewer |
| Startup: Go 120, Java 1,500 ms | 174, 4,119 | **missed** for Java: eight JVMs starting at once on three CPUs took 2.7× the guess (conformance's single restart under load took 0.49 s, [conformance.md](../design/conformance.md#results-2026-11-24)) |
| Saturation: Java's workers yes, Go's no | yes, none | met |
| Steady pickup p99: Go 6, Java 12 ms | 10.0, 15.7 | both higher |
| Steady end-to-end p99: Go 20, Java 60 ms | 29.8, 59.0 | Go higher; Java met |
| `kept_up` at 1,000/s, both | yes, both | met |
| Steady CPU per 1k, warm: Go 200, Java 1,200 ms | 249, 915 | Go higher; Java lower |
| 10,000 handlers RSS: Go 110, Java 300 MiB | 210, 249 | **missed** for Go: the stacks of three goroutines per handler are 121 MiB (MemStats); Java under |
| 10,000 handlers threads: Go 12, Java 30 | 9, 25 | both fewer |
| ~50 pins, all in the first second | 72, in two bursts 15 s apart | **missed**: the ack path's classes load at the first completions |

The prediction's own tests of a broken measurement:
- Go's drain outside 8,543–10,143: **tripped** (7,699). Explained above by
  the drain's length and its dead tuples, not taken as a broken measurement.
- `max_leased` above 128 in a drain or 10,000 in a wide run: no (126 and
  10,000 at most).
- Go's warm below its cold by more than 10%: **in one run of five** (6,751
  against 7,519, 10.2%), the same cause; not in the medians. Java's warm CPU
  above its cold: no (915 against 1,491).
- A Go worker above 50 MiB in a drain, or a Java `jvm-flags.txt` without
  `UseSerialGC` from the command line and a 288 MiB heap: no.
- Any rejected run: none, in any loop.
- Zero pins while the control fails: neither.

## Layout: the same slots on fewer workers

A follow-up loop, run after this write-up (`layout/`, 2026-11-25, at the
measured `14e87ee`): the same 128 slots on the same three worker CPUs, split
8 × 16 (the comparison's layout, rerun as the in-loop baseline), 2 × 64 and
1 × 128, for both SDKs, three drains each, rotated. 140,000 jobs, the pilot's
`WARM_JOBS`. Worker memory is 384 MiB at 8 × 16, as in `compare/`, and 1 GiB
at 2 × 64 and 1 × 128, as in `wide/`; both SDKs get the same limit in each
layout, so each row's ratio is like for like
(`layout/decision.txt`, `layout/summary.md`):

| Layout | Go, cold jobs/s | Java, cold jobs/s | Java/Go cold | Java/Go warm | Java CPU per 1,000 jobs, ms | Java workers' CPU |
|---|---:|---:|---:|---:|---:|---:|
| 8 × 16 | 7,356 (6,472–7,474) | 2,262 (1,795–2,573) | 0.31× | 0.41× | 1,378 | 324% of 300: saturated |
| 2 × 64 | 12,946 (12,945–13,014) | 7,817 (7,552–7,983) | 0.60× | 0.75× | 308 | 210% |
| 1 × 128 | 14,153 (13,461–14,692) | 8,475 (8,303–8,800) | 0.60× | 0.73× | 205 | 158% |

The baseline row reproduces `compare/` (0.31× against 0.266×, three runs
against five).

- **Java.** One JVM drains 3.7× faster than eight on the same CPUs, at 6.7×
  less CPU per job (205 ms per 1,000 against 1,378): it loads and compiles its
  code once, not eight times. Its start to an accepted stream falls from
  4,069 ms to 807, and its memory is 161 MiB in all, against eight × 128.
  What remains is a real per-job cost: 3.5× Go's CPU per job at 1 × 128
  (205 against 58 ms per 1,000).
- **Go.** The Go worker nearly doubles too, 7,356 to 14,153 jobs/s, at half
  the CPU per job (127 to 58 ms per 1,000). With it the database's work per
  job falls: commits per job 0.31 to 0.11, lock waits 65.4% of backend time
  to 26.7%, PostgreSQL CPU 129% to 88% (`layout/summary.md`).
  **A hypothesis, not tested:** a stream's lease calls are bounded by its
  free credit, so 16-slot streams lease and confirm in smaller batches than
  the broker's 32-row limit allows, and the same jobs cost more round trips.
  The 1b loops all measured one broker at 8 × 16.
- **Not identified:** what bounds a drain at 1 × 128. Nothing is marked
  saturated in those rows.

## What this does not show

- **One laptop.** macOS doesn't tie VM CPUs to physical cores, and the M2
  mixes performance and efficiency cores, so pinning removes sharing inside
  the VM, not all variation.
- **Three repetitions in the layout loop**, against five in the comparison.
- **One collector and one heap size:** Serial at 288 MiB (768 MiB for 10,000
  handlers). No G1, Parallel or ZGC, no GC tuning.
- **No start-up work:** no AOT cache (JEP 483/514/515), CRaC or native image
  (D18). Java's 4.1 s start and its pins at first use are what those address.
- **One broker, against one PostgreSQL.** Other broker counts, and the
  Kubernetes proofs with Java workers, are not measured.
- **Sleep and zero-work handlers.** Real handlers that do work would shrink
  the SDKs' share of each job's CPU.
- **Short steady runs** (30 s after a 60 s warm-up), at one rate.

Follow-ups (D18): a G1 or ZGC variant, an AOT cache for start-up, GC tuning,
the k8s proofs with Java workers; from this run, preloading the classes that
pin, and the Go worker's eager `slog.With`.

## Reproduce

Each loop needs the conditions `docs/bench/3d/common.sh` `preflight` checks:
the tree matching HEAD outside `docs/`, no other container on Docker (the kind
node `hopper-control-plane` stopped), a VM with 8 CPUs, and AC power. Each
records `pmset`'s sleep and wake lines in its `sleeps.txt`; any line voids it.

```bash
bash docs/bench/3d/compare/loop.sh       # drain and steady, Go and Java, x5: about 45 minutes
bash docs/bench/3d/wide/loop.sh          # 10,000 handlers, with the JFR proof: about 15
bash docs/bench/3d/layout/loop.sh        # 128 slots as 8x16, 2x64 and 1x128, both SDKs, x3: about 25
bash docs/bench/3d/pinning/control.sh hopper-bench/worker-java:ref-14e87ee
bash docs/bench/3d/profile/loop.sh       # profiles, from HEAD: about 10
```

The loops read the measured ref from `docs/bench/3d/ref.txt` (the profile
loop uses HEAD) and the pilot's settings from `docs/bench/3d/pilot/settings.env`.
A single run: `PIN=1 WORKERS=8 WORKER=java WORKER_MEM=384m
WORKER_ENV="HOPPER_METRICS_LISTEN=" VARIANT=java PAYLOAD_BYTES=300
caffeinate -dimsu scripts/bench.sh drain`.
