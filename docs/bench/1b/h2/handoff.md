# Phase 1b-2 handoff

Date: 2026-11-17. Current code/harness head: `3aaf0c1` on `main`.
Task 11's verification was explicitly skipped by the user. All correctness
claims below reuse completed Task 8/9 evidence; no tests, formatting checks,
lint, focused store repetitions or 20-repetition batcher run were performed
at close-out. Nothing was merged or pushed.

## H2 result

The [full comparison](comparison.md) has one table per broker count for
`nobatch`, `batch`, `batch-flights1` and `async`: jobs/s medians and ranges,
commits/job, WAL syncs/job, confirmation statements/job, WALWrite+WALSync
share, active backends, Ack/Heartbeat p99, submitted batch size and exact
fallback share per kind. p99s interpolate within the listed fixed buckets.
All 60 runs use clean code/harness `3aaf0c1`, five per configuration in the
same interleaved loop. No rejected, failed or replacement runs; zero retried
jobs. The 5,000-job counter smoke check is diagnostic, outside the 60-run
comparison.

## Decision and approval status

The [written rule output](decision.txt) is:

```text
batch qualifies: True
batch-flights1 qualifies: False
```

With two flights, throughput improves measurably at one broker: 6,446
(5,304–8,170) against 3,369 (2,039–4,725), 1.913× with separated ranges.
Two-broker medians are 1.364× with overlapping ranges, and four-broker
medians are 0.988×. Worst median p99 ratio is 1.431×; worst exact median
Heartbeat fallback share is 9.0675%, below the 10% gate. One flight fails
because four-broker throughput is 0.922×, below the 0.95 floor.

Recommendation: batch size 64, two flights. **No approval to change the
default was given, so batching remains off (`confirm-batch=1`, flights=2).**
The [proposed default patch](proposed-default.patch) remains unapplied.
Task 11's skip-verification instruction is not an approval to enable batching.
The documentation commit also remains pending the explicit approval required
by the plan. Approval to enable later should resolve whether Task 10 Step 4's
post-default checks are wanted; this close-out ran none.

## Async ceiling on the fixed build

Async/control median ratios are 1.57×, 1.82× and 0.96× at 1, 2 and 4 brokers,
but ranges overlap at every count. The written rule establishes no measurable
throughput gain from removing synchronous-commit waits in this loop. Higher
one/two-broker medians are not proof of no benefit; the evidence is inconclusive
about a flush-only throughput ceiling. Async is diagnostic and gives up the
synchronous durability guarantee; it is not recommended for production.

## Revisions needed before plan 1b-3

Tasks 18–26 of `docs/plans/2026-11-12-phase-1b-throughput.md` need these
updates before execution:

- Use this plan's code head `3aaf0c1` as the starting ref, superseded by a
  later default-enablement commit only if the user approves one. Measurement
  refs after each new implementation still name that implementation's head.
  Preserve the H2 same-build control; do not attribute the H3a fix to H2.
- Defaults are still off. Every experiment that assumes batching on needs
  `BROKER_ENV="HOPPER_CONFIRM_BATCH=64"` on both control and treatment, with
  two flights. In Task 23, combine that setting with `HOPPER_WAKEUPS=false`
  for the off variant; do not overwrite it with the wakeups setting.
- Task 24's chaos invocation needs `HOPPER_CHAOS_CONFIRM_BATCH=64` to prove
  batching. Its integration runs need `HOPPER_TEST_CONFIRM_BATCH=64`.
  Even after a production-default change, the chaos overlay still explicitly
  supplies `1` unless its test switch is supplied or its default is updated.
- Task 25 should drop its planned §5 and §11.3 additions: those paragraphs
  are now in `docs/semantics.md`. Update the off/default wording there only
  if enabling batching is approved. Its §12 work remains for plan 1b-3.
- Task 24 should retain the completed §15 rows, use the current store test
  names, and add new evidence rather than duplicate the rows. Do not claim
  the skipped close-out `-count=20` batcher run happened.
- Task 21 must revise its claim that H2 removes the flush ceiling. With two
  flights, flush waits still account for 82.3%, 79.3%, 71.9% of active-backend
  samples at 1, 2, 4 brokers; active backends decline to 6.00, 8.44, 15.38.
- The Task 20/23 load rates derived from 60% of H2's two-flight medians,
  rounded to 500, are 4,000 jobs/s at one broker and 2,500 at four. Keep
  batching enabled when using those capacity references, or remeasure a
  matching reference if the configuration changes.
- H4 remains worth measuring: at four brokers, Heartbeat fallbacks take
  35.5% of two-flight statement execution time, while Heartbeat batches take
  17.3%; the async reference spends 50.6% on one-row Heartbeats. See the
  [statement diagnostics](statements-summary.md). H2 helps at one broker;
  it did not make four-broker throughput measurably faster.

## Problems, skips and review notes

- Initial Task 6/7 sandboxed tests could not bind loopback sockets; reruns
  with local-network access passed. No correctness tests were weakened.
- The integration plan overlooked the restart broker constructor. Both
  constructors now apply the suite switch; a real regression test failed
  with zero batch calls before the fix and passed afterward.
- The 10% fallback gate uses exact retained statement/RPC counts rather than
  one-decimal CSV percentages, avoiding a rounded borderline acceptance.
- Deferred minor: explicit `HOPPER_TEST_CONFIRM_BATCH=1` is treated as on by
  the counter-assertion helper; prescribed unset/64 runs are unaffected.
- Deferred minor: the harness lacks a positive Heartbeat unit fixture;
  the real smoke run and all 60 measurement runs exercised those counters.
- Task 11's entire verification work was skipped by explicit user request,
  including additional focused store checks and 20 batcher repetitions.
- Code is committed through `3aaf0c1`. Documentation and evidence are left
  uncommitted for review, including the pre-existing baseline/H3a material.
