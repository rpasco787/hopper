Phase 3c conformance suite, evidence of 2026-11-24 (branch phase-3c-conformance).

Passes: scripts/ci.sh conformance, 15:03:49-15:14:50 (11 min 1 s), exit 0, run at
89b4d21 with scripts/ci.sh and scripts/conformance.sh then uncommitted (committed
as d485947). Go pass ok 301.895 s; Java pass ok 353.598 s (the mixed fleet runs in
it). The lease was 30 s (the Compose default).
Java worker image: hopper-example-worker:dev, id sha256:61a44b130a45a811b3aec2b9a80f99a9834a4ac89080439a47ce38cce0dbf39f,
built from java/ by scripts/conformance.sh. Go worker: built from the tree. Laptop: AC power,
Docker Desktop VM of 3.83 GiB, no sleep or wake in the window, 0 containers and
0 volumes left, the kind node stopped.

Layout: go/ and java/, then <stack>/<scenario>/ with expect.json (what was
expected), rows.csv (the job rows), judgement.txt (the verdict), exits.json (exit
codes) and each worker's .stderr. Raw worker .stdout (20 MB in all, over the 5 MB
this directory is allowed) is left out. The live judges read it, and the offline
check (HOPPER_EVIDENCE=<a scenario's directory> go test -tags evidence
./test/accounting) needs it, so these directories are a record and cannot be
re-judged from here.

summary.txt            both passes' lines, times and judge summaries
measure.txt            Task 10: Java memory, startup under load, D22 (2026-11-24, 14:20-14:21, image d8cf79ddb670 at e0fb10b)
negative-controls.txt  25 damaged runs, 25 failures; every undamaged baseline clean
chaos-java-5.txt       the chaos suite five times against the Java worker: 4 of 5; pass 2's failure and its cause

The five chaos passes ran against image 61a44b130a45 (before the fix in a595053). Passes 6 (void, the laptop slept) and 7 (clean) ran against the rebuilt image bf9f80d41c0a; see chaos-java-5.txt.
