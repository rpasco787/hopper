#!/usr/bin/env bash
# Phase 3d: the pinning count's positive control, run in the Java worker image
# the benchmarks used (docs/bench/java.md). PinControl parks 5 ms inside a class
# initializer, which pins. With the benchmarks' JFR setting the recording must
# hold at least one jdk.VirtualThreadPinned event; at JFR's default 20 ms
# threshold, none. If either fails, a zero from the worker runs proves nothing.
#   bash docs/bench/3d/pinning/control.sh hopper-bench/worker-java:ref-<sha>
set -euo pipefail
cd "$(dirname "$0")"
image="${1:?usage: control.sh <java worker image>}"
jdk="${JDK_HOME:-$(/usr/libexec/java_home -v 25)}"
work="$(mktemp -d "$PWD/.control.XXXXXX")" # under /Users, which Docker Desktop shares
trap 'rm -rf "$work"' EXIT
"$jdk/bin/javac" --release 25 -d "$work" PinControl.java
chmod -R a+rX "$work" # the image runs as uid 65532
count() { # the JFR settings before filename=, empty for the defaults
  local cid
  cid="$(docker create -v "$work:/w:ro" --entrypoint java "$image" \
    "-XX:StartFlightRecording=${1}filename=/tmp/control.jfr" -Xlog:jfr+startup=off -cp /w PinControl)"
  docker start -a "$cid" >/dev/null
  docker cp "$cid:/tmp/control.jfr" "$work/control.jfr" >/dev/null
  docker rm "$cid" >/dev/null
  "$jdk/bin/jfr" summary "$work/control.jfr" | awk '$1 == "jdk.VirtualThreadPinned" { n = $2 } END { print n + 0 }'
}
bench="$(count 'jdk.VirtualThreadPinned#threshold=0ms,')"
default="$(count '')"
{
  echo "image: $image ($(docker image inspect --format '{{.Id}}' "$image"))"
  echo "with the benchmarks' threshold (0 ms): $bench pinned events (want at least 1)"
  echo "with JFR's default threshold (20 ms): $default pinned events (want 0)"
} | tee control.txt
if [ "$bench" -ge 1 ] && [ "$default" -eq 0 ]; then
  echo "control.sh: PASS" | tee -a control.txt
else
  echo "control.sh: FAIL" | tee -a control.txt
  exit 1
fi
