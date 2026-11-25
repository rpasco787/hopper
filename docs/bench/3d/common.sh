# Shared by the Phase 3d loops (docs/bench/3d/*/): the measured ref, the laptop
# checks, the sleep check for a loop's window, and row lookups. Source it from
# the repository root.
REF=$(cat docs/bench/3d/ref.txt)
JFR_TOOL=${JFR_TOOL:-$(/usr/libexec/java_home -v 25)/bin/jfr}
preflight() {
  # .gitignore is excluded too: it carries the user's own unstaged line, and no
  # build reads it (every image builds from REF through git archive).
  git diff --quiet HEAD -- . ':(exclude)docs' ':(exclude).gitignore' ||
    { echo "the tree differs from HEAD outside docs/: measure a commit" >&2; exit 2; }
  if [ "$(docker inspect -f '{{.State.Running}}' hopper-control-plane 2>/dev/null)" = true ]; then
    echo "hopper-control-plane is running; stop it first" >&2; exit 2
  fi
  [ -z "$(docker ps -q)" ] || { echo "other containers are running; stop them first" >&2; docker ps >&2; exit 2; }
  [ "$(docker info --format '{{.NCPU}}')" = 8 ] || { echo "the Docker VM needs 8 CPUs" >&2; exit 2; }
  pmset -g batt | grep -q "'AC Power'" ||
    { echo "plug the laptop in: a loop on battery can hibernate mid-run" >&2; exit 2; }
}
now() { date '+%Y-%m-%d %H:%M:%S'; }
# pmset's sleep and wake lines between two now() stamps. Any line voids the loop.
sleeps_between() {
  pmset -g log | awk -v s="$1" -v f="$2" '$4 ~ /^(Sleep|Wake|DarkWake)$/ { t = $1 " " $2; if (t >= s && t <= f) print }'
}
# The evidence directories of a results.csv's rows with this variant, in run order.
evidence_of() {
  python3 -c 'import csv, sys
for r in csv.DictReader(open(sys.argv[1])):
    if r["variant"] == sys.argv[2]:
        print(r["evidence"])' "$1" "$2"
}
