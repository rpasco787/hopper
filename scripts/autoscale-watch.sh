#!/usr/bin/env bash
# Prints the worker autoscaler at work, one line per interval, until one scale
# cycle is over: the backlog that the ScaledObject's own query reads, and the
# worker pods running and terminating against the HPA's desired count, each
# with a bar. docs/img/session9-autoscale.gif is a recording of it. Needs
# autoscaling on (scripts/kind-addons.sh, then
# scripts/kind-up.sh -f deploy/kind/autoscaling.yaml):
#
#   scripts/autoscale-watch.sh          # then enqueue from another terminal
#
# It exits once it has seen every allowed worker running and then the minimum
# again with no backlog, or after WATCH_LIMIT_SECONDS (default 1200). A burst
# too small to demand the maximum runs until that limit. WATCH_INTERVAL
# (seconds, default 2) sets the pace, and WATCH_DEPTH_SCALE (default 20000)
# the backlog that fills its bar. Every kubectl call names the kind-hopper
# context.
set -euo pipefail

context=kind-hopper
ns="${HOPPER_NAMESPACE:-hopper}"
release="${HOPPER_RELEASE:-hopper}"
interval="${WATCH_INTERVAL:-2}"
limit="${WATCH_LIMIT_SECONDS:-1200}"
depth_scale="${WATCH_DEPTH_SCALE:-20000}"
width=40

k() { kubectl --context "$context" -n "$ns" "$@"; }

so="$(k get scaledobject "$release-worker" -o json)"
query="$(jq -r '.spec.triggers[0].metadata.query' <<<"$so")"
server="$(jq -r '.spec.triggers[0].metadata.serverAddress' <<<"$so")"
per="$(jq -r '.spec.triggers[0].metadata.threshold' <<<"$so")"
min="$(jq -r '.spec.minReplicaCount' <<<"$so")"
max="$(jq -r '.spec.maxReplicaCount' <<<"$so")"
# serverAddress is http://<service>.<namespace>.svc:<port>; the API server's
# service proxy reaches it without a port-forward.
hostport="${server#*://}"
host="${hostport%%:*}"
port="${hostport##*:}"
svc="${host%%.*}"
rest="${host#*.}"
path="/api/v1/namespaces/${rest%%.*}/services/$svc:$port/proxy/api/v1/query?query=$(jq -rn --arg q "$query" '$q|@uri')"
selector="app.kubernetes.io/instance=$release,app.kubernetes.io/component=worker"

# bar <count> <glyph>: the glyph, count times. printf pads bytes, not
# characters, so callers pad with spaces themselves.
bar() {
  local s="" i
  for ((i = 0; i < $1; i++)); do s+="$2"; done
  printf '%s' "$s"
}

printf 'hopper workers on KEDA: %s to %s, one per %s available jobs\n\n' "$min" "$max" "$per"
printf '  time  backlog  %-*s  workers\n' "$width" ""
start=$SECONDS
peaked=0
quiet=0
while ((SECONDS - start < limit)); do
  # jq, not bash, reads the number: bash arithmetic takes integers only.
  depth="$(kubectl --context "$context" get --raw "$path" | jq -r '.data.result[0].value[1] // empty | tonumber | floor')"
  desired="$(k get hpa "keda-hpa-$release-worker" -o jsonpath='{.status.desiredReplicas}')"
  read -r running terminating < <(k get pods -l "$selector" -o json | jq -r '
    "\([.items[] | select(.metadata.deletionTimestamp == null and .status.phase == "Running")] | length) \([.items[] | select(.metadata.deletionTimestamp != null)] | length)"')

  if [[ -z "$depth" ]]; then
    backlog="unknown"
    n=0
  else
    backlog="$depth"
    n=$((backlog * width / depth_scale))
    if ((n > width)); then n=$width; fi
    if ((backlog > 0 && n == 0)); then n=1; fi
  fi
  t=$((SECONDS - start))
  printf '%3d:%02d  %7s  \033[33m%s%s\033[0m  %2d/%-2d \033[32m%s\033[31m%s\033[0m\n' \
    $((t / 60)) $((t % 60)) "$backlog" "$(bar "$n" '█')" "$(bar $((width - n)) ' ')" \
    "$running" "$desired" "$(bar "$running" '█')" "$(bar "$terminating" '░')"

  if ((running == max)); then peaked=1; fi
  if ((peaked)) && [[ "$backlog" == 0 ]] && ((running == min && terminating == 0 && desired == min)); then
    quiet=$((quiet + 1))
    if ((quiet >= 3)); then
      echo "scaled to $max and back to $min worker(s) with no backlog"
      exit 0
    fi
  else
    quiet=0
  fi
  sleep "$interval"
done
echo "stopped after ${limit}s without finishing a scale cycle" >&2
exit 1

