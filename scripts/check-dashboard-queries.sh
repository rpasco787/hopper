#!/usr/bin/env bash
# Runs every query in the Grafana dashboard against a live Prometheus and fails
# if any is rejected (a parse or type error) or returns no series. Run it with
# the Compose stack up and after it has served a few jobs:
#
#   docker compose -p hopper-session7 up -d --build --scale worker=3
#   scripts/check-dashboard-queries.sh
#
# $__rate_interval is a Grafana variable; it is replaced with 20s, four scrape
# intervals, which is what Grafana would pick for a 5s scrape.
set -euo pipefail

prom="${PROM_URL:-http://127.0.0.1:9095}"
dashboard="$(dirname "$0")/../deploy/grafana/dashboards/queue.json"

failed=0
count=0
while IFS= read -r expr; do
  count=$((count + 1))
  query="${expr//\$__rate_interval/20s}"
  resp="$(curl -sS --fail-with-body -G "$prom/api/v1/query" --data-urlencode "query=$query")" || {
    echo "FAIL (http): $expr"
    echo "  $resp"
    failed=1
    continue
  }
  status="$(jq -r .status <<<"$resp")"
  series="$(jq -r '.data.result | length' <<<"$resp")"
  if [[ "$status" != success ]]; then
    echo "FAIL ($status): $expr"
    echo "  $(jq -r .error <<<"$resp")"
    failed=1
  elif [[ "$series" -eq 0 ]]; then
    echo "FAIL (no series): $expr"
    failed=1
  else
    echo "ok ($series series): $expr"
  fi
done < <(jq -r '.panels[].targets[].expr' "$dashboard")

if [[ "$count" -eq 0 ]]; then
  echo "no queries found in $dashboard" >&2
  exit 1
fi
exit "$failed"
