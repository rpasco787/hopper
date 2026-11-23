#!/usr/bin/env bash
# Installs the CloudNativePG operator on the kind cluster "hopper", creating
# the cluster if it does not exist yet, for the chart's replicated database:
#
#   scripts/kind-cnpg.sh
#   scripts/kind-up.sh -f deploy/kind/replicated.yaml --timeout 10m
#
# Chart 0.29.2 (operator 1.30.2), in namespace cnpg-system. Pinned, and not
# added as a Helm repository, so this leaves your Helm configuration alone.
# Every kubectl and helm call names the kind-hopper context.
#
# Checked against the operator's 1.30 docs, which differ from what the Phase 2
# plan expected in three ways:
#   - They live at https://cloudnative-pg.io/docs/1.30/, not under
#     /documentation/<version>/, which now returns 404.
#   - The chart pins its own image, ghcr.io/cloudnative-pg/postgresql:
#     16.16-standard-bookworm (postgres.cloudnativepg.image). The operator's
#     default is now PostgreSQL 18 (18.7-system-trixie); 16 is still supported.
#   - spec.postgresql.synchronous.failoverQuorum, an opt-in guard against
#     losing the primary and its synchronous standby together, is left off:
#     the proofs crash the primary alone. Failover still promotes the standby
#     that has received the most WAL (pkg/postgres/status.go, v1.30.2).
set -euo pipefail
cd "$(dirname "$0")/.."

cluster=hopper
context=kind-hopper
version=0.29.2

if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --config deploy/kind/cluster.yaml
fi

helm upgrade --install cnpg cloudnative-pg --repo https://cloudnative-pg.github.io/charts \
  --version "$version" --kube-context "$context" -n cnpg-system --create-namespace \
  --wait --timeout 5m
# The chart's Cluster needs the CRD, and --wait has seen the operator, which
# serves its webhook, ready.
kubectl --context "$context" get crd clusters.postgresql.cnpg.io >/dev/null

echo "CloudNativePG (chart $version) is ready on $context"
