#!/usr/bin/env bash
# Installs the cluster add-ons that worker autoscaling needs on the kind
# cluster "hopper", creating the cluster if it does not exist yet:
#
#   kube-prometheus-stack 91.7.0 in namespace monitoring: the Prometheus
#   Operator, one Prometheus, kube-state-metrics and Grafana, with the hopper
#   dashboard from deploy/grafana/dashboards/queue.json;
#   KEDA 2.20.2 in namespace keda.
#
# Then install hopper with autoscaling on:
#
#   scripts/kind-up.sh -f deploy/kind/autoscaling.yaml
#
# Both charts are pinned, and neither is added as a Helm repository, so this
# leaves your Helm configuration alone. Every kubectl and helm call names the
# kind-hopper context.
set -euo pipefail
cd "$(dirname "$0")/.."

cluster=hopper
context=kind-hopper

if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --config deploy/kind/cluster.yaml
fi

# First: its CRDs define ServiceMonitor and PodMonitor, which the hopper chart
# renders when autoscaling is on.
helm upgrade --install monitoring oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack \
  --version 91.7.0 --kube-context "$context" -n monitoring --create-namespace \
  -f deploy/kind/monitoring-values.yaml --wait --timeout 10m
# The operator creates the Prometheus pods itself, so helm --wait cannot see them.
kubectl --context "$context" -n monitoring wait prometheus/monitoring-kube-prometheus-prometheus \
  --for=condition=Available --timeout=5m

helm upgrade --install keda keda --repo https://kedacore.github.io/charts \
  --version 2.20.2 --kube-context "$context" -n keda --create-namespace \
  --wait --timeout 5m

# The dashboard Compose provisions, from the same file. Grafana's sidecar
# loads every ConfigMap labelled grafana_dashboard=1.
kubectl --context "$context" -n monitoring create configmap hopper-dashboard \
  --from-file=queue.json=deploy/grafana/dashboards/queue.json --dry-run=client -o yaml |
  kubectl --context "$context" label --local -f - grafana_dashboard=1 -o yaml |
  kubectl --context "$context" apply --server-side -f -

echo "monitoring (kube-prometheus-stack 91.7.0) and keda (2.20.2) are ready on $context"
