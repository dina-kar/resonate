#!/usr/bin/env bash
# kind on rootless podman, TiDB Operator v2 with 1 PD + 1 TiKV, Dapr, and the
# apps. Idempotent where the tools are.
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
export KIND_EXPERIMENTAL_PROVIDER=podman
OPERATOR_VERSION="${OPERATOR_VERSION:-v2.0.1}"
DAPR_VERSION="${DAPR_VERSION:-1.18.4}"

free -g | awk '/Mem:/ { if ($7 < 5) { print "less than 5 GiB available; refusing"; exit 1 } }'

# Rootless podman: the shell may sit in a slice without the cpu controller;
# a delegated user scope gets all of them (no sudo needed).
kind get clusters 2>/dev/null | grep -qx loam-rtd || systemd-run --user --scope -p Delegate=yes env KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --config "$HERE/kind/cluster.yaml"
kubectl config use-context kind-loam-rtd

# TiDB Operator v2: CRDs, then the operator.
base="https://github.com/pingcap/tidb-operator/releases/download/$OPERATOR_VERSION"
kubectl apply --server-side -f "$base/tidb-operator.crds.yaml"
kubectl apply --server-side -f "$base/tidb-operator.yaml"
kubectl -n tidb-admin rollout status deploy/tidb-operator --timeout=300s

# Dapr control plane, no workflow use (nothing registers one; the bridge's
# guard refuses a workflow backend component).
dapr status -k >/dev/null 2>&1 || dapr init -k --runtime-version "$DAPR_VERSION" --wait --timeout 600

kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata: { name: loam }
EOF
kubectl apply -f "$HERE/tidb/cluster.yaml"
echo "waiting for PD and TiKV"
kubectl -n loam wait --for=condition=Ready pod -l pingcap.com/component=pd --timeout=600s
kubectl -n loam wait --for=condition=Ready pod -l pingcap.com/component=tikv --timeout=600s
kubectl apply -f "$HERE/dapr/components.yaml"
kubectl apply -f "$HERE/apps/apps.yaml"
kubectl -n loam rollout status deploy/redis --timeout=300s
kubectl -n loam rollout status deploy/resonate --timeout=300s
kubectl -n loam rollout status deploy/worker --timeout=300s
kubectl -n loam rollout status deploy/bridge --timeout=300s
kubectl -n loam get pods -o wide
