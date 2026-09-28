#!/usr/bin/env bash
# In-cluster porcupine: conctrace in the recorder pod against the two debug
# replicas, via the Service (VIA=svc) or via Dapr service invocation
# (VIA=dapr); the history is copied out and checked on the host.
#   VIA=svc|dapr SEED=1 OPS=2400 ./porcupine-cluster.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
VIA="${VIA:-svc}" SEED="${SEED:-1}" OPS="${OPS:-2400}" CLIENTS="${CLIENTS:-8}"
case "$VIA" in
  svc)  URL="http://resonate-debug.loam.svc.cluster.local:8001/" ;;
  dapr) URL="http://127.0.0.1:3500/v1.0/invoke/resonate-debug/method/" ;;
esac
out="/tmp/porc-$VIA-$SEED"
kubectl -n loam exec deploy/porc-recorder -c rec -- sh -c "rm -rf $out && mkdir -p $out && cd $out && /usr/local/bin/conctrace --url $URL --out trace --clients $CLIENTS --ops $OPS --seed $SEED"
local_dir="$(mktemp -d -t porc-k8s-XXXX)"
kubectl -n loam exec deploy/porc-recorder -c rec -- cat "$out/trace.history" > "$local_dir/trace.history"
cd "$ROOT/spec/valid/porc"
go run ./cmd/conccheck -partition=false < "$local_dir/trace.history"
echo "porcupine-cluster via=$VIA seed=$SEED ops=$OPS history=$local_dir/trace.history"
