#!/usr/bin/env bash
# Porcupine with TWO Resonate servers over the same store and key prefix,
# requests spread round-robin between them: the multi-replica case the
# Kubernetes deployment runs. READ_THROUGH=true makes every request read the
# store; false keeps upstream's per-process document cache.
#   STORE=tikv://127.0.0.1:22379 READ_THROUGH=true SEED=1 ./porcupine-two-servers.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
T="${CARGO_TARGET_DIR:-$HOME/.cache/cargo-target/resonate}/release"
STORE="${STORE:?}" CLIENTS="${CLIENTS:-8}" OPS="${OPS:-2400}" SEED="${SEED:-1}"
DIR="$(mktemp -d -t porc2-XXXX)"
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])'; }
PREFIX="porc2-$(date +%s%N)/"
PIDS=()
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT
URLS=()
for i in 1 2; do
  P=$(free_port); M=$(free_port)
  RESONATE_DEBUG=true RESONATE_SERVERS__ACTIVE=server_blob \
  RESONATE_SERVERS__SERVER_BLOB__STORE="$STORE" RESONATE_SERVERS__SERVER_BLOB__PREFIX="$PREFIX" \
  RESONATE_SERVERS__SERVER_BLOB__READ_THROUGH="${READ_THROUGH:-true}" \
  RESONATE_GATEWAYS__GATEWAY_HTTP__BIND="127.0.0.1:$P" RESONATE_GATEWAYS__GATEWAY_METRICS__BIND="127.0.0.1:$M" \
    "$T/resonate" serve > "$DIR/server$i.log" 2> "$DIR/server$i.err" &
  PIDS+=($!); URLS+=("http://127.0.0.1:$P/")
  for _ in $(seq 1 80); do curl -sf "http://127.0.0.1:$P/ready" >/dev/null && break; sleep 0.5; done
done
PP=$(free_port)
python3 "$HERE/rr-proxy.py" "$PP" "${URLS[@]}" & PIDS+=($!)
sleep 1
cd "$DIR"
"$T/examples/conctrace" --url "http://127.0.0.1:$PP/" --out trace --clients "$CLIENTS" --ops "$OPS" --seed "$SEED"
cd "$ROOT/spec/valid/porc"
go run ./cmd/conccheck -partition=false < "$DIR/trace.history"
echo "porcupine-2 seed=$SEED read_through=${READ_THROUGH:-true} store=$STORE dir=$DIR"
