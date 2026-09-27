#!/usr/bin/env bash
# Porcupine against the embedded server over a given store, with already-built
# binaries (the same steps as `cargo xtask porcupine`, without rebuilding):
# start `resonate serve` in debug mode on the blob server over STORE, record a
# concurrent history with conctrace, check it with the spec's conccheck.
#   STORE=tikv://127.0.0.1:22379 CLIENTS=8 OPS=600 ./porcupine-embedded.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
T="${CARGO_TARGET_DIR:-$HOME/.cache/cargo-target/resonate}/release"
STORE="${STORE:?set STORE, e.g. tikv://127.0.0.1:22379}"
CLIENTS="${CLIENTS:-8}" OPS="${OPS:-600}"
DIR="$(mktemp -d -t porc-XXXX)"
PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')"
MPORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')"
PREFIX="porc-$(date +%s%N)/"
cd "$DIR"
RESONATE_DEBUG=true \
RESONATE_SERVERS__ACTIVE=server_blob \
RESONATE_SERVERS__SERVER_BLOB__STORE="$STORE" \
RESONATE_SERVERS__SERVER_BLOB__PREFIX="$PREFIX" \
RESONATE_SERVERS__SERVER_BLOB__READ_THROUGH="${READ_THROUGH:-false}" \
RESONATE_GATEWAYS__GATEWAY_HTTP__BIND="127.0.0.1:$PORT" \
RESONATE_GATEWAYS__GATEWAY_METRICS__BIND="127.0.0.1:$MPORT" \
  "$T/resonate" serve > server.log 2> server.err &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 1 80); do curl -sf "http://127.0.0.1:$PORT/ready" >/dev/null && break; sleep 0.5; done
start=$(date +%s.%N)
"$T/examples/conctrace" --url "http://127.0.0.1:$PORT/" --out trace --clients "$CLIENTS" --ops "$OPS" --seed "${SEED:-1}"
rec=$(python3 -c "print(round($(date +%s.%N)-$start,1))")
cd "$ROOT/spec/valid/porc"
start=$(date +%s.%N)
go run ./cmd/conccheck -partition=false < "$DIR/trace.history"
chk=$(python3 -c "print(round($(date +%s.%N)-$start,1))")
echo "porcupine seed=${SEED:-1} store=$STORE clients=$CLIENTS ops=$OPS record_secs=$rec check_secs=$chk dir=$DIR"
