#!/usr/bin/env bash
# Build the release binaries on the host and wrap them in two images with
# rootless podman, then load them into the kind cluster.
#   CARGO_TARGET_DIR  default ~/.cache/cargo-target/resonate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
TARGET="${CARGO_TARGET_DIR:-$HOME/.cache/cargo-target/resonate}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}" CARGO_INCREMENTAL=0
CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT

(cd "$ROOT/impl/server/core" && CARGO_TARGET_DIR="$TARGET" cargo build --release --bin resonate --features tikv)
(cd "$ROOT/loam/dapr-bridge" && CARGO_TARGET_DIR="$TARGET" cargo build --release --bins)

max_glibc() { objdump -T "$1" | grep -o 'GLIBC_[0-9.]*' | sort -Vu | tail -1; }
for b in resonate resonate-dapr-bridge trigger-worker trigger-check; do
  echo "$b needs $(max_glibc "$TARGET/release/$b") (debian trixie has GLIBC_2.41)"
done

build() { # name, binaries...
  local name="$1"; shift
  rm -rf "$CTX/bin"; mkdir -p "$CTX/bin"
  for b in "$@"; do cp "$TARGET/release/$b" "$CTX/bin/"; done
  cp "$ROOT/deploy/loam/images/Containerfile" "$CTX/Containerfile"
  podman build -q -t "localhost/loam/$name:tikv-dapr" "$CTX"
}
build resonate resonate examples/conctrace
build dapr-bridge resonate-dapr-bridge trigger-worker trigger-check

if [ "${LOAD_KIND:-1}" = 1 ]; then
  export KIND_EXPERIMENTAL_PROVIDER=podman
  for img in resonate dapr-bridge; do
    podman save "localhost/loam/$img:tikv-dapr" -o "$CTX/$img.tar"
    kind load image-archive "$CTX/$img.tar" --name loam-rtd
  done
fi
