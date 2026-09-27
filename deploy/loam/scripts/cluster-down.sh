#!/usr/bin/env bash
# Tear the validation cluster down completely (the machine is small).
set -euo pipefail
export KIND_EXPERIMENTAL_PROVIDER=podman
kind delete cluster --name loam-rtd || true
