#!/usr/bin/env bash
set -euo pipefail
build_dir=build
results_dir=docs/results
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir) build_dir="$2"; shift 2;;
    --results-dir) results_dir="$2"; shift 2;;
    -h|--help) echo "Offline recovery gate only; does not connect to a broker or certify five-day acceptance."; exit 0;;
    *) echo "unsupported option: $1" >&2; exit 2;;
  esac
done
mkdir -p "$results_dir"
ctest --test-dir "$build_dir" --no-tests=error --output-on-failure \
  -R 'DurableOrderEventInbox|WalReplay|AccountExecutionScheduler|RuntimeIdentity|StatePersistence|DeploymentConfig' \
  --output-junit "$results_dir/offline_recovery.xml"
