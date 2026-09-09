#!/usr/bin/env bash
set -euo pipefail
build_dir=build
results_dir=docs/results
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir) build_dir="$2"; shift 2;;
    --results-dir) results_dir="$2"; shift 2;;
    -h|--help) echo "Offline online-event consistency: --build-dir PATH --results-dir PATH"; exit 0;;
    *) echo "unsupported option: $1" >&2; exit 2;;
  esac
done
mkdir -p "$results_dir"
ctest --test-dir "$build_dir" --no-tests=error --output-on-failure \
  -R 'TradingDomainStore|StrategyEngine|PendingExit|PositionManager|ExecutionEngine' \
  --output-junit "$results_dir/online_consistency.xml"
