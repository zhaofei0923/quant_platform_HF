#!/usr/bin/env bash
set -euo pipefail
repo_root="."
build_dir="build"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-root) repo_root="$2"; shift 2;;
        --build-dir) build_dir="$2"; shift 2;;
        *) echo "unknown boundary option: $1" >&2; exit 2;;
    esac
done
cd "$repo_root"
for subtree in src/backtest src/core/backtest src/optim src/rolling src/indicators src/strategy/atomic; do
    if [[ -d "$subtree" ]] && find "$subtree" -type f -print -quit | grep -q .; then
        echo "migrated implementation remains in trading: $subtree" >&2
        exit 1
    fi
done
if rg -n -e 'quant_hft/(backtest|optim|rolling)/' -e 'apps/backtest_replay_support.h' src include; then
    echo "trading source still imports research headers" >&2
    exit 1
fi
if rg -n -e '(find_package|add_subdirectory).*Python|pybind|quant_hft_backtest|quant_hft_rolling|quant_hft_optim' CMakeLists.txt; then
    echo "research/runtime dependency remains in trading build" >&2
    exit 1
fi
test -s "$build_dir/CMakeCache.txt"
test -s dependencies.lock.json
echo "three-project trading boundary passed"
