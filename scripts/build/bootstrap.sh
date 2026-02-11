#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
python3 scripts/perf/run_hotpath_bench.py \
  --benchmark-bin build/hotpath_benchmark \
  --baseline configs/perf/baseline.json \
  --result-json docs/results/hotpath_bench_result.json
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e ".[dev]"
.venv/bin/ruff check python scripts
.venv/bin/black --check python scripts
.venv/bin/mypy python/quant_hft
.venv/bin/python scripts/build/verify_contract_sync.py
.venv/bin/python scripts/build/verify_develop_requirements.py
.venv/bin/pytest python/tests -q
