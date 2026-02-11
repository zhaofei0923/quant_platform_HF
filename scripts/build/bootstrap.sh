#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e ".[dev]"
.venv/bin/ruff check python
.venv/bin/black --check python
.venv/bin/mypy python/quant_hft
.venv/bin/pytest python/tests -q
