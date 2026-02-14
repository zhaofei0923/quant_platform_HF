# Agent Guidelines for quant_platform_HF

This document provides essential commands and style guidelines for agentic coding agents working in this repository. It covers build, lint, test commands, and code style conventions for both C++ and Python.

## Build Commands

### C++ Build
```bash
# Configure with tests enabled (default)
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON

# If mixed-toolchain issues appear in WSL, use isolated GCC build dir
cmake -S . -B build-gcc -DQUANT_HFT_BUILD_TESTS=ON -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++

# Build all targets
cmake --build build -j$(nproc)

# Build all targets in GCC-isolated directory
cmake --build build-gcc -j$(nproc)

# Build a specific target (e.g., core_engine)
cmake --build build --target core_engine

# Configure with external Redis/TimescaleDB support (optional)
cmake -S . -B build-ext -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_REDIS_EXTERNAL=ON -DQUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=ON
```

### Python Environment
```bash
# Create virtual environment (if not present)
python3 -m venv .venv
source .venv/bin/activate

# Install package with development dependencies
pip install -e ".[dev]"
```

## Lint & Format Commands

### C++ Linting
```bash
# Run clang-tidy (requires build directory with compile_commands.json)
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cd build && run-clang-tidy -header-filter='.*' -checks='-*,bugprone-*,cppcoreguidelines-*,modernize-*,performance-*,readability-*'

# Format with clang-format (check)
clang-format --dry-run -Werror --style=file include/**/*.h src/**/*.cpp

# Format (apply)
clang-format -i --style=file include/**/*.h src/**/*.cpp
```

### Python Linting & Formatting
```bash
# Ruff check (fast linting)
ruff check python scripts

# Black format check
black --check python scripts

# Black format (apply)
black python scripts

# MyPy type checking (strict)
mypy python/quant_hft
```

## Test Commands

### C++ Tests
```bash
# Run all C++ tests via CTest
ctest --test-dir build --output-on-failure

# Run a specific test executable
./build/tests/unit/core/object_pool_test

# Run a single test case with gtest filter
./build/tests/unit/core/object_pool_test --gtest_filter=*ReusesReleasedSlot*

# Discover and run tests with CTest regex
ctest --test-dir build -R object_pool_test
```

### Python Tests
```bash
# Run all Python tests
pytest python/tests -q

# Run a specific test file
pytest python/tests/unit/test_contracts.py -q

# Run a single test function
pytest python/tests/unit/test_contracts.py::test_signal_intent_validation -v

# With coverage report
pytest python/tests --cov=quant_hft --cov-report=html
```

### Additional Verification Scripts
```bash
# Verify Protobuf contract synchronization
python scripts/build/verify_contract_sync.py

# Verify development requirements
python scripts/build/verify_develop_requirements.py

# Performance benchmark smoke test
python scripts/perf/run_hotpath_bench.py --benchmark-bin build/hotpath_benchmark --baseline configs/perf/baseline.json

# One-command v3 acceptance (core_sim + WAL replay + evidence)
scripts/ops/run_v3_acceptance.sh
```

## Code Style Guidelines

### C++ Style
- **Formatting**: Follow `.clang-format` (Google style with 4‑space indent, 100‑column limit, pointer alignment left).
- **Includes**: Sort includes case‑sensitive. Use `#pragma once` guards.
- **Naming**:
  - Class/struct names: `PascalCase`
  - Functions/variables: `snake_case`
  - Member variables: `snake_case_` (trailing underscore)
  - Constants: `kCamelCase` or `UPPER_SNAKE_CASE`
- **Namespace**: `quant_hft` for project code.
- **Error handling**: Prefer `throw std::runtime_error` for fatal errors; use `try`/`catch(...)` for recovery where appropriate.
- **Smart pointers**: Use `std::unique_ptr` and `std::shared_ptr` with `std::make_unique`/`std::make_shared`.
- **Clang‑Tidy**: Enabled checks include `bugprone-*`, `cppcoreguidelines-*`, `modernize-*`, `performance-*`, `readability-*`.

### Python Style
- **Formatting**: Black with line length 100, target Python 3.10.
- **Linting**: Ruff with select rules (`E`, `F`, `I`, `B`, `UP`).
- **Type checking**: MyPy strict mode.
- **Naming**:
  - Class names: `PascalCase`
  - Functions/variables: `snake_case`
  - Constants: `UPPER_SNAKE_CASE`
- **Imports**: Group standard library, third‑party, local imports separated by blank lines. Use absolute imports.
- **Docstrings**: Triple‑quoted strings, first line brief, followed by description.
- **Error handling**: Raise specific exceptions (`ValueError`, `RuntimeError`) with descriptive messages. Avoid bare `except:`.

## Import Conventions

### C++
```cpp
// System headers first
#include <cstdint>
#include <memory>

// Third‑party headers (if any)
#include <gtest/gtest.h>

// Project headers (relative to `include/`)
#include "quant_hft/core/object_pool.h"
```

### Python
```python
from __future__ import annotations

import os
import sys
from typing import Any, Optional

import numpy as np
import pandas as pd

from quant_hft.contracts import OrderEvent, SignalIntent
```

## Naming Conventions

| Language | Category | Convention | Example |
|----------|----------|------------|---------|
| C++ | Class/struct | `PascalCase` | `ObjectPool` |
| C++ | Function/method | `snake_case` | `acquire()` |
| C++ | Variable | `snake_case` | `buffer_size` |
| C++ | Member variable | `snake_case_` | `capacity_` |
| C++ | Constant | `kCamelCase` or `UPPER_SNAKE_CASE` | `kDefaultSize`, `MAX_RETRIES` |
| Python | Class | `PascalCase` | `StrategyBase` |
| Python | Function | `snake_case` | `validate_signal_intents` |
| Python | Variable | `snake_case` | `instrument_id` |
| Python | Constant | `UPPER_SNAKE_CASE` | `BACKTEST_CTX_REQUIRED_KEYS` |

## Error Handling Patterns

### C++
- Use exceptions for unrecoverable errors: `throw std::runtime_error("description");`
- Catch exceptions at subsystem boundaries with `catch(...)` to log and continue where recovery is possible.
- Avoid throwing in destructors.
- Use `noexcept` where appropriate.

### Python
- Raise built‑in exceptions with clear messages: `raise ValueError("intent strategy_id is required")`
- Use custom exception classes for domain‑specific errors.
- Use `try`/`except` blocks to handle recoverable errors; always specify exception types.
- Log errors with `logging.exception()` in exception handlers.

## Additional Resources

- **`.clang‑format`**: Google‑based style with project‑specific tweaks.
- **`.clang‑tidy`**: Enabled check groups listed in the file.
- **`pyproject.toml`**: Black, Ruff, MyPy configuration.
- **`CMakeLists.txt`**: Build options, test definitions.
- **`.github/workflows/ci.yml`**: CI pipeline for reference.
- **`scripts/build/bootstrap.sh`**: Full bootstrap script.

## Notes for Agents

- Always run linting and formatting commands before committing.
- For C++, ensure the build directory is configured with `-DCMAKE_EXPORT_COMPILE_COMMANDS=ON` for clang‑tidy.
- Python dependencies are installed via `pip install -e ".[dev]"`.
- Run tests for both C++ and Python after making changes.
- Follow the existing patterns in the codebase for consistency.

---

*This file is intended to be updated as the project evolves. Keep it concise and focused on information needed by automated agents.*