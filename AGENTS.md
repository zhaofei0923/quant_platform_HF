# Agent Guidelines for quant_platform_HF

Use this file as the always-on project guide for coding agents. Keep it concise: link to deeper docs instead of copying them here.

## Current Stack

- The active execution and strategy stack is pure C++: C++17 by default, C++20 only when `QUANT_HFT_ENABLE_ARROW_PARQUET=ON` is configured.
- Do not reintroduce the retired Python package, pybind bridge, or Python strategy runner. The only Python assets allowed by repository policy are the small analysis/build helper scripts and tests whitelisted in [scripts/build/repo_purity_check.sh](scripts/build/repo_purity_check.sh).
- Treat [docs/archive](docs/archive) and [docs/archive_legacy](docs/archive_legacy) as historical context only. Prefer [README.md](README.md), [develop/00-实现对齐矩阵与缺口总览.md](develop/00-实现对齐矩阵与缺口总览.md), and focused docs under [develop](develop) for current design intent.
- Never hard-code CTP credentials. Use local `.env` values derived from [.env.example](.env.example) and keep secrets out of YAML, docs, and tests.

## Architecture Map

- Main library: `quant_hft_core` in [CMakeLists.txt](CMakeLists.txt), with CLI entry points in [src/apps](src/apps).
- Public APIs and shared domain types live under [include/quant_hft](include/quant_hft); keep cross-module contracts centralized there.
- The intended dependency direction is `Apps -> Strategy -> Services -> Core -> Contracts`.
- Strategy orchestration lives in [src/strategy](src/strategy) and [include/quant_hft/strategy](include/quant_hft/strategy). `StrategyEngine` dispatches `ILiveStrategy` callbacks and routes `SignalIntent` into execution/risk.
- Domain services live under [src/services](src/services): risk, order/execution, portfolio, settlement, and market state.
- Core adapters live under [src/core](src/core): CTP gateway/config, WAL regulatory replay, storage clients, common dispatchers, performance helpers, and monitoring.
- Backtest, indicators, parameter optimization, and rolling validation are first-class C++ modules under [src/backtest](src/backtest), [src/indicators](src/indicators), [src/optim](src/optim), and [src/rolling](src/rolling).
- For detailed architecture, link to [develop/01-系统架构设计/01-02-核心模块划分与职责.md](develop/01-系统架构设计/01-02-核心模块划分与职责.md) and [develop/06-开发者指南/06-02-代码结构与开发流程.md](develop/06-开发者指南/06-02-代码结构与开发流程.md).

## Build and Test

```bash
# Full bootstrap on a fresh Ubuntu-like machine: install deps, configure, build, test, audit.
./scripts/build/bootstrap.sh

# Manual configure with tests and compile_commands.json for clang-tidy.
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# Build all targets.
cmake --build build -j$(nproc)

# Run all C++ tests.
ctest --test-dir build --output-on-failure

# Run focused tests discovered by CTest.
ctest --test-dir build -R "(strategy_engine_test|execution_engine_test)" --output-on-failure

# Run a test binary directly with a GoogleTest filter.
./build/object_pool_test --gtest_filter='*ReusesReleasedSlot*'
```

Useful configuration flags:

```bash
# Isolated GCC build when mixed-toolchain state causes trouble.
cmake -S . -B build-gcc -DQUANT_HFT_BUILD_TESTS=ON \
  -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++

# Optional feature paths.
cmake -S . -B build-parquet -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_ARROW_PARQUET=ON
cmake -S . -B build-ext -DQUANT_HFT_BUILD_TESTS=ON \
  -DQUANT_HFT_ENABLE_REDIS_EXTERNAL=ON -DQUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=ON
cmake -S . -B build-real -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_CTP_REAL_API=ON
```

## Quality Gates

Run the narrowest useful verification for the change first, then broaden before handoff.

```bash
# CI-style hard gates.
bash scripts/build/dependency_audit.sh --build-dir build
bash scripts/build/repo_purity_check.sh --repo-root .
bash scripts/build/doc_purity_check.sh --repo-root .

# Regression and rehearsal gates used by CI and release work.
bash scripts/build/run_consistency_gates.sh --build-dir build --results-dir docs/results
bash scripts/build/run_preprod_rehearsal_gate.sh --build-dir build --results-dir docs/results

# One-command v3 acceptance: core simulation, WAL replay, and evidence artifacts.
scripts/ops/run_v3_acceptance.sh
```

Use the helper scripts that match the area being changed:

```bash
python3 scripts/build/verify_products_info_sync.py
python3 scripts/build/verify_config_docs_coverage.py
bash scripts/build/run_parameter_optim.sh
bash scripts/build/run_rolling_backtest.sh
```

## Formatting and Static Checks

```bash
# Format check and apply using the repository .clang-format.
git ls-files '*.h' '*.cpp' | xargs clang-format --dry-run -Werror --style=file
git ls-files '*.h' '*.cpp' | xargs clang-format -i --style=file

# clang-tidy uses .clang-tidy and requires a configured build with compile_commands.json.
run-clang-tidy -p build -header-filter='include/quant_hft/.*|src/.*'
```

- Formatting is Google-based with 4-space indentation, 100-column limit, left pointer alignment, and case-sensitive include sorting.
- `.clang-tidy` enables `bugprone-*`, `cppcoreguidelines-*`, `modernize-*`, `performance-*`, and `readability-*`.

## C++ Conventions

- Namespace project code under `quant_hft`.
- Use `PascalCase` for classes/structs, `snake_case` for functions and locals, trailing underscore for member fields, and `kCamelCase` or `UPPER_SNAKE_CASE` for constants.
- Include order is system headers, third-party headers, then project headers relative to `include/`.
- Prefer `std::unique_ptr`/`std::shared_ptr` with `std::make_unique`/`std::make_shared`.
- Throw specific `std::runtime_error`-style exceptions for unrecoverable failures; use boundary-level catch blocks only where recovery or diagnostics are meaningful.
- Keep hot-path code allocation-aware. Reuse existing object pools, dispatchers, fixed-decimal utilities, and domain stores instead of adding parallel mechanisms.

## Documentation and Evidence

- Link existing docs instead of duplicating long explanations. Start with [README.md](README.md), [develop/06-开发者指南/06-01-环境搭建与快速开始.md](develop/06-开发者指南/06-01-环境搭建与快速开始.md), and [develop/08-测试与验证/08-01-测试策略与验证方案.md](develop/08-测试与验证/08-01-测试策略与验证方案.md).
- Keep current docs free of retired Python-era workflow references; [scripts/build/doc_purity_check.sh](scripts/build/doc_purity_check.sh) enforces this for `docs` and [README.md](README.md).
- For operational or acceptance work, write evidence under [docs/results](docs/results) or [runtime](runtime) only when a script or runbook expects it.
- When touching SimNow or CTP behavior, check the current SimNow notes in repository memory and [README.md](README.md) before changing config defaults.