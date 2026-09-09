# quant_platform_HF Agent Guide

This repository is the C++17 online host. The approved three-repository architecture supersedes the old monorepo map.

- Trading: this repository, one account per process, CTP/execution/account risk/authoritative transactions/recovery/readonly dashboard.
- Shared algorithms: separately released `quant_strategies`; consume only `find_package(QuantStrategies 1.0.0 EXACT CONFIG REQUIRED)` and the immutable dependency lock. Never copy algorithm/indicator/research implementations back here.
- Research: separate `quant_research`, owns all backtests/optimization/rolling, historical data and Python data/analysis tooling. No Python strategy runner or bridge in trading.
- Source/build in WSL. Runtime, SDK, credentials, and E-drive data stay outside Git.
- Configuration, account/instance identity and state envelopes are defined in `docs/ops/three_project_migration.md`. Keep stable ownership, gross long/short, economic vs physical close allocation, actual fees and transaction watermarks.
- Never modify existing trading services or force close positions as part of refactoring. Live deployment is separate from code validation. Keep readonly dashboard independent.
- C++ style: namespace quant_hft, C++17, Google-derived .clang-format, 4-space indent, 100 columns. Hot path allocation-aware. No secret literals.

Build: `cmake -S . -B build -DCMAKE_PREFIX_PATH=/installed/package -DQUANT_HFT_BUILD_TESTS=ON`; `cmake --build build -j`; `ctest --test-dir build --no-tests=error --output-on-failure`.

Gates: `scripts/build/dependency_audit.sh`, `repo_purity_check.sh`, `doc_purity_check.sh`, `three_project_boundary_check.sh`; the consistency/rehearsal scripts exercise offline online-event cases and do not certify five trading days.

Use existing `.env.example` only as a format template. Never inspect/print secret values to troubleshoot, and do not commit runtime state. Log evidence to `docs/results` or an explicitly designated migration directory. See README and docs/ops/three_project_migration.md first; archived monorepo docs are historical.
