# Architecture Handbook V1 (Pure C++)

## Summary

The platform is now a pure C++ system for runtime, backtest, and ops evidence generation.

## Core Components

- Runtime: `core_engine`
- Strategy orchestration: `StrategyEngine` + `ILiveStrategy`
- Backtest: `backtest_cli` + `factor_eval_cli` + benchmark/compare CLIs
- Ops evidence: reconnect/health/alert/cutover C++ CLIs

## Quality Gates

- Build + tests: `cmake` + `ctest`
- Dependency gate: `scripts/build/dependency_audit.sh`
- Purity gate: `scripts/build/repo_purity_check.sh`
- Contract/requirements gates: `verify_contract_sync_cli`, `verify_develop_requirements_cli`
