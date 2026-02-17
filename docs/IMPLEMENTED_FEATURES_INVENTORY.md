# Implemented Features Inventory (Pure C++)

## Scope

Inventory reflects the current pure C++ implementation only.

## Strategy and Runtime

- `core_engine` runtime entrypoint
- `ILiveStrategy` lifecycle interface
- `StrategyRegistry` + `StrategyEngine`
- `DemoLiveStrategy`
- In-process callback dispatch (`CallbackDispatcher`)

## Backtest and Research CLIs

- `backtest_cli`
- `factor_eval_cli`
- `backtest_benchmark_cli`
- `csv_parquet_compare_cli`

## SimNow CLIs

- `simnow_compare_cli`
- `simnow_weekly_stress_cli`
- `simnow_probe`

## Ops and Evidence CLIs

- `reconnect_evidence_cli`
- `ops_health_report_cli`
- `ops_alert_report_cli`
- `ctp_cutover_orchestrator_cli`

## Build and Contract Verification CLIs

- `verify_contract_sync_cli`
- `verify_develop_requirements_cli`

## Quality Gates

- `scripts/build/dependency_audit.sh`
- `scripts/build/repo_purity_check.sh`
- `tests/unit/build/quality_gate_scripts_test.cpp`

## Legacy Archive

Historical mixed-stack feature inventory is archived at:

- `docs/archive/IMPLEMENTED_FEATURES_INVENTORY_legacy_mixed_stack.md`
