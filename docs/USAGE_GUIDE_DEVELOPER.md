# Developer Usage Guide (Pure C++)

## Scope

This repository now runs on a pure C++ execution and strategy stack.
All runtime, backtest, ops evidence, and CI gates are C++/Bash based.

## Quick Start

```bash
./scripts/build/bootstrap.sh
```

## Configure, Build, Test

```bash
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON
cmake --build build -j$(nproc)
ctest --test-dir build --output-on-failure
```

## Quality Gates

```bash
bash scripts/build/dependency_audit.sh --build-dir build
bash scripts/build/repo_purity_check.sh --repo-root .
./build/verify_contract_sync_cli
./build/verify_develop_requirements_cli
```

## Core Engine

```bash
export CTP_SIM_PASSWORD='your_password'
./build/core_engine configs/sim/ctp.yaml
```

## Backtest Replay

```bash
mkdir -p docs/results
./build/backtest_cli \
  --engine_mode csv \
  --csv_path backtest_data/rb.csv \
  --max_ticks 5000 \
  --output_json docs/results/backtest_cli_smoke.json \
  --output_md docs/results/backtest_cli_smoke.md
```

## SimNow Compare and Stress

```bash
./build/simnow_compare_cli \
  --config configs/sim/ctp.yaml \
  --run_id simnow-daily \
  --output_json docs/results/simnow_compare_report.json

./build/simnow_weekly_stress_cli \
  --week "$(date -u +%G-W%V)" \
  --scenarios baseline \
  --output_json docs/results/simnow_weekly_stress.json
```

## Ops Evidence Chain

```bash
./build/reconnect_evidence_cli \
  --report_file docs/results/reconnect_fault_result.md \
  --health_json_file docs/results/ops_health_report.json \
  --health_markdown_file docs/results/ops_health_report.md \
  --alert_json_file docs/results/ops_alert_report.json \
  --alert_markdown_file docs/results/ops_alert_report.md

./build/ctp_cutover_orchestrator_cli \
  --cutover_env docs/results/cutover_result.env \
  --rollback_env docs/results/rollback_result.env
```

## Strategy Engine Smoke

```bash
ctest --test-dir build -R "(StrategyRegistryTest|StrategyEngineTest|DemoLiveStrategyTest|CallbackDispatcherTest)" --output-on-failure
```

## Notes

- If any old document still mentions removed runtime assets, treat it as historical context.
- Canonical runtime commands are the C++ CLIs under `build/`.
