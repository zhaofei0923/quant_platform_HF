# Systemd Deployment Runbook (Pure C++)

## Scope

Single-host deployment baseline for pure C++ runtime.

## Build and Smoke

```bash
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON
cmake --build build -j$(nproc)
ctest --test-dir build --output-on-failure
```

## Start Core Engine

```bash
export CTP_SIM_PASSWORD='your_password'
./build/core_engine configs/sim/ctp.yaml
```

## Generate Ops Evidence

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

## Readiness Gates

```bash
bash scripts/build/dependency_audit.sh --build-dir build
bash scripts/build/repo_purity_check.sh --repo-root .
./build/verify_contract_sync_cli
./build/verify_develop_requirements_cli
```

## Required Artifacts

- `docs/results/ops_health_report.json`
- `docs/results/ops_alert_report.json`
- `docs/results/cutover_result.env`
- `docs/results/rollback_result.env`
