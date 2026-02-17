# Kubernetes Deployment Runbook (Pure C++)

## Scope

Non-hotpath deployment validation for pure C++ binaries and evidence chain.

## Build Artifacts

```bash
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON
cmake --build build -j$(nproc)
```

## Core Readiness

```bash
./build/reconnect_evidence_cli \
  --report_file docs/results/reconnect_fault_result.md \
  --health_json_file docs/results/ops_health_report.json \
  --health_markdown_file docs/results/ops_health_report.md \
  --alert_json_file docs/results/ops_alert_report.json \
  --alert_markdown_file docs/results/ops_alert_report.md
```

## Cutover/Rollback Evidence

```bash
./build/ctp_cutover_orchestrator_cli \
  --cutover_env docs/results/cutover_result.env \
  --rollback_env docs/results/rollback_result.env
```

## CI-aligned Checks

```bash
bash scripts/build/dependency_audit.sh --build-dir build
bash scripts/build/repo_purity_check.sh --repo-root .
```

## Required Output

- `docs/results/ops_health_report.json`
- `docs/results/ops_alert_report.json`
- `docs/results/cutover_result.env`
- `docs/results/rollback_result.env`
