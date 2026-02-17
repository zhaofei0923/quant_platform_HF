# Pure C++ Cutover Gate Index (2026-02-17)

## Hard Gates

| Gate | Status | Evidence |
|---|---|---|
| 全阶段功能开发完成并自测通过 | PASS (local) | `ctest --test-dir build --output-on-failure` (254/254 pass) |
| 影子一致性测试通过（字段级） | PASS (local) | `docs/results/shadow_consistency_report.json`, `docs/results/consistency_gate_summary.json` |
| 回测一致性验证通过（双跑差异归档） | PASS (local) | `docs/results/backtest_consistency_report.json`, `docs/results/backtest_consistency_a.json`, `docs/results/backtest_consistency_b.json` |
| 预生产全量演练完成 | PASS (local) | `docs/results/preprod_rehearsal_report.json`, `docs/results/preprod_rehearsal_report.md` |
| 纯 C++ CI 门禁全绿 | PASS (workflow configured), PENDING (remote run) | `.github/workflows/ci.yml`, `.github/workflows/release-package.yml`, `.github/workflows/simnow_compare_daily.yml`, `.github/workflows/simnow_weekly_stress.yml` |
| 文档、requirements、runbook 同步更新 | PASS | `README.md`, `docs/USAGE_GUIDE_DEVELOPER.md`, `docs/requirements/develop_requirements.yaml` |
| 回滚演练已实际执行一次并通过 | PASS (local) | `docs/results/preprod_rollback_result.env`, `docs/results/preprod_rehearsal_report.json` |

## Implemented Gates and Commands

```bash
bash scripts/build/dependency_audit.sh --build-dir build
bash scripts/build/repo_purity_check.sh --repo-root .
bash scripts/build/run_consistency_gates.sh --build-dir build --results-dir docs/results
bash scripts/build/run_preprod_rehearsal_gate.sh --build-dir build --results-dir docs/results
./build/verify_contract_sync_cli
./build/verify_develop_requirements_cli
ctest --test-dir build --output-on-failure
bash scripts/build/package_nonhotpath_release.sh "$(date -u +%Y%m%dT%H%M%SZ)"
./build/simnow_compare_cli --config configs/sim/ctp.yaml --csv_path runtime/benchmarks/backtest/rb_ci_sample.csv --run_id simnow-daily --output_json docs/results/simnow_compare_report.json
./build/simnow_weekly_stress_cli --week "$(date -u +%G-W%V)" --scenarios baseline --csv_path runtime/benchmarks/backtest/rb_ci_sample.csv --output_json docs/results/simnow_weekly_stress.json
```

## Evidence Artifacts

- `docs/results/backtest_cli_smoke.json`
- `docs/results/backtest_benchmark_result.json`
- `docs/results/shadow_consistency_report.json`
- `docs/results/backtest_consistency_report.json`
- `docs/results/consistency_gate_summary.json`
- `docs/results/preprod_rehearsal_report.json`
- `docs/results/preprod_rehearsal_report.md`
- `docs/results/preprod_cutover_result.env`
- `docs/results/preprod_rollback_result.env`
- `docs/results/simnow_compare_report.json`
- `docs/results/simnow_weekly_stress.json`
- `docs/results/ops_health_report.json`
- `docs/results/ops_alert_report.json`
- `docs/results/reconnect_fault_result.md`
- `docs/results/cutover_result.env`
- `docs/results/rollback_result.env`
- `dist/quant-hft-cpp-*.tar.gz`
- `dist/quant-hft-cpp-*.tar.gz.sha256`
