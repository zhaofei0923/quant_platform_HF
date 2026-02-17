# 一键部署与演练（Pure C++）

通过统一入口完成切换证据链生成：

- 生成 `cutover_result.env`
- 生成 `rollback_result.env`
- 同步输出 health/alert 报告

## 命令

```bash
./build/ctp_cutover_orchestrator_cli \
  --cutover_env docs/results/cutover_result.env \
  --rollback_env docs/results/rollback_result.env

./build/ops_health_report_cli \
  --output_json docs/results/ops_health_report.json \
  --output_md docs/results/ops_health_report.md

./build/ops_alert_report_cli \
  --health-json-file docs/results/ops_health_report.json \
  --output_json docs/results/ops_alert_report.json \
  --output_md docs/results/ops_alert_report.md
```

## 输出

- `docs/results/cutover_result.env`
- `docs/results/rollback_result.env`
- `docs/results/ops_health_report.json`
- `docs/results/ops_alert_report.json`
