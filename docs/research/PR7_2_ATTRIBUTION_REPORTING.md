# PR-7.2 归因分析与报告落盘

## 范围
- 在 SimNow compare 结果中新增确定性归因字段：`attribution` 与 `risk_decomposition`。
- 新增 HTML 报告输出与 SQLite 持久化。
- CLI 扩展输出参数：`--output-html`、`--sqlite-path`。

## 关键字段
- `attribution.signal_parity`：意图数量一致性评分。
- `attribution.execution_coverage`：订单事件覆盖率评分。
- `attribution.threshold_stability`：阈值稳定性评分。
- `risk_decomposition.model_drift`：信号差异风险。
- `risk_decomposition.execution_gap`：执行覆盖缺口风险。
- `risk_decomposition.consistency_gap`：一致性差距风险。

## 执行示例
```bash
python scripts/simnow/run_simnow_compare.py \
  --config configs/sim/ctp.yaml \
  --csv-path backtest_data/rb.csv \
  --max-ticks 300 \
  --dry-run \
  --output-json docs/results/simnow_compare_report.json \
  --output-html docs/results/simnow_compare_report.html \
  --sqlite-path runtime/simnow/simnow_compare.sqlite
```
