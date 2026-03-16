# quant_platform_HF v1.0.1 Release Notes

发布日期：2026-03-16

## 版本摘要

v1.0.1 聚焦主力链回测与可视化一致性修复，解决换月场景下 5 分钟 trace 尖刺、夜盘成交错贴 bar、synthetic rollover marker 缺失，以及校验脚本把换月成交误判为异常的问题。

## 主要变更

1. 主力链连续分析价
- 新增 `backtest.product_series_mode`，支持 `raw` 与 `continuous_adjusted`。
- 产品主力链回测可在保留原始执行价的同时，为指标与 trace 生成连续调整后的分析价。
- `indicator_trace` / `sub_strategy_indicator_trace` 新增 `analysis_bar_*` 与 `analysis_price_offset` 字段。

2. 换月处理与 trace 连续性
- `parquet` 产品链 deterministic 回测启用自动换月处理。
- `strict` 模式会生成 synthetic `rollover_close / rollover_open` 成交并补全时间字段。
- 旧合约残留 5 分钟桶在生命周期结束时立即 flush，避免历史 bar 在回测尾部才写出并污染指标状态。

3. 原子策略与执行语义分离
- `KamaTrend` / `Trend` 系列策略改为基于连续调整后的分析价更新指标与判断趋势。
- 下单价格、风控执行价、成交、持仓与 PnL 仍使用原始主力合约价格。
- 保留真实 session open 跳空，不对底层真实波动做平滑。

4. HTML 标记与校验修复
- Plotly trace 的成交 marker 改为严格按 `trading_day + update_time` 对齐 bar。
- 多合约 trace 支持整条产品链的 marker 展示，并新增 `RolloverOpen / RolloverClose`。
- 回测校验报告改为区分策略成交与 rollover synthetic trades，消除误报。

## 验证结果

- `./build/backtest_replay_support_test` 通过（52 passed / 3 skipped）。
- `./build/atomic_strategies_test` 通过。
- `./build/strategy_main_config_loader_test` 通过。
- `./build/bar_aggregator_test` 通过。
- `./build/run_backtest_from_config_script_test` 通过。
- `./.venv/bin/python -m unittest tests.python.test_plot_sub_trace_plotly -q` 通过（28 tests）。
- `python3 scripts/analysis/backtest_validation_report.py --run-dir docs/results/backtest_runs/backtest-20260316T191548_20260316T191548 --strict` 通过（PASS）。

## 发布资产

- 主分支提交：`3c76b13`
- Git 标签：`v1.0.1`

## 备注

推送 `v1.0.1` tag 后，`.github/workflows/release-package.yml` 会按仓库既有流程触发 release bundle 打包与 GitHub Release 发布。
