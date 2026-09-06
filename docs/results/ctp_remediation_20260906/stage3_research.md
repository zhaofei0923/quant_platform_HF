# 阶段 3：研究语义与读取路径独立验收

本阶段暂存代码树为 `2ce5e355252d81c9e58169060d24bd52f15b912a`，基于运行时提交
`87ddeea`。独立目录 `/tmp/quant-hft-stage3-snapshot` 仍使用显式源清单的单库 CMake，
仅新增研究实现与测试；模块目标拆分随后提交。

默认 `online_parity` 使用 `sim` 参数，保留回测执行身份。复用市场 Bar、指标、主力
协调器和风险规则，使用虚拟时钟。连续调整价格及旧换月协议要求显式 `research`。
尚无等价模型的预算或规则上下文明确报错。输入改用批次游标和稳定归并，OOS/滚动
缓存身份覆盖输入内容与参数，选型用途及独立窗口的资金口径明确标注。

新增 `computation_semantics_version=3.0.0` 写入 JSON、Markdown 和参数签名；保留
原 `hf_standard.version=2.0` 报告格式字段。缺少计算语义版本的旧结果不能视作同一
计算版本。固定对照的原因和数值变化见 [研究验证](research_validation.md)。

## 快照验收

- Arrow/Parquet 16.1.0、C++20：`core_engine`、`backtest_cli`、`parameter_optim_cli`、
  `rolling_backtest_cli` 和 14 个研究测试二进制均构建成功。
- 14 个二进制共 136 项：132 通过、4 跳过、0 失败。
- 跳过项为 2 个未安装仓库大数据集的测试，及 2 个仅适用于关闭 Arrow 的反向测试。
  合成数据、完整 Bar、风控差异、缓存、批次读取及优化测试实际执行。
- 最后增加计算版本元数据后，仅增量重建并重跑相关的回测报告与 OOS 84 项，全部通过。
  没有重复整套矩阵。

证据分别为 `stage3_research_build.log`、`stage3_research_tests.log` 和
`research_artifacts/computation_version_tests.log`。完整工作区的 919 项 Arrow 验证
保留在研究报告中，并明确其采证时点。

```bash
PKG_CONFIG_PATH=/home/kevin/.cache/quant-hft-deps/pkgconfig \
cmake -S /tmp/quant-hft-stage3-snapshot -B /tmp/quant-hft-stage3-build \
  -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_ARROW_PARQUET=ON \
  -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src
cmake --build /tmp/quant-hft-stage3-build -j8 --target core_engine backtest_cli \
  parameter_optim_cli rolling_backtest_cli replay_market_parity_test backtest_replay_support_test \
  metric_extractor_test oos_top10_validation_test rolling_config_test rolling_runner_fixed_test \
  rolling_runner_optimize_test window_generator_test backtest_data_feed_test parquet_data_feed_test \
  indicator_trace_parquet_writer_test sub_strategy_indicator_trace_parquet_writer_test \
  temp_config_generator_test task_scheduler_test
```

各测试二进制从快照源码根目录运行。`stage3_tree.txt` 保存不含新增阶段文档的代码树身份。
