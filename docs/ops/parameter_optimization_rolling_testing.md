# 参数优化与 Rolling Testing 使用指南

## 适用范围

本文基于当前仓库已经完成的 phase1-phase3 能力整理，目标是把研究流程收敛成一条可重复执行的链路：

1. 定义参数空间并找出候选最优参数。
2. 在 rolling optimize 中检验参数是否具备跨窗口泛化能力。
3. 对 TopN 候选参数做样本外对比，并生成最终推荐参数。
4. 将推荐参数落地到正式回测配置，继续验证运行稳定性。

如果你只记住一条主线，可以按下面顺序执行：

```text
parameter_optim -> rolling_optimize -> OOS TopN -> final_recommended_params -> fixed_params rolling -> 单次回测验证
```

## 能力总览

| 阶段 | 目标 | 标准入口 | 关键产物 |
| --- | --- | --- | --- |
| Phase 1 | 单次参数寻优，输出最优参数和报告 | `scripts/build/run_parameter_optim.sh` | `parameter_optim_report.json/.md`、`parameter_optim_best_params.yaml` |
| Phase 2 | 在 rolling window 中训练、测试并筛选更稳的参数 | `scripts/build/run_rolling_backtest.sh`、`oos_top10_validation_cli` | `rolling_optimize_report.json/.md`、`window_XXXX_best.yaml`、`oos_top10_validation.csv`、`final_recommended_params.yaml` |
| Phase 3 | 增强研究能力：热力图、约束过滤、随机搜索与复现 | `parameter_optim_cli`、`rolling_backtest_cli` | `heatmap_*.json`、约束过滤后的报告、固定 seed 的随机搜索结果 |

仓库中已经有一组可直接复用的配置和脚本：

- 单次参数优化配置：`configs/ops/parameter_optim.yaml`
- rolling optimize 配置：`configs/ops/rolling_optimize_kama.yaml`
- 固定参数 rolling 验证配置：`configs/ops/rolling_backtest.yaml`
- 回测验证脚本配置：`configs/ops/backtest_run.yaml`
- TopN 样本外验证 CLI：`build-gcc/oos_top10_validation_cli` 或 `build/oos_top10_validation_cli`

## 环境准备

### 1. 准备构建工具与依赖

```bash
cmake -S . -B build-gcc \
  -DQUANT_HFT_BUILD_TESTS=ON \
  -DQUANT_HFT_ENABLE_ARROW_PARQUET=ON \
  -DCMAKE_C_COMPILER=/usr/bin/gcc \
  -DCMAKE_CXX_COMPILER=/usr/bin/g++
```

### 2. 准备支持 Parquet 的构建目录

参数优化、rolling optimize 和 rolling testing 当前都依赖 parquet 数据链路。若构建时未开启 parquet 支持，trial 会直接失败。

建议在仓库根目录执行：

```bash
cmake -S . -B build-gcc \
  -DQUANT_HFT_BUILD_TESTS=ON \
  -DQUANT_HFT_ENABLE_ARROW_PARQUET=ON \
  -DCMAKE_C_COMPILER=/usr/bin/gcc \
  -DCMAKE_CXX_COMPILER=/usr/bin/g++

cmake --build build-gcc \
  --target backtest_cli parameter_optim_cli rolling_backtest_cli oos_top10_validation_cli \
  -j"$(nproc)"
```

如果你更习惯直接跑脚本，也可以先执行：

```bash
./scripts/build/bootstrap.sh
```

### 3. 数据与输出目录前提

- parquet 数据根目录默认使用 `backtest_data/parquet_v2`
- 建议把 rolling optimize 运行产物统一落到 `runtime/` 下
- `docs/results/` 更适合作为最终文档或手工整理后的报告目录，不建议作为 rolling optimize 的原始 runtime 输出目录

## 标准工作流

### 第一步：定义参数空间

基础参数优化使用 `configs/ops/parameter_optim.yaml`。这个文件包含四组核心配置：

1. `composite_config_path` 与 `target_sub_config_path`
2. `backtest_args`
3. `optimization`
4. `parameters`

当前 checked-in 示例使用的是离散网格搜索：

```yaml
optimization:
  algorithm: grid
  maximize: true
  metric_path: profit_factor
  max_trials: 48
  batch_size: 2
  preserve_top_k_trials: 10
  export_heatmap: true
```

参数空间示例：

```yaml
parameters:
  - name: kama_filter
    type: double
    values: [0.3, 0.5, 0.8, 1.2]
  - name: stop_loss_atr_multiplier
    type: double
    values: [3.0, 4.0, 5.0]
  - name: risk_per_trade_pct
    type: double
    values: [0.003, 0.005, 0.008, 0.01]
```

建议：

- 参数空间较小、需要全覆盖时，用 `grid`
- 参数空间较大、离散组合太多时，用 `random`
- 目标函数优先选你真正关心的样本外稳定指标，当前仓库常用 `profit_factor` 或 `hf_standard.risk_metrics.calmar_ratio`
- 如果你想保留后续复盘证据，设置 `preserve_top_k_trials > 0`

### 第二步：执行单次参数优化，先找候选最优参数

推荐直接使用包装脚本：

```bash
scripts/build/run_parameter_optim.sh \
  --build-dir build-gcc \
  --config configs/ops/parameter_optim.yaml
```

默认输出由 `optimization.output_json`、`optimization.output_md` 和 `optimization.best_params_yaml` 控制。当前示例会生成：

```text
docs/results/opts/
  parameter_optim_report.json
  parameter_optim_report.md
  parameter_optim_best_params.yaml
```

同时，如果 `preserve_top_k_trials` 大于 0，会在输出目录同级生成 `top_trials/` 归档；如果 `export_heatmap: true`，还会额外生成 `heatmap_*.json`。

你需要重点看四类信息：

1. `best_trial.params`：当前最优参数组合
2. `trials[]`：哪些 trial 成功，哪些 trial 失败
3. `convergence_curve`：目标值是否快速收敛，是否还值得扩展搜索空间
4. `top_trials/`：留作后续复盘和样本外再验证的候选参数集合

### 第三步：如需增强研究能力，打开 phase3 选项

### 1. 热力图导出

如果你想先看参数敏感性，而不急于立刻定参，可以在单次优化配置中打开：

```yaml
optimization:
  export_heatmap: true
```

系统会按参数对输出类似：

```text
heatmap_kama_filter_vs_stop_loss_atr_multiplier.json
heatmap_kama_filter_vs_risk_per_trade_pct.json
heatmap_stop_loss_atr_multiplier_vs_risk_per_trade_pct.json
```

热力图 JSON 会包含：

- `x_param`
- `y_param`
- `x_values`
- `y_values`
- `z_values`
- `objective`

适用场景：

- 判断局部最优是否只是尖峰
- 识别哪些参数维度对目标函数更敏感
- 在进入 rolling optimize 前，先收窄搜索区间

### 2. 显式约束 DSL

如果你不想只按单一目标函数排序，而是希望先过滤掉不合格试验，可以在 `optimization.constraints` 中加入约束，例如：

```yaml
optimization:
  constraints:
    - "profit_factor > 1.1"
```

当前仓库可参考：

- `configs/ops/parameter_optim_constraints_rolling_acceptance.yaml`
- `configs/ops/rolling_optimize_constraints_acceptance.yaml`

使用建议：

- 先用约束去掉明显不达标的组合
- 再用目标函数在剩余 trial 中选优
- 对过于严格的约束要谨慎，否则可能导致大量 `constraint_violated` 或没有有效候选

### 3. 随机搜索与复现

当参数空间较大时，可以切换到随机搜索：

```yaml
optimization:
  algorithm: random
  max_trials: 20
  random_seed: 20260420
```

配合范围型参数：

```yaml
parameters:
  - name: kama_filter
    type: double
    range: [0.2, 0.6]
    step: 0.1
```

当前仓库可直接参考：

- `configs/ops/parameter_optim_random_rolling_acceptance.yaml`
- `configs/ops/rolling_optimize_random_acceptance.yaml`
- `configs/ops/rolling_optimize_random_acceptance_replay.yaml`

使用建议：

- 用 `max_trials` 控制预算，不必穷举全空间
- 固定 `random_seed` 后，同一参数空间和同一 trial 数预算下可以复现抽样序列
- 如果要验证复现性，复制一份 rolling 配置，只改 `output.root_dir`，其余保持一致再跑一遍即可

### 第四步：执行 rolling optimize，验证跨窗口泛化能力

单次优化只能告诉你“这个参数在某个样本区间是否好用”，rolling optimize 才能告诉你“这个参数是否有跨时间窗口的一致性”。

标准入口：

```bash
scripts/build/run_rolling_backtest.sh \
  --build-dir build-gcc \
  --config configs/ops/rolling_optimize_kama.yaml
```

当前 `configs/ops/rolling_optimize_kama.yaml` 的关键结构是：

```yaml
mode: rolling_optimize

window:
  type: rolling
  train_length_days: 120
  test_length_days: 30
  step_days: 30

optimization:
  algorithm: grid
  objective:
    path: hf_standard.risk_metrics.calmar_ratio
    maximize: true
  param_space: ./parameter_optim.yaml

output:
  root_dir: ../../runtime/rolling_optimize_kama
```

注意这里的一个关键约束：

- rolling optimize 的 `output.root_dir` 是相对当前 rolling 配置文件目录解析的
- 当配置文件位于 `configs/ops/` 时，建议写成 `../../runtime/...`
- 这样输出会稳定落在仓库根目录的 `runtime/` 下，而不是误落到配置目录下面

典型输出结构：

```text
runtime/rolling_optimize_kama/
  rolling_optimize_report.json
  rolling_optimize_report.md
  train_reports/window_0000/parameter_optim_report.json
  train_reports/window_0000/parameter_optim_report.md
  best_params/window_0000_best.yaml
  test_results/window_0000/result.json
  top_trials/window_0000/
```

此时你要看的不是单个最佳 trial，而是整个窗口序列：

1. 每个窗口是否都能正常完成训练与测试
2. `best_params/window_XXXX_best.yaml` 是否在相邻窗口间剧烈跳变
3. `test_results/window_XXXX/result.json` 的 OOS 指标是否稳定
4. 是否出现大量 `objective=0`、无成交、失败或单窗口异常高分驱动总体结果

### 第五步：对 TopN 参数做样本外对比，并生成最终推荐参数

rolling optimize 会给出每个窗口的训练报告，但如果你希望在单个窗口里更系统地比较 TopN 候选参数的样本外表现，需要额外跑 `oos_top10_validation_cli`。

命令示例：

```bash
./build-gcc/oos_top10_validation_cli \
  --train-report-json runtime/rolling_optimize_kama/train_reports/window_0000/parameter_optim_report.json \
  --oos-start 2024-07-01 \
  --oos-end 2024-12-31 \
  --top-n 10 \
  --overwrite
```

如果你的 CLI 在 `build/` 下，也可以把命令中的 `build-gcc` 替换成 `build`。

输出目录默认落在：

```text
runtime/rolling_optimize_kama/oos_validation/window_0000/
  oos_top10_validation.csv
  final_recommended_params.yaml
  01_window_0_trial_x/result.json
  ...
```

这个阶段有两个关键结果：

1. `oos_top10_validation.csv`
   - 给出 TopN 候选的样本内/样本外对比表
   - 便于你判断“训练集最优”是否真能转化为“测试集也相对更优”
2. `final_recommended_params.yaml`
   - 系统基于 OOS 排名自动挑选最终推荐参数
   - 这是后续正式稳定性验证时最推荐使用的参数来源

注意：

- `Params` 列是带引号的 JSON，不能用简单的 `cut -d','` 解析
- 请使用 Python `csv`、pandas 或其他 CSV-aware 工具读取
- `--oos-start` 和 `--oos-end` 应与目标窗口的 test 区间一致，最稳妥的做法是从 rolling 报告中按窗口读取

### 第六步：把推荐参数落地到正式验证配置

`final_recommended_params.yaml` 是结果产物，不会自动注入你的正式策略配置。进入稳定性验证前，建议采用“复制一份验证专用主策略配置，再通过 overrides 覆盖参数”的方式，避免直接污染基线配置。

当前主策略配置 `configs/strategies/main_backtest_strategy.yaml` 已支持：

```yaml
composite:
  sub_strategies:
    - id: kama_trend_1
      overrides:
        backtest:
          params:
            kama_filter: 0.3
            risk_per_trade_pct: 0.01
            stop_loss_atr_multiplier: 3.0
```

推荐做法：

1. 复制一份 `configs/strategies/main_backtest_strategy.yaml` 作为验证专用配置
2. 在目标子策略下添加 `overrides.backtest.params`
3. 将 `final_recommended_params.yaml` 中的参数值填进去
4. 让 rolling testing 或 backtest run 配置指向这份验证专用主策略配置

这样有两个好处：

- 不需要改动原始 `configs/strategies/sub/kama_trend_1.yaml`
- 可以并行维护多份候选参数验证配置

### 第七步：做参数运行稳定性验证

参数稳定性至少建议做两层验证。

### 1. 固定参数 rolling testing

先在多窗口上验证这个推荐参数是否持续可用。当前标准配置是 `configs/ops/rolling_backtest.yaml`，模式为 `fixed_params`。

建议复制一份 rolling 配置，例如 `rolling_backtest_candidate.yaml`，把其中的 `backtest_base.strategy_composite_config` 指向你的验证专用主策略配置，然后执行：

```bash
scripts/build/run_rolling_backtest.sh \
  --build-dir build-gcc \
  --config configs/ops/rolling_backtest_candidate.yaml
```

如果你是直接在 `configs/ops/rolling_backtest.yaml` 上修改，也可以把命令中的配置路径替换回原文件。运行后重点看：

- 各窗口 success 是否连续稳定
- test window 的目标函数是否明显退化
- 是否存在少数窗口贡献了几乎全部收益，而大多数窗口接近 0 或负值
- 最大回撤、成交笔数、胜率是否在可接受范围内

### 2. 单次全周期回测 + 检测报告

rolling 结果稳定后，再做一次正式回测与检测报告，确认该参数组合在完整运行链路中没有异常输出、无效报表或明显结构性风险。

建议复制 `configs/ops/backtest_run.yaml` 为验证专用版本，并把 `strategy_main_config_path` 指向你带有参数覆盖的主策略配置，然后执行：

```bash
bash scripts/build/run_backtest_with_validation.sh \
  --config configs/ops/backtest_run_candidate.yaml
```

如果只是快速试跑，也可以加 `--fast`：

```bash
bash scripts/build/run_backtest_with_validation.sh \
  --config configs/ops/backtest_run_candidate.yaml \
  --fast \
  --fast-start-date 20240101 \
  --fast-end-date 20240331 \
  --fast-max-ticks 20000
```

这一步会在 `output_root_dir` 下生成新的 run 目录，并输出：

- `validation_report.md`
- 正式中文分析报告
- 对应 JSON/Markdown 回测结果

如果一个参数组合经过了：

1. 单次寻优表现靠前
2. rolling optimize 多窗口不失真
3. TopN OOS 对比仍然靠前
4. fixed_params rolling 表现稳定
5. 单次正式回测验证无明显异常

那么它才更接近“可进入后续研究/实盘前评估”的候选参数，而不是一次偶然命中的最优点。

## 推荐的端到端执行顺序

实际研究中，建议按下面的节奏推进：

1. 先用 `parameter_optim.yaml` 跑单次寻优，确认搜索空间和目标函数没有明显问题。
2. 若参数空间过大，先开 `random`；若想先看敏感性，开 `export_heatmap`。
3. 若有业务底线，尽早加入 `constraints`，避免把不合格组合带入后续阶段。
4. 用 `rolling_optimize_kama.yaml` 做 rolling optimize，看跨窗口泛化。
5. 对关键窗口跑 `oos_top10_validation_cli`，生成 `final_recommended_params.yaml`。
6. 把推荐参数通过 `overrides.backtest.params` 落到验证专用主策略配置。
7. 用 `fixed_params` rolling testing 先看稳定性，再用 `run_backtest_with_validation.sh` 看完整运行质量。

## 常见问题

### 1. trial 全失败，报告里只有 failed

优先检查构建是否启用了 parquet：

- CMake 需要 `-DQUANT_HFT_ENABLE_ARROW_PARQUET=ON`
- 如果 stderr 出现 “built without Arrow/Parquet support”，说明当前二进制不能跑 parquet 数据

### 2. rolling optimize 输出写到了奇怪的目录

优先检查 `output.root_dir` 是否写成了相对 `configs/ops/` 的路径。对位于 `configs/ops/` 的 rolling 配置，推荐写法是：

```yaml
output:
  root_dir: ../../runtime/your_run_name
```

### 3. 推荐参数已经生成，但不知道怎么正式验证

最稳妥的方式不是直接改子策略 YAML，而是复制主策略配置后，通过 `overrides.backtest.params` 注入参数，再把 rolling/backtest 配置指向那份验证专用主策略配置。

### 4. 想确认随机搜索是否可复现

保持以下条件完全一致：

- `random_seed`
- 参数空间定义
- `max_trials`
- 搜索算法配置

然后只修改输出目录，重跑一次即可比较 trial 序列和结果。

## 参考配置与产物

- 单次寻优配置：`configs/ops/parameter_optim.yaml`
- rolling optimize 配置：`configs/ops/rolling_optimize_kama.yaml`
- 固定参数 rolling 配置：`configs/ops/rolling_backtest.yaml`
- 回测验证配置：`configs/ops/backtest_run.yaml`
- 约束示例：`configs/ops/parameter_optim_constraints_rolling_acceptance.yaml`
- 随机搜索示例：`configs/ops/parameter_optim_random_rolling_acceptance.yaml`
- 已有验收文档：
  - `docs/results/opts/rolling_optimize_step2_1_acceptance.md`
  - `docs/results/opts/rolling_optimize_step2_2_acceptance.md`
  - `docs/results/opts/rolling_optimize_step2_3_acceptance.md`
  - `docs/results/opts/rolling_optimize_step3_1_acceptance.md`

如果你要在当前仓库里复用已有流程，最小可执行组合就是：

1. `configs/ops/parameter_optim.yaml`
2. `configs/ops/rolling_optimize_kama.yaml`
3. `configs/strategies/main_backtest_strategy.yaml`
4. `configs/ops/backtest_run.yaml`

把这四类文件理解透，再结合 TopN OOS 和 overrides 覆盖参数，整条“找最优参数到验证稳定性”的链路就能稳定跑通。
