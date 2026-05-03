# 参数优化 Skill：寻找稳健最优参数组合

本 Skill 用于指导在现有 quant_hft 框架内寻找、验证并部署策略参数组合。执行时必须先生成本轮优化计划，再按计划执行；每次参数优化都必须显式声明数据集时间范围、优化品种、使用策略、搜索空间、目标函数、约束、并发预算和输出目录。

本 Skill 将“幻方量化标准”落实为四条可检查原则：可复现、防过拟合、输出透明、自动化程度高。任何参数组合只有同时通过单次优化、Walk-Forward、样本外验证和部署前回测检查，才可进入后续研究或实盘前评估。

## 1. 概述与前置条件

### 适用策略与代码依据

本 Skill 当前面向 KAMA 趋势策略及其组合层封装：

- 原子策略：`KamaTrendStrategy`，实现位于 [src/strategy/atomic/kama_trend_strategy.cpp](../../src/strategy/atomic/kama_trend_strategy.cpp)，头文件位于 [include/quant_hft/strategy/atomic/kama_trend_strategy.h](../../include/quant_hft/strategy/atomic/kama_trend_strategy.h)。该实现读取并校验 `er_period`、`fast_period`、`slow_period`、`std_period`、`kama_filter`、`risk_per_trade_pct`、`stop_loss_atr_multiplier` 等参数。
- 组合层：`CompositeStrategy`，实现位于 [src/strategy/composite_strategy.cpp](../../src/strategy/composite_strategy.cpp)，头文件位于 [include/quant_hft/strategy/composite_strategy.h](../../include/quant_hft/strategy/composite_strategy.h)。组合层负责子策略配置、参数覆盖、风险预算和交易信号聚合。
- 默认主策略配置：参考 [configs/strategies/main_backtest_strategy.yaml](../../configs/strategies/main_backtest_strategy.yaml)。KAMA 子策略基线配置参考 [configs/strategies/sub/kama_trend_production.yaml](../../configs/strategies/sub/kama_trend_production.yaml)。

### 必要二进制与入口

CLI 目标由 [CMakeLists.txt](../../CMakeLists.txt) 定义，参数优化流程至少需要以下二进制：

- `parameter_optim_cli`：单次参数优化入口，源码为 [src/apps/parameter_optim_cli_main.cpp](../../src/apps/parameter_optim_cli_main.cpp)。
- `rolling_backtest_cli`：仓库中 rolling optimize / rolling backtest 的统一入口，源码为 [src/apps/rolling_backtest_cli_main.cpp](../../src/apps/rolling_backtest_cli_main.cpp)。如文档或需求中提到 `rolling_optimize_cli`，在本仓库内应映射为 `rolling_backtest_cli`。
- `backtest_cli`：基础回测入口，源码为 [src/apps/backtest_cli_main.cpp](../../src/apps/backtest_cli_main.cpp)。
- `oos_top10_validation_cli`：TopN 参数样本外验证入口，源码为 [src/apps/oos_top10_validation_cli_main.cpp](../../src/apps/oos_top10_validation_cli_main.cpp)。

推荐优先使用包装脚本，以便自动补齐构建和运行环境：

- [scripts/build/run_parameter_optim.sh](../../scripts/build/run_parameter_optim.sh)
- [scripts/build/run_rolling_backtest.sh](../../scripts/build/run_rolling_backtest.sh)
- [scripts/build/run_backtest_with_validation.sh](../../scripts/build/run_backtest_with_validation.sh)

### 数据要求

首选数据格式为 Parquet v2：

- 默认数据根目录为 `backtest_data/parquet_v2`，单次优化示例见 [configs/ops/parameter_optim.yaml](../../configs/ops/parameter_optim.yaml)。
- 多品种多日数据通过 `_manifest/partitions.jsonl` 描述。rolling 配置会在未显式指定 `dataset_manifest` 时由 `dataset_root/_manifest/partitions.jsonl` 推导，相关逻辑位于 [src/rolling/rolling_config.cpp](../../src/rolling/rolling_config.cpp) 和 [src/rolling/window_generator.cpp](../../src/rolling/window_generator.cpp)。
- 数据必须覆盖本轮优化、验证、测试和 OOS 区间。研究数据应覆盖至少 9 个品种，以便后续扩展横截面稳健性检验；当前 checked-in 参数优化配置只优化 `c` 品种，见 [configs/ops/parameter_optim.yaml](../../configs/ops/parameter_optim.yaml)。

CSV tick 数据仅作为备用路径。Parquet 数据链路由 [src/core/backtest/parquet_data_feed.cpp](../../src/core/backtest/parquet_data_feed.cpp) 支持，文件中也保留 CSV 行解析逻辑；正式优化仍应优先使用 Parquet v2 并开启 `strict_parquet: true`。

### 环境要求

- 编译器：GCC >= 11 或 Clang >= 14。
- 构建工具：CMake >= 3.20。
- Parquet 依赖：Arrow/Parquet 23.0.1；建议构建时启用 `-DQUANT_HFT_ENABLE_ARROW_PARQUET=ON`。
- Python：Python 3.12+ 用于后续 CSV/JSON 分析、可视化和复盘，非 CLI 运行的硬性前置。
- 推荐构建目录：`build-gcc`。若 WSL 或混合工具链出现问题，使用 GCC 隔离构建目录。

最小构建命令：

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

## 2. 参数优化空间定义

### 每轮优化必须先写计划

每次运行参数优化前，先记录以下基础信息：

- `run_id`：本轮优化唯一标识，建议包含策略、品种、训练区间和日期，例如 `kama_c_20230103_20251101_pf_grid_v1`。
- 数据集：`engine_mode`、`dataset_root`、`dataset_manifest`、`start_date`、`end_date`、`max_ticks`、是否 `strict_parquet`。
- 合约换月口径：rolling / OOS 验证前必须声明如何识别合约切换，默认用 Parquet manifest 的 `instrument_id` 序列并交叉检查 `contract_expiry_calendar_path`。
- 优化品种：当前默认只优化 `c`，如扩展多品种，应在 `symbols` 中显式列出并检查每个品种覆盖区间。
- 策略：`composite_config_path`、`target_sub_config_path`、目标子策略 ID 或目标子配置。
- 搜索空间：参数名、类型、取值集合或范围、业务约束。
- 目标函数：指标路径、是否最大化、是否使用加权复合目标。
- 约束：最小成交数、收益风险下限、最大回撤上限等 DSL 约束。
- 并发预算：`batch_size` 或 `parallel`、机器内存、每 trial 预算。
- 输出目录：JSON、Markdown、best params、top trials、heatmap、rolling runtime。

### 配置文件字段

单次优化主配置为 [configs/ops/parameter_optim.yaml](../../configs/ops/parameter_optim.yaml)。当前仓库单次优化实际使用 `parameters` 作为参数空间字段；rolling optimize 使用 `optimization.param_space` 指向该参数空间配置。若团队内部习惯称为 `param_space`，在本 Skill 中它是概念名，落到当前单次优化 YAML 时对应 `parameters`。

关键字段含义：

- `composite_config_path`：组合策略主配置，当前为 `configs/strategies/main_backtest_strategy.yaml`。
- `target_sub_config_path`：被优化的子策略配置路径，当前为 `./sub/kama_trend_production.yaml`。
- `backtest_args`：传给 `backtest_cli` 的回测参数，包括数据、品种、日期、撮合、换月和导出选项。
- `optimization.algorithm`：搜索算法，常用 `grid` 或 `random`。
- `optimization.metric_path`：单指标目标路径，例如 `profit_factor`。
- `optimization.objective`：复合或结构化目标配置；rolling 示例使用 `objective.path`。
- `optimization.maximize`：目标是否越大越好。
- `optimization.max_trials`：最大 trial 数，控制研究预算。
- `optimization.batch_size`：单次优化调度批大小，当前默认示例为 `2`。
- `optimization.preserve_top_k_trials`：保留前 K 个 trial 的回测产物，供复盘和 OOS 验证。
- `optimization.export_heatmap`：导出参数两两热力图，用于检查局部尖峰和参数敏感性。
- `optimization.output_json`、`optimization.output_md`、`optimization.best_params_yaml`：报告和最优参数输出路径。
- `optimization.constraints`：约束 DSL，例如 `profit_factor > 1.3`。
- `parameters`：待优化参数列表。每个参数至少包含 `name`、`type`，并通过 `values` 或 `range` 定义搜索空间。

### 参数类型与约束

参数空间解析逻辑位于 [src/optim/parameter_space.cpp](../../src/optim/parameter_space.cpp)。当前应按以下方式定义参数：

- `double`：浮点参数，可使用离散 `values`，也可使用 `range` 与 `step` 生成候选值。
- `int`：整数参数，适合周期类参数，例如 `er_period`。
- `categorical`：枚举参数，适合模式开关或字符串类别。

当前优化空间只包含三项核心参数：

- `kama_filter`：KAMA 趋势过滤阈值，约束为非负，策略实现中要求 `kama_filter >= 0`。
- `stop_loss_atr_multiplier`：ATR 止损倍数，约束为正数，策略实现中要求 `stop_loss_atr_multiplier > 0`。
- `risk_per_trade_pct`：单笔风险比例，约束为 `(0, 1]`，组合层和原子策略都会使用该值计算风险预算。

可以添加其他合规参数，但必须保留策略实现中的约束：

- `er_period > 0`
- `fast_period > 0`
- `slow_period > 0`
- `std_period > 0`
- 周期类参数不应盲目放大搜索空间；新增参数会增加过拟合自由度，必须同步降低其他维度或增加 OOS 检验。

### 目标函数

单指标目标示例：

```yaml
optimization:
  algorithm: grid
  maximize: true
  metric_path: profit_factor
```

`profit_factor` 会由 [src/optim/result_analyzer.cpp](../../src/optim/result_analyzer.cpp) 中的指标别名解析逻辑映射到标准报告路径。目标值从回测 JSON 中提取，入口包括 `ComputeObjectiveFromJson` 和 `ComputeObjectiveFromJsonText`。

复合目标用于降低“单一指标最优但风险形态很差”的概率。`ResultAnalyzer` 支持 `OptimizationObjective` 列表，并在报告中将复合目标标记为 `weighted_objective`。示例：

```yaml
optimization:
  algorithm: grid
  maximize: true
  objectives:
    - path: hf_standard.advanced_summary.profit_factor
      weight: 0.50
      maximize: true
    - path: hf_standard.risk_metrics.calmar_ratio
      weight: 0.35
      maximize: true
    - path: hf_standard.risk_metrics.max_drawdown_pct
      weight: 0.15
      maximize: false
```

使用复合目标时，必须在优化计划中写清每个指标的业务含义、权重方向和是否需要按初始权益缩放。不要在看到结果后再改权重。

### 综合得分与筛选阶段

参数优化的最终目标并非输出“一个最佳参数”，而是产出一组经过充分检验的稳健候选。为此，流程被严格划分为两个阶段：研发筛选与最终决策。

#### 研发筛选阶段（可以使用综合得分）

该阶段目标为剔除明显劣质组合，输出 TopN 候选参数。允许使用多维综合得分，但必须遵守以下规则：

- 综合得分权重必须在优化计划中事先声明，不得在事后根据结果调整。
- 得分至少覆盖以下维度：
  - 收益质量：`profit_factor`、总 PnL、平均窗口 PnL。
  - 风险质量：Calmar、最大回撤、最大单窗口亏损。
  - 稳定性：正收益窗口数、PF > 1 窗口数、Calmar > 0 窗口数、窗口间指标标准差。
  - 交易质量：成交数、无成交窗口、低成交伪高分。
  - 参数质量：TopN 参数聚集程度、相邻 rolling 窗口参数是否剧烈跳变。
- 综合得分仅用于筛选 TopN，不得作为最终决策依据。

#### 最终决策阶段（必须使用单一硬性指标）

进入最终验证（固定参数 rolling、全周期回测）前，必须声明一个单一、客观、事先固定的硬性决策指标。该指标从通过所有验证环节的 TopN 候选参数中，以无偏的方式选出最终部署参数。

示例声明：

> 最终参数将选择在全周期 Walk-Forward 测试集上 `calmar_ratio` 最高的候选组合。

该决策指标一经声明不得更改，也不得参考其他未事先列出的指标。OOS 和最终测试结果只可以否决候选参数，不得反向调整决策指标。

### 约束 DSL

约束用于先过滤不合格 trial，再在剩余 trial 中按目标函数排序。当前不再保留专用 acceptance YAML；需要验证时，从 [configs/ops/parameter_optim.yaml](../../configs/ops/parameter_optim.yaml) 复制运行配置并加入约束：

```yaml
optimization:
  constraints:
    - "profit_factor > 1.3"
```

建议常用约束包括：

- `profit_factor > 1.3`：过滤收益质量不足的组合。
- `hf_standard.trade_statistics.total_trades > 30`：过滤低成交或无成交伪高分。
- `hf_standard.risk_metrics.max_drawdown_pct < 20`：过滤不可接受回撤。

如果大量 trial 变为 `constraint_violated`，先检查约束是否过严，再检查数据区间和搜索空间是否过窄。

### data_period 与样本切分

当前单次优化配置没有独立 `data_period` 顶层字段，训练区间由 `backtest_args.start_date` 和 `backtest_args.end_date` 控制。单次优化只代表该区间内的结果；如需训练、验证、测试分离，应复制配置文件并手动划分数据集，或使用 rolling 模式。

推荐约定：

- `train`：用于参数搜索。
- `validation`：用于筛选 TopN 和调参决策，不参与最终报告夸大。
- `test` / `OOS`：参数冻结后才使用。
- Walk-Forward：由 rolling 配置自动生成多个 train/test 窗口。

### batch_size 与并发

[configs/ops/parameter_optim.yaml](../../configs/ops/parameter_optim.yaml) 默认 `batch_size: 2`。单次优化 CLI 中的 `SafeMaxConcurrent` 位于 [src/apps/parameter_optim_cli_main.cpp](../../src/apps/parameter_optim_cli_main.cpp)，rolling 中对应逻辑位于 [src/rolling/rolling_runner.cpp](../../src/rolling/rolling_runner.cpp)。并发上限会同时考虑 CPU 和内存，单 trial 内存预算为 1536 MB，并保留系统内存余量。

执行原则：

- 默认使用 `batch_size: 2` 或 rolling 中的 `parallel: 2`。
- 单机内存不足、trial 失败或系统换页时，先降到 `1`。
- 不要仅为了加速把并发提高到 4 以上；`SafeMaxConcurrent` 会限制并发，但配置仍应反映真实预算。

### 完整配置示例

以下片段来自 [configs/ops/parameter_optim.yaml](../../configs/ops/parameter_optim.yaml)，并补充注释说明。实际运行时可以复制为新文件，例如 `configs/ops/parameter_optim_kama_c_2024.yaml`，再修改日期、品种、搜索空间和输出路径。

```yaml
# 单次参数优化：KAMA T0 baseline
composite_config_path: configs/strategies/main_backtest_strategy.yaml
target_sub_config_path: ./sub/kama_trend_production.yaml

backtest_args:
  engine_mode: parquet
  dataset_root: backtest_data/parquet_v2
  detector_config: configs/sim/ctp.yaml
  product_config_path: configs/strategies/instrument_info.json
  contract_expiry_calendar_path: configs/strategies/contract_expiry_calendar.yaml
  symbols: c
  start_date: 20230103
  end_date: 20251101
  max_ticks: 200000
  deterministic_fills: true
  strict_parquet: true
  rollover_mode: expiry_close
  rollover_price_mode: bbo
  rollover_slippage_bps: 0
  emit_trades: true
  emit_orders: false
  emit_position_history: false

optimization:
  algorithm: grid
  maximize: true
  metric_path: profit_factor
  max_trials: 48
  batch_size: 2
  preserve_top_k_trials: 10
  export_heatmap: true
  output_json: docs/results/opts/parameter_optim_report.json
  output_md: docs/results/opts/parameter_optim_report.md
  best_params_yaml: docs/results/opts/parameter_optim_best_params.yaml

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

## 3. 单次参数优化流程

### 执行前检查

运行 `parameter_optim_cli` 前，必须确认：

- 本轮优化计划已经写明 `run_id`、数据区间、品种、策略、参数空间、目标函数、约束和输出目录。
- 构建目录存在，且 `backtest_cli` 和 `parameter_optim_cli` 已构建。
- `dataset_root/_manifest/partitions.jsonl` 存在，且覆盖 `start_date` 到 `end_date`。
- `emit_trades: true` 已开启。`profit_factor` 是交易派生指标，没有交易明细时报告可能不可用或失真。
- 输出目录不覆盖重要历史结果。正式研究建议每轮使用独立目录，例如 `docs/results/opts/kama_c_2024_pf_grid_v1/`。

### 推荐命令

优先使用包装脚本：

```bash
scripts/build/run_parameter_optim.sh \
  --build-dir build-gcc \
  --config configs/ops/parameter_optim.yaml
```

直接运行二进制：

```bash
./build-gcc/parameter_optim_cli \
  --config configs/ops/parameter_optim.yaml \
  --backtest-cli-path ./build-gcc/backtest_cli
```

### 输出位置

输出由 `optimization.output_json`、`optimization.output_md` 和 `optimization.best_params_yaml` 控制。当前默认配置会写入 `docs/results/opts/`：

```text
docs/results/opts/
  parameter_optim_report.json
  parameter_optim_report.md
  parameter_optim_best_params.yaml
  top_trials/
  heatmap_*.json
```

其中：

- `parameter_optim_report.json`：机器可读报告，保留 trial 状态、参数、目标值、约束状态、错误信息和摘要。
- `parameter_optim_report.md`：人工复盘报告。
- `parameter_optim_best_params.yaml`：当前区间内目标函数最优且满足约束的参数组合（仅作为候选，不可直接部署）。
- `top_trials/`：当 `preserve_top_k_trials > 0` 时保存前 K 个 trial 的回测产物。
- `heatmap_*.json`：当 `export_heatmap: true` 时导出参数两两热力图。

### 报告解读

单次优化只用于产生候选参数，不可直接视为最终参数。复盘报告时按以下顺序检查：

1. `best_trial.params` 是否全部来自预先声明的搜索空间。
2. `best_trial.objective` 是否由指定 `metric_path` 或 `weighted_objective` 得到。
3. completed / failed / constraint_violated trial 数量是否合理。
4. TopN 参数是否聚集在稳定区域，而不是某个孤立尖峰。
5. `top_trials/` 中的回测结果是否能追溯到对应 trial 和 backtest run。
6. 成交数、回撤、收益来源是否正常；若目标值高但成交极少，应进入 OOS 前先淘汰。

如报告中有 `run_id`、输出路径或 trial 目录，应在研究记录中把它们与本轮优化计划对应起来，确保后续 OOS 和部署验证可追溯。

## 4. 滚动优化（Walk-Forward）流程

### 配置文件

Walk-Forward 使用 [configs/ops/rolling_optimize_kama.yaml](../../configs/ops/rolling_optimize_kama.yaml)。关键字段：

- `mode: rolling_optimize`：启用滚动训练 + 样本外测试模式。
- `backtest_base`：基础回测参数，包括 `dataset_root`、`symbols`、`strategy_factory`、`strategy_composite_config`、合约信息、撮合和导出选项。
- `window.train_length_days`：每个窗口训练长度，当前为 `120`。
- `window.test_length_days`：每个窗口测试长度，默认且最低为 `60`。除非用户明确批准并写明原因，rolling / Walk-Forward / fixed rolling 的评估窗口不得低于 60 个交易日。
- `window.step_days`：窗口前进步长，当前为 `30`。
- `window.start_date` / `window.end_date`：滚动研究总区间。
- `optimization.objective.path`：rolling 阶段目标函数，当前示例为 `hf_standard.risk_metrics.calmar_ratio`。
- `optimization.param_space`：参数空间来源，当前为 `./parameter_optim.yaml`，即复用 [configs/ops/parameter_optim.yaml](../../configs/ops/parameter_optim.yaml)。
- `optimization.parallel`：窗口内 trial 并发，当前为 `2`。
- `output.root_dir`：rolling 结果根目录。该路径相对配置文件目录解析；配置位于 `configs/ops/` 时，`../../runtime/rolling_optimize_kama` 会落到仓库根目录的 `runtime/rolling_optimize_kama`。
- `output.window_parallel`：窗口级并发，当前为 `1`。

### 执行命令

推荐使用包装脚本：

```bash
scripts/build/run_rolling_backtest.sh \
  --build-dir build-gcc \
  --config configs/ops/rolling_optimize_kama.yaml
```

直接运行二进制：

```bash
./build-gcc/rolling_backtest_cli \
  --config configs/ops/rolling_optimize_kama.yaml
```

### 输出结构

典型输出位于 `runtime/rolling_optimize_kama/`：

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

重点检查：

- 每个窗口是否成功完成训练和测试。
- 每个 rolling / Walk-Forward / fixed rolling 的 test 窗口长度是否至少 60 个交易日；若因数据不足低于 60，必须在计划和报告中标记为例外，并不得作为最终推荐的核心证据。
- 每个 test / OOS 窗口是否只包含单一合约；若 manifest 中同一窗口出现多个 `instrument_id`，该窗口为换月污染窗口，不得用于候选排名。
- `best_params/window_XXXX_best.yaml` 是否在相邻窗口间剧烈跳变。
- `test_results/window_XXXX/result.json` 的 OOS 指标是否稳定。
- 是否存在大量无成交、`objective=0`、失败 trial 或由单一窗口贡献大部分收益的情况。
- train 最优参数在 test 窗口中是否明显退化。

Walk-Forward 结论优先级高于单次优化。若单次优化最优参数在 rolling 中不稳定，应回到搜索空间、目标函数或约束设计阶段，而不是直接部署。

### 合约换月审计

rolling 分窗必须考虑合约换月。默认审计方法：

1. 从 `dataset_manifest` 读取目标品种在每个交易日对应的 `instrument_id`。
2. 对每个 rolling test / OOS 窗口列出合约序列，例如 `20230816:rb2310 -> 20230901:rb2401`。
3. 用 `contract_expiry_calendar_path` 中的到期日交叉确认切换位置。
4. 如果同一个 test / OOS 窗口包含多个 `instrument_id`，该窗口判定为换月污染窗口。
5. 换月污染窗口不能作为候选参数排名、综合得分或最终推荐依据；应重新生成单合约窗口，或在报告中单独列出并剔除。

训练窗口如因较长训练期跨合约，必须在计划和报告中说明这是训练样本设计的一部分；但评估窗口、OOS 窗口和固定参数 rolling 测试窗口默认必须保持单合约。若用户明确要求所有 rolling 窗口都不得换月，则训练窗口也必须按同一规则过滤或重分。

## 5. 过拟合防范与样本外验证

### 防过拟合规则

执行参数优化时必须遵守以下规则：

1. 先写计划，后跑任务。不得根据已看到的结果反向修改目标函数、权重、搜索空间或约束。
2. 搜索空间要小而有理据。新增一个参数维度，就要说明它的策略含义、合法范围和为什么不会制造无意义自由度。
3. 保留样本外。最终 OOS 区间不能参与搜索空间设计、目标函数调整和约束调试。
4. 优先稳定性，不优先单点最高分。rolling 多窗口稳定优于单次区间最高 `profit_factor`。
5. 检查成交质量。无成交、极低成交、单笔异常盈利、单窗口异常高分都不能作为部署依据。
6. 使用热力图检查尖峰。若最优点周围参数组合明显崩塌，应视为高过拟合风险。
7. 固定随机种子。使用 `random` 搜索时必须设置并记录 `random_seed`。
8. 保留证据。JSON、Markdown、best params、TopN trial 和最终推荐参数必须可追溯到同一轮计划。
9. 剔除换月污染窗口。rolling / OOS 评估窗口若包含多个合约，不能进入综合得分或最终排名。
10. 严格分离筛选与决策。综合得分仅用于筛选 TopN；最终部署参数必须由事先声明的单一硬性指标决定。

### TopN 样本外验证

rolling optimize 会生成每个窗口的训练报告。若要对某个窗口的 TopN 候选参数做更系统的样本外对比，使用 `oos_top10_validation_cli`。当前仓库的 OOS TopN 验证入口是命令行参数，而不是独立 YAML 配置。

示例：

```bash
./build-gcc/oos_top10_validation_cli \
  --train-report-json runtime/rolling_optimize_kama/train_reports/window_0000/parameter_optim_report.json \
  --oos-start 2024-07-01 \
  --oos-end 2024-12-31 \
  --top-n 10 \
  --output-dir runtime/rolling_optimize_kama/oos_validation/window_0000 \
  --overwrite
```

输出：

```text
runtime/rolling_optimize_kama/oos_validation/window_0000/
  oos_top10_validation.csv
  oos_validated_candidates.yaml
  01_window_0_trial_x/result.json
```

解读原则：

- `oos_top10_validation.csv` 是 TopN 参数的样本内/样本外对比表，应使用 Python `csv`、pandas 或其他 CSV-aware 工具读取，不要用简单分隔符硬切。
- `oos_validated_candidates.yaml` 是通过 OOS 验证的候选参数列表，但不等于最终部署参数。它仍需经过固定参数 rolling 和全周期回测，并由事先声明的单一硬性决策指标选出最终组合。
- 若训练集第一名在 OOS 中明显退化，而第二梯队更稳定，应优先保留 OOS 稳健组合为候选。
- 若 TopN 在 OOS 中整体退化，说明参数空间或目标函数可能只适合训练区间，应重新设计。

### 复现要求

每轮优化必须能被另一个人复跑：

- Git commit 或工作树状态可追踪。
- 配置文件路径和内容固定。
- 数据集根目录、manifest、日期和品种固定。
- `random_seed` 固定，或声明使用全量 grid。
- 输出目录独立，历史结果不被覆盖。
- 报告文件和最终参数文件完整保存。

## 6. 结果解读与部署

### 晋级与决策流程

参数组合必须按以下顺序晋级，并严格分离“筛选”与“决策”：

#### 阶段一：研发筛选（产生 TopN 候选）

1. 单次参数优化：进入 TopN，且满足所有约束。
2. Walk-Forward：在多个窗口训练和测试中表现稳定，没有明显参数跳变。
3. TopN 样本外验证：通过 OOS 检验，确认不是低成交伪高分，产出 `oos_validated_candidates.yaml`。

此阶段可使用事先声明的综合得分辅助筛选，但综合得分仅用于剔除劣质组合、保留 TopN，不得作为最终决策依据。

#### 阶段二：最终决策（选出唯一部署参数）

1. 声明单一硬性决策指标：在进入固定参数滚动验证之前，必须公开声明一个无偏的、客观的决策指标（例如“在全周期 Walk-Forward 测试集上 Calmar 比率最高的候选”）。该指标一经声明不得更改。
2. 固定参数 rolling 验证：对候选参数执行固定参数滚动回测，检查各窗口成功率、OOS 指标、成交和回撤分布。
3. 全周期回测与验证报告：对候选参数执行全周期回测，确认无结构性异常。
4. 最终选择：根据事先声明的单一硬性决策指标，从通过所有验证的候选中选择唯一部署参数。

只有完成以上全部步骤，参数才可进入后续研究或实盘前评估。单次优化输出的 `parameter_optim_best_params.yaml` 不得直接作为生产参数。

### 参数落地方式

推荐不要直接修改 [configs/strategies/sub/kama_trend_production.yaml](../../configs/strategies/sub/kama_trend_production.yaml)。更稳妥的方式是复制 [configs/strategies/main_backtest_strategy.yaml](../../configs/strategies/main_backtest_strategy.yaml) 为验证专用主策略配置，再通过组合层 overrides 注入参数：

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

这样可以并行维护多份候选参数配置，并保持生产子策略基线清晰。

### 固定参数 rolling 验证

参考 [configs/backtest/rolling_backtest.yaml](../../configs/backtest/rolling_backtest.yaml)，复制一份候选验证配置，并将 `backtest_base.strategy_composite_config` 指向带 overrides 的验证专用主策略配置。执行：

```bash
scripts/build/run_rolling_backtest.sh \
  --build-dir build-gcc \
  --config configs/backtest/rolling_backtest_candidate.yaml
```

检查各窗口 success、OOS 指标、成交笔数、回撤、收益分布和窗口间稳定性。

固定参数 rolling 报告必须附带换月审计结论：列出每个 test 窗口的 `instrument_id` 序列，标记并剔除换月污染窗口，再基于剩余有效窗口计算决策指标。

### 全周期回测与验证报告

参考 [configs/backtest/backtest_run.yaml](../../configs/backtest/backtest_run.yaml)，复制为候选配置并指向验证专用主策略配置。执行：

```bash
bash scripts/build/run_backtest_with_validation.sh \
  --config configs/backtest/backtest_run_candidate.yaml
```

快速试跑可使用：

```bash
bash scripts/build/run_backtest_with_validation.sh \
  --config configs/backtest/backtest_run_candidate.yaml \
  --fast \
  --fast-start-date 20240101 \
  --fast-end-date 20240331 \
  --fast-max-ticks 20000
```

最终部署评审至少需要保存：

- 本轮优化计划。
- 单次优化 JSON/Markdown 报告。
- rolling optimize JSON/Markdown 报告。
- OOS TopN CSV 和 `oos_validated_candidates.yaml`。
- 固定参数 rolling 报告（含换月审计）。
- 全周期回测验证报告。
- 单一硬性决策指标的完整计算过程与最终得分。
- 采用参数的配置 diff。

## 7. 部署前最终检查

在将参数推入实盘或模拟交易前，必须逐项确认以下清单。任何一项未通过，参数不得进入生产环境。

### 必须归档的产物

- 本轮优化计划（含 `run_id`、时间窗口、品种、策略、搜索空间、目标函数、约束、综合得分规则、单一决策指标声明）。
- 单次优化报告（JSON + Markdown）。
- Walk-Forward 报告（JSON + Markdown），包含换月审计结论。
- OOS 验证 CSV 与 `oos_validated_candidates.yaml`。
- 固定参数 rolling 报告（含换月审计）。
- 全周期回测验证报告。
- 单一决策指标的计算过程、候选排名与最终选择理由。
- 最终参数配置 diff（与生产基线对比）。

### 过程合规性

- 搜索空间、目标函数、约束和综合得分权重在实验前已锁定，未根据结果调整。
- OOS 区间从未参与搜索、约束调试或综合得分设计。
- 随机种子已固定（若适用）。
- 换月污染窗口已从评估中剔除。
- 最终决策严格使用事先声明的单一硬性指标，未参考其他未声明指标。
- 没有在任何步骤将单次优化 best_params 误认为最终参数。

### 绩效与稳健性

- 候选参数在滚动窗口间未出现剧烈跳变。
- 全周期回测中无结构性异常（如单笔交易贡献大部分利润、某特定时段持续亏损）。
- 成交笔数充足，非低成交伪高分。
- 参数敏感性检验通过（若可用热力图或扰动分析）。

### 部署安全

- 最终参数通过 overrides 注入验证专用配置，未直接修改生产基线 YAML。
- SimNow 模拟环境中已用最终参数运行至少一个完整交易日，无系统级错误。
- 回滚方案已准备（保留当前生产配置的完整备份）。

## 8. 常见问题与检查清单

### 常见问题

**trial 全失败**

优先检查是否构建了 Arrow/Parquet 支持。若 stderr 出现未启用 Arrow/Parquet 的提示，重新使用 `-DQUANT_HFT_ENABLE_ARROW_PARQUET=ON` 配置并构建。

**manifest 不存在**

检查 `dataset_root/_manifest/partitions.jsonl` 是否存在。rolling 配置若未指定 `dataset_manifest`，会按该默认路径推导。

**rolling 输出落到错误目录**

`output.root_dir` 相对 rolling 配置文件所在目录解析。配置位于 `configs/ops/` 时，建议写成 `../../runtime/your_run_name`。

**全部 constraint_violated**

先降低约束强度，确认是否有正常成交和可用指标。约束应过滤明显不合格组合，不应替代目标函数。

**目标值为 0 或没有成交**

检查 `emit_trades: true`、数据区间、品种、合约信息、换月配置和策略信号是否正常。无成交高分不得晋级。

**随机搜索无法复现**

确认 `random_seed`、参数空间、`max_trials`、目标函数、约束、数据区间和二进制版本完全一致。复现实验只改输出目录。

**并发导致机器卡顿或失败**

降低 `batch_size` 或 `parallel`。当前并发由 `SafeMaxConcurrent` 保护，并按 1536 MB/trial 估算，但配置仍应保守。

**不知道如何部署推荐参数**

不要直接改生产子策略 YAML。复制主策略配置，通过 `overrides.backtest.params` 注入参数，再让 rolling/backtest 配置指向该验证专用主策略配置。

**混淆了筛选指标与决策指标**

回顾第 2 节和第 6 节的阶段分离规则：综合得分仅在研发筛选阶段使用以产生 TopN；最终部署参数必须由单一硬性指标决定。如有疑问，回到优化计划中检查是否已事先声明该指标。

### 每日操作检查清单

- [ ] 已写明本轮 `run_id`、策略、品种、训练区间、验证区间、测试/OOS 区间。
- [ ] 已确认 `parameter_optim_cli`、`rolling_backtest_cli`、`backtest_cli`、`oos_top10_validation_cli` 可构建或已存在。
- [ ] 已确认 Parquet v2 数据和 `_manifest/partitions.jsonl` 覆盖目标区间。
- [ ] 已声明当前只优化 `c`，或已显式列出多品种符号及覆盖情况。
- [ ] 已固定参数空间、目标函数、约束和随机种子。
- [ ] 已控制 `max_trials`、`batch_size` / `parallel` 和输出目录。
- [ ] rolling / Walk-Forward / fixed rolling 的 test 窗口长度至少 60 个交易日，或已记录用户批准的例外原因。
- [ ] 单次优化报告中 completed trial 足够，failed 和 constraint_violated 原因已复核。
- [ ] heatmap 或 TopN trial 显示参数不是孤立尖峰。
- [ ] 已声明综合得分规则（仅用于筛选 TopN）。
- [ ] 在进入固定参数 rolling 前，已声明单一硬性决策指标。
- [ ] Walk-Forward 多窗口表现稳定，参数未剧烈跳变。
- [ ] rolling / OOS test 窗口已完成合约换月审计，换月污染窗口已剔除或重分。
- [ ] TopN OOS 验证通过，并生成 `oos_top10_validation.csv` 和 `oos_validated_candidates.yaml`。
- [ ] 候选参数已通过固定参数 rolling testing（含换月审计）。
- [ ] 候选参数已通过全周期回测与验证报告。
- [ ] 最终参数由事先声明的单一硬性决策指标选出，过程可追溯。
- [ ] 参数部署使用验证专用主策略配置和 `overrides.backtest.params`，未污染生产基线。
- [ ] 所有 JSON、Markdown、YAML、CSV 结果都已归档，且能追溯到同一轮计划。
- [ ] SimNow 仿真验证已完成，无系统级错误。
- [ ] 回滚方案已就绪。