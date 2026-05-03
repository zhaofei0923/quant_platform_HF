# Parameter Optimizer Agent 使用说明

本文说明如何在当前 quant_hft 项目中使用自定义 Agent [Parameter Optimizer](../../.github/agents/parameter-optimizer.agent.md) 自动寻找稳健策略参数组合。它严格遵循 [参数优化 Skill](./parameter_optimization_skill.md) 的全部规则与晋级标准，按照“先计划 -> 再执行 -> 复盘 -> 收窄区间 -> 验证”的闭环工作。

Agent 的总目标不是追逐训练集最高分，而是自动化地产出可复现、抗过拟合、输出透明、能通过全部晋级门槛的候选参数。过程中它会把“综合得分仅用于筛选 TopN，最终参数由单一硬性指标决定”这条铁律落实到每一步交互里。

## 1. 什么时候使用

适用场景：

- 为单个品种优化 KAMA / CompositeStrategy 参数（例如 `rb`、`m`、`ma`、`hc`）。
- 基于已有 `parameter_optim_report.json` 与 heatmap 生成 refinement 搜索区间。
- 执行粗网格搜索、随机搜索、Walk-Forward、TopN OOS 验证。
- 对多窗口结果做合约换月审计，并剔除污染窗口。
- 判断一组候选参数是否可以进入固定参数 rolling testing 与全周期回测验证。
- 在进入固定参数 rolling 之前，提醒并协助用户锁定最终单一决策指标。

不适用场景：

- 启动 SimNow、CTP 实盘、探测交易柜台或处理实盘运行异常。
- 直接修改生产基线配置并部署参数。
- 泛化代码重构、C++ bug 修复或无参数化的纯策略逻辑调整。

## 2. 如何启动 Agent

在 VS Code Chat 的 Agent 选择器中选择 `Parameter Optimizer`。如果列表中暂时没有：

1. 确认文件存在：[.github/agents/parameter-optimizer.agent.md](../../.github/agents/parameter-optimizer.agent.md)。
2. 确认 frontmatter 中 `user-invocable: true`。
3. 必要时重启 VS Code Chat 或执行 Reload Window。

你也可以在默认 Agent 中直接点名：

```text
使用 Parameter Optimizer 优化 c 品种，区间 20230103 到 20241231，目标 profit_factor，最多 2 轮 refinement，max_trials 48。
```

## 3. 最小输入模板

一次可执行的参数优化请求应至少给出品种、日期区间与计算预算。Agent 会根据这些信息生成一份包含完整合规字段的优化计划，待你确认后再执行。

```text
使用 Parameter Optimizer 优化 <symbol> 品种，训练区间 <start_date> 到 <end_date>，使用默认 KAMA 策略，目标 <objective>，最多 <rounds> 轮 refinement，每轮 max_trials <n>，batch_size <m>。
```

示例：

```text
使用 Parameter Optimizer 优化 rb 品种，训练区间 20230103 到 20241231，使用默认 KAMA 策略，目标 profit_factor，最多 2 轮 refinement，每轮 max_trials 48，batch_size 2。
```

如果你仅提供简短指令（例如“优化 rb 品种参数”），Agent 会用 checked-in 默认 KAMA 配置作为起点，但在消耗较大搜索预算之前，会先向你确认日期区间、目标函数和 trial 预算，避免撞墙。

## 4. 常用任务 Prompt

### 4.1 从零开始优化某个品种

```text
使用 Parameter Optimizer 优化 rb 品种，训练区间 20230103 到 20241231，目标 profit_factor，最多 2 轮 refinement，每轮 max_trials 48。先生成计划，确认后执行。
```

### 4.2 从零开始完成完整优化链路

如果希望 Agent 从零开始跑完整流程，可以使用下面这个模板。把尖括号替换为实际值；如果已经确定最终硬性决策指标，也应直接写进 prompt。

```text
使用 Parameter Optimizer 从零开始完成 <symbol> 品种 KAMA / CompositeStrategy 参数优化与验证。

基础设置：
- 数据区间：<start_date> 到 <end_date>。
- 使用 Parquet v2 数据，开启 strict_parquet，不设置 max_ticks 限制。
- 初始资金：<initial_equity>。
- bar 周期：<bar_interval>，例如 5min。
- 策略：默认 CompositeStrategy -> KamaTrendStrategy，不修改生产基线配置。

搜索预算：
- 单次优化目标：<objective>，例如 profit_factor。
- 搜索算法：<grid/random>。
- 最多 <rounds> 轮 refinement，每轮 max_trials <n>，batch_size <m>。
- 使用 run-specific 配置和输出目录，不覆盖 baseline YAML。

筛选与验证规则：
- 先生成完整计划，列出 run_id、配置路径、命令、搜索空间、约束、综合得分规则、换月审计口径和输出目录，等待我确认后再执行。
- 综合得分只用于筛选 TopN，至少覆盖 PF、PnL、Calmar、最大回撤、正收益窗口数、PF>1 窗口数、成交质量和参数稳定性。
- OOS 只用于验收和淘汰，不允许根据 OOS 结果反向修改搜索空间、目标权重、约束或随机种子。
- rolling / Walk-Forward / fixed rolling 的 test 窗口长度至少 60 个交易日；低于 60 必须先获得我的明确批准，并在报告中标记限制。
- rolling / OOS / fixed rolling 的 test 窗口必须执行合约换月审计；如果窗口包含多个 instrument_id，必须剔除或重新生成单合约窗口，不得用于排名。

最终决策：
- 在进入固定参数 rolling 前，最终硬性决策指标锁定为：<final_hard_metric>。
- 示例：在所有通过验证的候选参数中，选择剔除换月污染窗口后的全周期 fixed rolling 测试集 calmar_ratio 最高的组合。
- 该硬性指标一经声明不得更改；最终参数不能由综合得分或单次 PF 冠军直接决定。

执行链路：
计划确认后，依次执行粗搜、报告分析、refinement、Walk-Forward、TopN OOS、固定参数 rolling、全周期回测验证，并输出最终建议。

最终报告必须包含：
- 每轮配置、命令和输出文件。
- TopN 参数、目标值和综合筛选得分表。
- 换月审计结果和被剔除窗口。
- OOS 验证摘要与 oos_validated_candidates.yaml。
- 最终硬性决策指标下的候选排名和选择理由。
- 被拒绝候选及原因。
- 建议：可进入实盘前评估 / 继续搜索 / 直接淘汰。
```

### 4.3 基于已有报告生成下一轮 refinement

```text
使用 Parameter Optimizer 读取 docs/results/opts/parameter_optim_report.json，分析 TopN 和 heatmap，生成下一轮 refinement 配置。不要运行 OOS，也不要覆盖原配置。
```

### 4.4 执行 Walk-Forward 验证

```text
使用 Parameter Optimizer 对当前最优 KAMA 候选参数做 Walk-Forward 验证，train 120 天、test 60 天、step 30 天，输出到 runtime/rolling_optimize_kama_rb_v1。完成窗口分析后，执行合约换月审计，标记并剔除任何包含多个 instrument_id 的污染窗口。
```

### 4.5 做 TopN OOS 验证

```text
使用 Parameter Optimizer 对 runtime/rolling_optimize_kama/train_reports/window_0000/parameter_optim_report.json 做 TopN OOS 验证，OOS 区间 2024-07-01 到 2024-12-31，top_n 10。完成后输出 oos_validated_candidates.yaml，不作为最终部署依据。
```

### 4.6 锁定最终决策指标并完成全部验证

```text
使用 Parameter Optimizer 完成对 rb 品种的全部验证流程。在进入固定参数 rolling 之前，我声明最终决策指标为：在所有通过验证的候选参数中，选择全周期 Walk-Forward 测试集上 calmar_ratio 最高的组合。
```

### 4.7 只计划不执行

```text
使用 Parameter Optimizer 为 c 品种 2024 年 KAMA 参数优化生成完整计划，但不要创建配置或运行命令。
```

## 5. Agent 会创建和修改哪些文件

Parameter Optimizer 永远不会修改以下基线文件：

- `configs/ops/parameter_optim.yaml`
- `configs/ops/rolling_optimize_kama.yaml`
- `configs/strategies/main_backtest_strategy.yaml`
- `configs/strategies/sub/kama_trend_production.yaml`

它只会创建 run-specific 文件，典型命名规则：

```text
configs/ops/parameter_optim_<symbol>_<run_id>.yaml
configs/ops/parameter_optim_<symbol>_<run_id>_refine1.yaml
configs/ops/rolling_optimize_<symbol>_<run_id>.yaml
configs/strategies/main_backtest_strategy_<run_id>.yaml
```

常见输出目录：

```text
docs/results/opts/<run_id>/
runtime/rolling_optimize_<run_id>/
runtime/rolling_optimize_<run_id>/oos_validation/window_XXXX/
```

## 6. 品种指定方式

单次优化配置中，品种字段为：

```yaml
backtest_args:
  symbols: c
```

rolling optimize 配置中：

```yaml
backtest_base:
  symbols: [c]
```

你可以在 prompt 中直接写“优化 `rb` 品种”或“优化 `[c, rb]`”，Agent 会在新生成的 run-specific YAML 中正确更新这些字段。

## 7. 自适应 refinement 规则

Agent 在每轮单次优化后读取报告，决定是否生成下一轮搜索区间。它遵循以下安全约束：

1. 只使用 completed 且满足 constraints 的 trial 进入 TopN。
2. 优先检查 TopN 是否形成稳定参数簇，拒绝追逐孤立尖峰。
3. 若当前最优值落在搜索空间边界，且附近 TopN 也支持继续探索，可谨慎扩展边界一次，并在计划中写明理由。
4. 若 TopN 很分散或 heatmap 显示周围参数崩塌，自动停止 refinement，避免拟合噪声。
5. 默认最多 3 轮优化（含粗搜与 refinement），超出需用户显式批准。
6. 综合得分权重必须事先声明，并在 refinement 中保持固定，不得在事后根据本轮结果调整。
7. OOS 表现绝对不能用于反向修改搜索空间、目标权重或约束。OOS 的唯一职责是验收与淘汰。

## 8. 完整执行链路

Agent 推荐的完整链路如下（每一步都保留配置、命令和输出路径）：

```text
计划 -> 粗搜 -> 报告分析 -> refinement -> 再搜索
    -> Walk-Forward（含换月审计） -> TopN OOS
    -> 用户声明单一最终决策指标
    -> 固定参数 rolling（含换月审计）
    -> 全周期回测验证 -> 最终参数推荐
```

最终推荐参数时，Agent 必须附上至少以下信息：

- 本轮 `run_id` 和优化品种。
- 每轮配置文件路径。
- 每轮 TopN 参数、目标值、综合得分（若使用）。
- 每次收窄/扩展参数区间的逻辑与证据。
- Walk-Forward 多窗口表现摘要与换月审计结论（污染窗口已剔除）。
- TopN OOS 对比表的简要解读。
- 用户声明的单一最终决策指标、各候选在该指标下的得分及最终选择理由。
- 明确建议：可进入实盘前评估 / 需继续搜索 / 直接淘汰。

## 9. 常见问题

### Agent 一上来就向我确认细节，正常吗？

完全正常，且是刻意设计。若 prompt 缺少日期区间、目标函数或 trial 预算，Agent 必须先确认，避免盲目消耗计算资源。

### 能让 Agent 全自动跑到最终参数吗？

可以，但必须给出清晰预算和决策指标。例如：“最多 2 轮 refinement，每轮 48 trial，在进入固定参数 rolling 前使用全周期测试集 calmar_ratio 作为最终决策指标”。无边界自动搜索极易导致过拟合。

### 为什么 Agent 不直接把参数写进生产配置？

符合 Skill 的防污染原则。推荐参数必须先写入验证专用主策略配置（通过 `overrides.backtest.params` 注入），经过完整验证链路后，再由你手动合并到生产配置或另建生产运行 YAML。

### OOS 表现更好时，能不能围绕 OOS 最优点继续搜索？

绝对禁止，Agent 也自动遵循此规则。OOS 的职责是验收与拒绝，而不是为下一轮提供搜索方向。一旦用 OOS 结果调整搜索空间，OOS 即被污染，失去样本外意义。

### 如何在计划阶段暂停？

在 prompt 末尾追加：

```text
只生成计划，不创建文件，不运行命令。
```

### 如何限制计算资源？

在 prompt 中明确写：

```text
max_trials 24，batch_size 1，最多 1 轮 refinement，不运行 rolling。
```

### Walk-Forward 报告中很多窗口，怎么判断稳健？

Agent 会自动执行合约换月审计，标记包含多合约的污染窗口并建议剔除。你只需确认审计结论；最终稳健性判断基于剔除污染窗口后的有效窗口。

## 10. 快速检查清单

在每次启动 Agent 前或完成一轮优化后，请核验以下项目：

- [ ] 已选择 `Parameter Optimizer` Agent。
- [ ] 已给出品种（例如 `c`、`rb` 或 `[c, rb]`）。
- [ ] 已给出训练区间，并在需要时预留 OOS 区间。
- [ ] 已指定目标函数或复合目标（如 `profit_factor`、`calmar_ratio`）。
- [ ] 已控制预算：`max_trials`、`batch_size`、最大 refinement 轮数、是否运行 rolling。
- [ ] 已确认 rolling / Walk-Forward / fixed rolling 的 test 窗口长度至少 60 个交易日。
- [ ] 已要求输出到独立 `run_id` 目录，不覆盖基线配置。
- [ ] 已确认 OOS 只用于验证，不用于继续调参。
- [ ] 若进入最终验证阶段，已事先声明单一硬性决策指标。
- [ ] 已检查 Walk-Forward / rolling 报告的换月审计结论（污染窗口已剔除）。
- [ ] 最终推荐参数附带了完整的晋级记录与决策依据。