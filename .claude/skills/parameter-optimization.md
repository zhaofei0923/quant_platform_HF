---
name: parameter-optimization
description: >-
  量化策略参数优化完整工作流。
  适用：KAMA / CompositeStrategy 参数搜索、网格/随机搜索、Walk-Forward 滚动优化、TopN OOS 验证、
  换月审计、refinement 区间收窄、固定参数滚动验证、全周期回测验证。
  不适用：实盘交易、SimNow 启动、CTP 柜台操作、策略逻辑重构。
---

# 参数优化 Skill

本 Skill 加载 `docs/ops/parameter_optimization_skill.md` 中的全部规则作为执行依据。
任何参数优化任务都必须遵守该文档中定义的四条核心原则：

1. **可复现** — Git commit、配置、数据、随机种子固定
2. **防过拟合** — OOS 不参与搜索，搜索空间小而有理据，优先稳定性
3. **输出透明** — JSON/Markdown/YAML/CSV 全部归档，可追溯到优化计划
4. **自动化程度高** — 脚本优先于手动操作

## 核心规则摘要

### 参数空间
- 当前优化三项核心参数：`kama_filter`、`stop_loss_atr_multiplier`、`risk_per_trade_pct`
- 必须遵守策略实现中的约束：`kama_filter >= 0`，`stop_loss_atr_multiplier > 0`，`risk_per_trade_pct ∈ (0, 1]`
- 新增参数维度必须说明策略含义，并同步增加 OOS 检验

### 目标函数与约束
- 单指标目标：`metric_path: profit_factor`（需 `emit_trades: true`）
- 复合目标：多指标加权，权重必须在实验前声明
- 约束 DSL：`profit_factor > 1.3`、`total_trades > 30`、`max_drawdown_pct < 20`

### 阶段分离（铁律）
- 研发筛选阶段：综合得分仅用于筛选 TopN，权重必须事先声明
- 最终决策阶段：必须使用事先声明的单一硬性指标，一经声明不得更改

### rolling / Walk-Forward
- test 窗口长度 ≥ 60 个交易日（低于 60 需用户明确批准）
- 每个 test/OOS 窗口必须做换月审计，污染窗口（包含多个 instrument_id）必须剔除

### 晋级流程
单次优化 → Walk-Forward → TopN OOS → 声明硬性指标 → 固定参数 rolling → 全周期回测 → 最终推荐

### 部署安全
- 不直接修改生产基线配置
- 通过 `overrides.backtest.params` 注入验证专用配置
- 最终参数必须通过 SimNow 仿真至少一个完整交易日

## 完整规则

执行任何参数优化任务前，必须完整加载以下文件中的规则：

- [参数优化 Skill 完整文档](../../docs/ops/parameter_optimization_skill.md)
- [Parameter Optimizer Agent 使用说明](../../docs/ops/parameter_optimizer_agent_usage.md)

本 Skill 被 `/parameter-optimization` 命令调用，或被 Parameter Optimizer Agent 在启动时加载。
