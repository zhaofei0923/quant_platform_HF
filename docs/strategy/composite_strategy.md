# Composite Strategy 插件框架（V1）

## 目标

`CompositeStrategy` 支持将开仓、止损、止盈、时间过滤、风控拆分为原子策略并组合运行。  
V1 保持 `ILiveStrategy` 与 `StrategyEngine` 接口不变，通过 `strategy_factory: composite` 接入。

## 接入步骤

1. 在 `ctp.yaml` 中启用 composite：

```yaml
ctp:
  strategy_factory: "composite"
  strategy_composite_config: "../strategies/composite_strategy.yaml"
```

2. 提供 composite 子配置文件（相对路径相对于 `ctp.yaml` 目录）：

```yaml
composite:
  merge_rule: kPriority
  opening_strategies:
    - id: trend_open
      type: TrendOpening
      params:
        instrument_id: SHFE.ag2406
        er_period: 10
        fast_period: 2
        slow_period: 30
        volume: 1
      market_regimes: [kStrongTrend, kWeakTrend]
  stop_loss_strategies:
    - id: atr_sl
      type: ATRStopLoss
      params:
        atr_period: 14
        atr_multiplier: 2.0
  take_profit_strategies:
    - id: atr_tp
      type: ATRTakeProfit
      params:
        atr_period: 14
        atr_multiplier: 3.0
  time_filters:
    - id: night_filter
      type: TimeFilter
      params:
        start_hour: 21
        end_hour: 2
        timezone: UTC
  risk_control_strategies:
    - id: max_pos
      type: MaxPositionRiskControl
      params:
        max_abs_position: 10
```

## 合并规则

- 当前仅支持 `merge_rule: kPriority`
- 同一标的信号优先级：`ForceClose > StopLoss > TakeProfit > Close > Open`
- 同优先级 tie-break：`volume desc -> ts_ns desc -> trace_id asc`
- 每标的每轮仅保留 1 条信号

## 执行顺序

每次 `OnState` 固定顺序：

1. `RiskControl`
2. `StopLoss`
3. `TakeProfit`
4. `TimeFilter`
5. `Opening`（仅当 time filter 全部放行，且满足 `market_regimes`）

## 内置原子策略参数

- `TrendOpening`
  - `id`（可选，默认 `TrendOpening`）
  - `instrument_id`（可选，空表示不限制）
  - `er_period`、`fast_period`、`slow_period`（默认 `10/2/30`，必须 > 0）
  - `volume`（默认 `1`，必须 > 0）

- `ATRStopLoss`
  - `id`（默认 `ATRStopLoss`）
  - `atr_period`（默认 `14`，必须 > 0）
  - `atr_multiplier`（默认 `2.0`，必须 > 0）

- `ATRTakeProfit`
  - `id`（默认 `ATRTakeProfit`）
  - `atr_period`（默认 `14`，必须 > 0）
  - `atr_multiplier`（默认 `2.0`，必须 > 0）

- `TimeFilter`
  - `id`（默认 `TimeFilter`）
  - `start_hour`、`end_hour`（`0..23`）
  - `timezone`：仅支持 `UTC`、`Asia/Shanghai`

- `MaxPositionRiskControl`
  - `id`（默认 `MaxPositionRiskControl`）
  - `max_abs_position`（必须 > 0）

## 订单事件持仓算法

`CompositeStrategy::OnOrderEvent` 内部维护：

- `delta_filled` 去重（同一订单重复上报不重复记账）
- 同向加仓按加权均价更新
- 部分减仓均价不变
- 平仓归零清除均价
- 反向穿仓后，剩余仓位均价重置为当前成交价
