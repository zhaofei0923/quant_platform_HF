# Composite Strategy（V2）

配置字段索引：`docs/ops/config_catalog.md`

## 核心边界

- 每个子策略是完整策略：`entry + sizing + stop_loss + take_profit`。
- 主策略只负责：
  - 子策略启停调度（`enabled`）
  - 开仓市场状态门控（`entry_market_regimes`，仅影响 `kOpen`）
  - 信号合并与持仓归属/反手两步门控
  - 回测上下文注入（权益、合约乘数）
- V2 仅支持 `run_type=backtest`，`sim/live + composite` 会直接报错。

## 主配置示例

文件：`configs/strategies/main_backtest_strategy.yaml`

```yaml
run_type: backtest
market_state_mode: true

backtest:
  initial_equity: 200000
  symbols: [c]
  start_date: 20240101
  end_date: 20240131
  product_config_path: ./instrument_info.json

composite:
  merge_rule: kPriority
  sub_strategies:
    - id: kama_trend_1
      enabled: true
      type: KamaTrendStrategy
      config_path: ./sub/kama_trend_1.yaml
      entry_market_regimes: [kStrongTrend, kWeakTrend]
    - id: trend_1
      enabled: false
      type: TrendStrategy
      config_path: ./sub/trend_1.yaml
      entry_market_regimes: [kStrongTrend]
```

## 子策略示例

文件：`configs/strategies/sub/kama_trend_1.yaml`

```yaml
params:
  id: kama_trend_1
  er_period: 10
  fast_period: 2
  slow_period: 30
  std_period: 20
  kama_filter: 0.5
  risk_per_trade_pct: 0.01
  default_volume: 1
  stop_loss_mode: trailing_atr
  stop_loss_atr_period: 14
  stop_loss_atr_multiplier: 2.0
  take_profit_mode: atr_target
  take_profit_atr_period: 14
  take_profit_atr_multiplier: 3.0
```

## 信号裁决

- 优先级：`ForceClose > StopLoss > TakeProfit > Close > Open`
- 同级 tie-break：`volume desc -> ts_ns desc -> trace_id asc`
- 反手两步：先发平仓，仓位归零后的下一根 Bar 再发反向开仓。

## 破坏性变更

- 删除主配置与 CLI 的 `max_loss_percent`。
- 删除分段配置键：
  - `opening_strategies`
  - `stop_loss_strategies`
  - `take_profit_strategies`
  - `time_filters`
  - `risk_control_strategies`
- 旧类型名不再用于 V2 配置：
  - `KamaTrendOpening`
  - `TrendOpening`
  - `TrailingStopLoss`
  - `ATRStopLoss`
  - `ATRTakeProfit`
