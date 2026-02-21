# Composite Strategy（V2）

配置字段索引：`docs/ops/config_catalog.md`

## 核心边界

- 每个子策略是完整策略：`entry + sizing + stop_loss + take_profit`。
- 主策略只负责：
  - 子策略启停调度（`enabled`）
  - 开仓市场状态门控（`entry_market_regimes`，仅影响 `kOpen`）
  - 时间过滤器链（`ITimeFilterStrategy`）与风控策略链（`IRiskControlStrategy`）
  - 信号合并与持仓归属/反手两步门控
  - 回测上下文注入（权益、合约乘数）
- 默认仅允许 `run_type=backtest`；若配置 `enable_non_backtest: true`，才允许 `sim/live`。

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
  enable_non_backtest: false
  sub_strategies:
    - id: kama_trend_1
      enabled: true
      type: KamaTrendStrategy
      config_path: ./sub/kama_trend_1.yaml
      entry_market_regimes: [kStrongTrend, kWeakTrend]
      overrides:
        backtest:
          params:
            take_profit_atr_multiplier: 20.0
        sim:
          params:
            default_volume: 2
    - id: trend_1
      enabled: false
      type: TrendStrategy
      config_path: ./sub/trend_1.yaml
      entry_market_regimes: [kStrongTrend]
```

## 升级后如何使用

### 示例 1：保持历史回测行为（推荐起点）

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
  enable_non_backtest: false
  sub_strategies:
    - id: kama_trend_1
      enabled: true
      type: KamaTrendStrategy
      config_path: ./sub/kama_trend_1.yaml
```

### 示例 2：开启 sim/live 并按运行模式覆盖参数

```yaml
run_type: sim
market_state_mode: true
backtest:
  initial_equity: 200000
  symbols: [c]
  start_date: 20240101
  end_date: 20240131
  product_config_path: ./instrument_info.json
composite:
  merge_rule: kPriority
  enable_non_backtest: true
  sub_strategies:
    - id: kama_trend_1
      enabled: true
      type: KamaTrendStrategy
      config_path: ./sub/kama_trend_1.yaml
      overrides:
        backtest:
          params:
            take_profit_atr_multiplier: 20.0
        sim:
          params:
            default_volume: 2
        live:
          params:
            risk_per_trade_pct: 0.005
```

说明：

- 若 `run_type` 为 `sim/live`，必须配合 `enable_non_backtest: true`，否则初始化会 fail-fast。
- `overrides` 仅允许 `backtest|sim|live` 三个键，且每层仅支持 `params` 标量键值。
- 真实初始化参数为：`base params + overrides[run_mode].params`。

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
  take_profit_atr_multiplier: 20.0
```

## 信号裁决

- 优先级：`ForceClose > StopLoss > TakeProfit > Close > Open`
- 同级 tie-break：`volume desc -> ts_ns desc -> trace_id asc`
- 反手两步：先发平仓，仓位归零后的下一根 Bar 再发反向开仓。
- 合并器当前实现：`kPriority`（可插拔，后续可扩展其它规则）。

## overrides 规则

- `sub_strategies[].overrides` 支持三层：`backtest`、`sim`、`live`。
- 每层仅支持 `params`，并且值必须是标量。
- 初始化时参数合并顺序：`base params + overrides[run_mode].params`（后者覆盖前者）。
- 非法 run_mode 键会 fail-fast（仅允许 `backtest|sim|live`）。

## 持久化与指标

- `CompositeStrategy` 支持 `SaveState/LoadState`：
  - 组合层上下文（仓位、均价、权益、乘数、归属）会持久化。
  - 子策略若实现 `IAtomicStateSerializable`，可一并保存原子状态。
- 支持 `CollectMetrics()`：
  - 子策略数量、过滤器数量、持仓维度、权益/保证金/可用资金等指标。

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
