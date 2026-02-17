# Backtest Engine

## Overview

`BacktestEngine` stitches together:

- `DataFeed` (historical replay)
- `SimulatedBroker` (order matching and account bookkeeping)
- `Strategy` (signal generation)

The engine is event-driven. Every market tick is processed in time order and triggers:

1. strategy callback (`OnTick`)
2. broker matching (`OnTick` -> fills/order updates)
3. result recording (orders/trades/equity)

## Components

- Events: [include/quant_hft/backtest/events.h](include/quant_hft/backtest/events.h)
- Broker: [include/quant_hft/backtest/broker.h](include/quant_hft/backtest/broker.h)
- Engine: [include/quant_hft/backtest/engine.h](include/quant_hft/backtest/engine.h)
- Performance: [include/quant_hft/backtest/performance.h](include/quant_hft/backtest/performance.h)
- Strategy base: [include/quant_hft/strategy/base_strategy.h](include/quant_hft/strategy/base_strategy.h)

## Matching Rules (V1)

- limit order: price-check against top-of-book from tick (`bid_price1`/`ask_price1`)
- market order: immediate fill at top-of-book
- partial fill: enabled by config, capped by `last_volume`
- commission: proportional to traded notional
- FIFO close PnL: closes historical lots in open-time order

## Current Limits

- no advanced queue simulation (latency/queue position)
- no multi-level orderbook consumption
- no bar aggregation in engine core

## Performance Summary (PR-6.4)

- `AnalyzePerformance(...)` 基于 `BacktestResult` 输出统一绩效摘要。
- 当前指标：`net_profit/total_return/max_drawdown/max_drawdown_ratio/return_volatility/sharpe_ratio`。
- pybind `BacktestEngine.get_result()` 返回 `performance` 字段，供 Python 研究层直接消费。
