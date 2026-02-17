# 策略 API 参考（纯 C++）

## 对齐信息

- 对齐基线：main@f1f98c884be1538c06acff265f2904745175be96
- 实现状态：已落地
- 证据路径：`include/quant_hft/strategy/live_strategy.h`、`include/quant_hft/strategy/strategy_registry.h`、`include/quant_hft/strategy/strategy_engine.h`、`include/quant_hft/strategy/demo_live_strategy.h`
- 最后更新：2026-02-17

## 概述

当前策略运行时已统一为 C++：
- 策略接口：`ILiveStrategy`
- 策略注册：`StrategyRegistry`
- 策略调度：`StrategyEngine`
- 示例实现：`DemoLiveStrategy`

## 1. 核心接口

### 1.1 StrategyContext

`StrategyContext` 用于初始化策略运行上下文，包含：
- `strategy_id`
- `account_id`
- `metadata`

### 1.2 ILiveStrategy

`ILiveStrategy` 定义最小生命周期接口：
- `Initialize(const StrategyContext&)`
- `OnState(const StateSnapshot7D&) -> std::vector<SignalIntent>`
- `OnOrderEvent(const OrderEvent&)`
- `OnTimer(EpochNanos) -> std::vector<SignalIntent>`
- `Shutdown()`

## 2. 注册与调度

### 2.1 StrategyRegistry

- `RegisterFactory(factory_name, factory)`：注册策略工厂。
- `Create(factory_name)`：按工厂名实例化策略。
- `HasFactory(factory_name)`：检查工厂是否存在。

### 2.2 StrategyEngine

- `Start(strategy_ids, strategy_factory, base_context, error)`：启动策略引擎。
- `EnqueueState(state)`：输入状态事件。
- `EnqueueOrderEvent(event)`：输入订单回报事件。
- `GetStats()`：读取队列与回调统计。
- `Stop()`：停止并清理策略资源。

## 3. 最小示例

```cpp
#include "quant_hft/strategy/demo_live_strategy.h"
#include "quant_hft/strategy/strategy_engine.h"

quant_hft::StrategyEngine engine;
quant_hft::StrategyContext ctx;
ctx.account_id = "sim-account";

std::string error;
if (!engine.Start({"demo"}, "demo_live", ctx, &error)) {
    throw std::runtime_error(error);
}
```

## 4. 相关命令

- 策略链路 smoke：
  - `ctest --test-dir build -R "(StrategyRegistryTest|StrategyEngineTest|DemoLiveStrategyTest|CallbackDispatcherTest)" --output-on-failure`
- 全量回归：
  - `./scripts/build/bootstrap.sh`

## 5. 迁移说明

本文件保留原文件名用于文档索引兼容，内容已切换为 C++ 策略接口口径。
