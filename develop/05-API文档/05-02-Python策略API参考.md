# Python 策略 API 参考

## 对齐信息

- 对齐基线：main@17f0f8c957a4c0f95ebb054725f04b21e0e6861b
- 实现状态：已落地
- 证据路径：`python/quant_hft/`、`scripts/strategy/run_strategy.py`、`python/tests/`
- 最后更新：2026-02-11

## 概述

本仓库的 Python 包 `quant_hft` 提供：
- 策略接口（`StrategyBase`）
- 策略运行时分发器（`StrategyRuntime`）
- Redis 策略桥 runner（`quant_hft.runtime.strategy_runner.StrategyRunner`）
- 回测回放与确定性成交模拟（`quant_hft.backtest`）
- 运维与验证工具库（`quant_hft.ops`，对应 `scripts/ops/*`）
- 数据管道组件（`quant_hft.data_pipeline`，对应 `scripts/data_pipeline/*`）

## 1. 合约与数据结构（quant_hft.contracts）

源码：`python/quant_hft/contracts.py`

### 1.1 Side

```python
class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
```

### 1.2 OffsetFlag

```python
class OffsetFlag(str, Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    CLOSE_TODAY = "CLOSE_TODAY"
    CLOSE_YESTERDAY = "CLOSE_YESTERDAY"
```

### 1.3 SignalIntent

`SignalIntent` 表达策略产生的下单意图（信号到订单的桥接对象）：

```python
@dataclass(frozen=True)
class SignalIntent:
    strategy_id: str
    instrument_id: str
    side: Side
    offset: OffsetFlag
    volume: int
    limit_price: float
    ts_ns: int
    trace_id: str
```

### 1.4 OrderEvent

`OrderEvent` 表达订单生命周期事件（可用于回放、对账、策略侧状态更新）：

```python
@dataclass(frozen=True)
class OrderEvent:
    account_id: str
    client_order_id: str
    instrument_id: str
    status: str
    total_volume: int
    filled_volume: int
    avg_fill_price: float
    reason: str
    ts_ns: int
    trace_id: str
```

### 1.5 StateSnapshot7D

`StateSnapshot7D` 表达七维状态快照（当前实现为结构承载与分发；具体计算逻辑在 C++ 侧的规则引擎中）：

```python
@dataclass(frozen=True)
class StateSnapshot7D:
    instrument_id: str
    trend: dict[str, float]
    volatility: dict[str, float]
    liquidity: dict[str, float]
    sentiment: dict[str, float]
    seasonality: dict[str, float]
    pattern: dict[str, float]
    event_drive: dict[str, float]
    ts_ns: int
```

## 2. 策略接口（quant_hft.strategy.base）

源码：`python/quant_hft/strategy/base.py`

`StrategyBase` 是策略的最小接口：

```python
class StrategyBase(ABC):
    def on_bar(self, ctx: dict[str, Any], bar_batch: list[dict[str, Any]]) -> list[SignalIntent]:
        raise NotImplementedError

    def on_state(self, ctx: dict[str, Any], state_snapshot: StateSnapshot7D) -> list[SignalIntent]:
        raise NotImplementedError

    def on_order_event(self, ctx: dict[str, Any], order_event: OrderEvent) -> None:
        raise NotImplementedError
```

约定：
- `ctx` 用于策略运行上下文（可由上层框架注入，例如风控参数、账户信息、策略级状态等）
- `bar_batch/state_snapshot` 为输入事件
- 返回值为 `SignalIntent` 列表

## 3. 策略运行时（quant_hft.runtime.engine）

源码：`python/quant_hft/runtime/engine.py`

`StrategyRuntime` 负责将事件分发给已注册策略，并收集意图：

```python
runtime = StrategyRuntime()
runtime.add_strategy(DemoStrategy("demo"))

intents = runtime.on_bar({}, [{"instrument_id": "SHFE.ag2406"}])
runtime.on_order_event({}, OrderEvent(...))
```

## 4. Redis 策略桥 Runner（quant_hft.runtime.strategy_runner）

源码：
- `python/quant_hft/runtime/strategy_runner.py`
- `python/quant_hft/runtime/redis_hash.py`
- `python/quant_hft/runtime/redis_schema.py`

`StrategyRunner` 负责：
- 读取 `market:state7d:<instrument>:latest`
- 调用 `runtime.on_state(...)` 产出 `SignalIntent`
- 写入 `strategy:intent:<strategy_id>:latest`（带 `seq/count/intent_i`）
- 轮询 `trade:order:<trace_id>:info` 并触发 `runtime.on_order_event(...)`

核心调用：

```python
runner = StrategyRunner(
    runtime=runtime,
    redis_client=redis_client,
    strategy_id="demo",
    instruments=["SHFE.ag2406"],
    poll_interval_ms=200,
)
runner.run_once()      # 单次循环
runner.run_forever()   # 持续轮询
```

Runner 配置加载：
- `load_runner_config("configs/sim/ctp.yaml")`
- 读取 `instruments`、`strategy_ids`、`strategy_poll_interval_ms`

Redis 协议约定见：`docs/STRATEGY_BRIDGE_REDIS_PROTOCOL.md`。

## 5. 最小可运行示例

（与 `python/tests/test_runtime_engine.py` 一致）

```python
from quant_hft.contracts import OffsetFlag, OrderEvent, Side, SignalIntent, StateSnapshot7D
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.strategy.base import StrategyBase


class DemoStrategy(StrategyBase):
    def on_bar(self, ctx: dict[str, object], bar_batch: list[dict[str, object]]) -> list[SignalIntent]:
        return [
            SignalIntent(
                strategy_id=self.strategy_id,
                instrument_id="SHFE.ag2406",
                side=Side.BUY,
                offset=OffsetFlag.OPEN,
                volume=1,
                limit_price=4500.0,
                ts_ns=1,
                trace_id="t1",
            )
        ]

    def on_state(self, ctx: dict[str, object], state_snapshot: StateSnapshot7D) -> list[SignalIntent]:
        return []

    def on_order_event(self, ctx: dict[str, object], order_event: OrderEvent) -> None:
        ctx["events"] = int(ctx.get("events", 0)) + 1


runtime = StrategyRuntime()
runtime.add_strategy(DemoStrategy("demo"))
print(runtime.on_bar({}, [{"instrument_id": "SHFE.ag2406"}]))
```

## 6. 回测回放（quant_hft.backtest）

- 文档：`docs/BACKTEST_REPLAY_HARNESS.md`
- CLI：`scripts/backtest/replay_csv.py`
- 关键参数：
  - `--scenario-template`：场景模板回放
  - `--emit-state-snapshots`：回放时派发 `StateSnapshot7D`（研究输入对齐实盘合约）
  - `--report-json` / `--report-md`：输出结构化与可读报告

## 7. 数据管道（quant_hft.data_pipeline）

- CLI：`scripts/data_pipeline/run_pipeline.py`

## 8. 运维工具（quant_hft.ops）

- systemd 渲染：`scripts/ops/render_systemd_units.py`
- k8s 渲染：`scripts/ops/render_k8s_manifests.py`
- CTP 预检：`scripts/ops/ctp_preflight_check.py`
