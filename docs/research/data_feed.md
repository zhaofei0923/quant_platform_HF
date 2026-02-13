# DataFeed：统一数据访问接口

## 设计目标

实盘与回测共用一套策略代码的核心挑战在于数据源接口差异。`DataFeed` 抽象用于屏蔽底层数据来源（历史 Parquet、实时行情、数据库等），对策略层统一提供订阅与历史查询能力。

## 核心抽象

`DataFeed` 提供：

- `Subscribe`：注册 Tick/Bar 回调。
- `GetHistoryBars` / `GetHistoryTicks`：历史数据同步查询。
- `Run` / `Stop`：启动与停止数据流。
- `CurrentTime`：返回当前事件时间（回测）或系统时间（实盘）。
- `IsLive`：区分实盘与回测模式。

## BacktestDataFeed（事件驱动回放）

### V1 模型

- 构造函数接收 `parquet_root` + 起止时间。
- `Subscribe` 阶段加载订阅 symbol 的 Tick 并压入最小堆事件队列。
- `Run` 以时间升序消费事件并触发回调，`CurrentTime` 随事件推进。

### 当前限制

- 首版采用预加载，数据规模大时可能占用较多内存。
- `GetHistoryBars` 暂未聚合实现，返回空。
- Tick 文件读取依赖 `.ticks.csv` 测试侧车文件（用于 PR-6.2 单测）。

### 优化方向

- 分块流式加载（chunk iterator）降低峰值内存。
- Tick->Bar 在线聚合并支持多周期。
- 增加多源 DataFeed 适配（ClickHouse / Kafka 回放）。

## LiveDataFeed（实盘桩实现）

`LiveDataFeed` 当前为可编译桩实现：

- `Subscribe` 仅保存回调。
- `GetHistory*` 返回空。
- `Run` 阻塞等待 `Stop`。
- `IsLive` 返回 `true`。

后续可由 `CtpDataFeed` 继承 `DataFeed` 接入真实行情。

## 使用示例

### C++

```cpp
using namespace quant_hft;
using namespace quant_hft::backtest;

BacktestDataFeed feed("runtime/backtest/parquet",
                      Timestamp::FromSql("2024-01-01"),
                      Timestamp::FromSql("2024-01-02"));
feed.Subscribe({"rb2405"}, [](const Tick& tick) {
    (void)tick;
});
feed.Run();
```

### Python

```python
from quant_hft.data_feed import BacktestDataFeed

feed = BacktestDataFeed("runtime/backtest/parquet", "2024-01-01", "2024-01-02")
feed.subscribe(["rb2405"], lambda tick: print(tick.ts_ns))
feed.run()
```
