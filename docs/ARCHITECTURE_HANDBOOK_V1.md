# 架构规范手册 v1（0->1）

## 目标
- 先完成 SimNow 闭环，再进入小资金实盘。
- 采用 C++ 核心执行 + Python 策略编排。

## 进程边界
- `core_engine`（C++17）: CTP Gateway、Risk、Order、Portfolio、MarketState Rule v1、RegulatorySink。
- `strategy_runtime`（Python）: 策略回调 `on_bar/on_state/on_order_event`。
- `data_pipeline`（后续可拆）: Redis/TSDB/DuckDB/MinIO数据写入和ETL。
- `ops_agent`: 指标、日志、告警和运维控制。

## 强制语言边界
- L0（关键路径）必须 C++：行情、风控前置、下单、成交回报、账本。
- L1（中低频）可 Python：策略编排、参数调度。
- L2（离线研究）Python 优先。
- 迁移阈值：Python 模块 `P99 > 5ms` 或 CPU 持续 > 30% 且影响链路，必须下沉 C++。

## CTP v6.7.11 适配规则
- 必须显式传入 `bIsProductionMode`，禁止依赖默认值。
- SimNow:
  - 7x24 环境（40001/40011）: `bIsProductionMode=false`
  - 交易时段一致环境（3000x，看穿式前置生产秘钥）: `bIsProductionMode=true`
- 预留 `ReqQryUserSession` 接口和 `LastLoginTime`/`ReserveInfo` 字段处理。
- 预留 `ApplySrc` 字段适配，避免自对冲撤销错配。
- 查询统一走 `QueryScheduler`，默认硬限流 10 req/s。

## 数据一致性
- 交易事件先写本地 WAL，再异步入库。
- 失败恢复通过 WAL 重放。

## 质量门禁
- C++: C++17 + clang-format + clang-tidy + GTest。
- Python: ruff + black + mypy + pytest。
- CI必须通过构建、单测、类型检查、格式检查。

## Protobuf兼容规则
- 仅允许新增字段；删除字段需跨两个小版本弃用周期。
