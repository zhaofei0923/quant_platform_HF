# 量化系统架构总览（生产主链路）

> 范围说明：本文聚焦“生产主链路”，即 C++ 核心交易链路 + Python 策略编排 + 基础运维部署与数据归档。未将路线图中尚未落地项计入。

## 1. 架构分层

### L0：低延迟关键路径（C++）
- 进程：`core_engine`
- 职责：行情处理、交易时段判定、风控前置、订单执行、持仓账本、监管 WAL
- 关键入口：
  - `src/apps/core_engine_main.cpp`
  - `src/services/market_state/*`
  - `src/services/risk/*`
  - `src/services/order/*`
  - `src/services/portfolio/*`
  - `src/core/regulatory/*`

### L1：策略编排（Python）
- 进程：`strategy_runner`（脚本入口）
- 职责：消费状态/Bar，调用策略回调（`on_bar/on_state/on_order_event`），产出交易意图
- 关键入口：
  - `scripts/strategy/run_strategy.py`
  - `python/quant_hft/runtime/strategy_runner.py`
  - `python/quant_hft/strategy/*`

### L2：数据与运维（Python）
- 进程：`data_pipeline`（脚本入口）
- 职责：导出分析数据、归档对象存储、产出运维证据
- 关键入口：
  - `scripts/data_pipeline/run_pipeline.py`
  - `python/quant_hft/data_pipeline/*`
  - `scripts/ops/*`

## 2. 主链路数据流

## 2.1 交易闭环
1. CTP 行情进入 `core_engine`
2. `BarAggregator` + `RuleMarketStateEngine` 生成可消费市场状态
3. 风控链路（`basic_risk_engine` / `risk_policy_engine` / `self_trade_risk_engine`）拦截非法指令
4. 执行链路（`execution_planner` / `execution_router` / `execution_engine` / `order_manager`）下单与状态迁移
5. 账本链路（`position_manager` / `ctp_position_ledger` / `ctp_account_ledger`）维护持仓与账户
6. 监管链路（`local_wal_regulatory_sink`）先写 WAL，支持恢复

## 2.2 策略桥闭环（Redis Protocol）
- 协议文档：`docs/STRATEGY_BRIDGE_REDIS_PROTOCOL.md`
- C++ 侧：发布 `market:state7d:*`，消费 `strategy:intent:*`，写 `trade:order:*`
- Python 侧：消费 `market:state7d:*`，产出 `strategy:intent:*`，消费 `trade:order:*`

## 2.3 恢复与回放
- WAL 重放工具：`src/apps/wal_replay_main.cpp`（`wal_replay_tool`）
- 引擎启动恢复：`src/core/regulatory/wal_replay_loader.cpp`
- 回测重放：`python/quant_hft/backtest/replay.py` + `scripts/backtest/replay_csv.py`

## 3. 进程与部署形态

### 3.1 进程角色
- `core_engine`：交易核心进程
- `daily_settlement`：日结流程
- `simnow_probe`：联通与健康探针
- `strategy_runner`：策略运行
- `data_pipeline`：分析与归档

### 3.2 部署基线
- 单机生产基线（systemd）：`docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md`
- 非热路径 K8s：`docs/K8S_DEPLOYMENT_RUNBOOK.md`
- 基础设施容器编排：`infra/docker-compose.single-host.yaml`

## 4. 代码结构映射
- C++ 核心库：`quant_hft_core`（定义见 `CMakeLists.txt`）
- C++ 应用：`core_engine` / `simnow_probe` / `wal_replay_tool` / `daily_settlement` / `hotpath_benchmark`
- Python 包：`python/quant_hft`（`runtime`、`strategy`、`backtest`、`data_pipeline`、`ops`）
- 契约定义：`proto/quant_hft/v1/contracts.proto`

## 5. 性能与可靠性设计要点
- 语言边界：关键路径优先 C++，策略编排与离线链路使用 Python
- 一致性策略：交易事件先 WAL 再异步入库
- 质量门禁：C++（GTest + clang 系列）与 Python（pytest + ruff + black + mypy）双栈验证
- 可观测性：指标/告警/SLO 见 `docs/OPS_SLI_SLO_SPEC.md` 与 `docs/ops/metrics.md`

## 6. 架构证据路径（核心）
- `README.md`
- `docs/ARCHITECTURE_HANDBOOK_V1.md`
- `CMakeLists.txt`
- `src/apps/core_engine_main.cpp`
- `python/quant_hft/runtime/strategy_runner.py`
- `docs/STRATEGY_BRIDGE_REDIS_PROTOCOL.md`
- `docs/WAL_RECOVERY_RUNBOOK.md`
