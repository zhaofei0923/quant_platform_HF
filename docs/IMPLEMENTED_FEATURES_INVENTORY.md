# 已实现功能清单（生产主链路）

> 认定口径：代码存在 + 可追溯测试/目标（CMake 或 pytest）即判定为“已实现”。

## 1. 交易接入与执行（C++）

### 1.1 CTP 接入与配置
- 已实现：CTP 配置加载、校验、网关适配、交易适配、行情适配、查询调度
- 代码：
  - `src/core/ctp/ctp_config_loader.cpp`
  - `src/core/ctp/ctp_gateway_adapter.cpp`
  - `src/core/ctp/ctp_trader_adapter.cpp`
  - `src/core/ctp/ctp_md_adapter.cpp`
  - `src/core/ctp/query_scheduler.cpp`
- 测试/目标：
  - `ctp_config_loader_test`
  - `ctp_gateway_adapter_test`
  - `ctp_trader_adapter_test`
  - `ctp_md_adapter_test`
  - `query_scheduler_test`

### 1.2 订单执行与状态机
- 已实现：执行规划、路由、执行引擎、订单管理、状态机
- 代码：
  - `src/services/order/execution_planner.cpp`
  - `src/services/order/execution_router.cpp`
  - `src/services/order/execution_engine.cpp`
  - `src/services/order/order_manager.cpp`
  - `src/services/order/order_state_machine.cpp`
- 测试/目标：
  - `execution_planner_test`
  - `execution_router_test`
  - `execution_engine_test`
  - `order_manager_test`
  - `order_state_machine_test`

## 2. 风控与仓位账本（C++）

### 2.1 风控引擎
- 已实现：基础风控、策略规则风控、自成交风控、风险规则执行器与管理器
- 代码：
  - `src/services/risk/basic_risk_engine.cpp`
  - `src/services/risk/risk_policy_engine.cpp`
  - `src/services/risk/self_trade_risk_engine.cpp`
  - `src/risk/risk_rule_executor.cpp`
  - `src/risk/risk_manager.cpp`
- 测试/目标：
  - `basic_risk_engine_test`
  - `risk_policy_engine_test`
  - `self_trade_risk_engine_test`
  - `risk_rule_executor_test`
  - `risk_manager_test`

### 2.2 账本与持仓
- 已实现：持仓管理、CTP 持仓账本、CTP 账户账本、内存账本
- 代码：
  - `src/services/portfolio/position_manager.cpp`
  - `src/services/portfolio/ctp_position_ledger.cpp`
  - `src/services/portfolio/ctp_account_ledger.cpp`
  - `src/services/portfolio/in_memory_portfolio_ledger.cpp`
- 测试/目标：
  - `position_manager_test`
  - `ctp_position_ledger_test`
  - `ctp_account_ledger_test`
  - `in_memory_portfolio_ledger_test`

## 3. 市场状态与策略桥（C++ + Python）

### 3.1 市场状态引擎
- 已实现：Bar 聚合、交易时段过滤、规则化市场状态
- 代码：
  - `src/services/market_state/bar_aggregator.cpp`
  - `src/services/market_state/rule_market_state_engine.cpp`
- 测试/目标：
  - `bar_aggregator_test`
  - `rule_market_state_engine_test`

### 3.2 Redis Strategy Bridge
- 已实现：状态发布、意图消费、订单事件回传协议闭环
- 协议：`docs/STRATEGY_BRIDGE_REDIS_PROTOCOL.md`
- Python 运行：
  - `python/quant_hft/runtime/strategy_runner.py`
  - `scripts/strategy/run_strategy.py`
- 测试：
  - `python/tests/test_strategy_runner.py`
  - `python/tests/test_bar_dispatch_e2e.py`

## 4. 回测与重放（C++ + Python）

### 4.1 C++ 回测组件
- 已实现：backtest data feed、broker、engine、performance、live data feed
- 代码：`src/backtest/*`
- 测试/目标：
  - `backtest_data_feed_test`
  - `broker_test`
  - `engine_test`
  - `performance_test`
  - `live_data_feed_test`

### 4.2 Python 重放框架
- 已实现：CSV 回放、确定性成交、报告输出
- 代码：
  - `python/quant_hft/backtest/replay.py`
  - `scripts/backtest/replay_csv.py`
- 测试：
  - `python/tests/test_backtest_replay.py`
  - `python/tests/test_backtest_replay_cli.py`
  - `python/tests/test_backtest_scenarios.py`

## 5. 存储、一致性与恢复（C++ + Python）

### 5.1 存储抽象
- 已实现：Redis 实时存储、Timescale 事件存储、连接池/工厂、客户端适配
- 代码：`src/core/storage/*`
- 测试/目标：
  - `redis_realtime_store_test`
  - `timescale_event_store_test`
  - `storage_client_pool_test`
  - `storage_client_factory_test`

### 5.2 WAL 与恢复
- 已实现：本地 WAL 写入、WAL 重放加载、重放工具
- 代码：
  - `src/core/regulatory/local_wal_regulatory_sink.cpp`
  - `src/core/regulatory/wal_replay_loader.cpp`
  - `src/apps/wal_replay_main.cpp`
- 测试/目标：
  - `wal_replay_loader_test`
  - `python/tests/test_verify_wal_recovery_evidence_script.py`

## 6. 结算与监管导出
- 已实现：日结查询、结算价服务、日结服务与编排
- 代码：
  - `src/services/settlement/*`
  - `src/apps/daily_settlement_main.cpp`
  - `scripts/ops/run_daily_settlement.py`
- 测试/目标：
  - `settlement_query_client_test`
  - `settlement_price_provider_test`
  - `daily_settlement_service_test`
  - `python/tests/test_daily_settlement_orchestrator.py`

## 7. 运维部署与演练（Python）
- 已实现：systemd/k8s 渲染、单机/多机场景编排、rollout/failover/rollback 证据校验
- 代码与脚本：
  - `scripts/ops/render_systemd_units.py`
  - `scripts/ops/render_k8s_manifests.py`
  - `scripts/ops/rollout_orchestrator.py`
  - `scripts/ops/failover_orchestrator.py`
  - `scripts/ops/one_click_deploy.py`
- 测试：
  - `python/tests/test_systemd_render.py`
  - `python/tests/test_k8s_render.py`
  - `python/tests/test_rollout_orchestrator_script.py`
  - `python/tests/test_failover_orchestrator_script.py`
  - `python/tests/test_one_click_deploy_script.py`

## 8. 可观测与 SLO（Python + 文档）
- 已实现：SLI 目录、健康探针日志报告、SLO 证据生成
- 代码与文档：
  - `python/quant_hft/ops/sli_catalog.py`
  - `scripts/ops/reconnect_slo_report.py`
  - `docs/OPS_SLI_SLO_SPEC.md`
- 测试：
  - `python/tests/test_reconnect_slo.py`
  - `python/tests/test_run_reconnect_evidence_script.py`

## 9. 性能基准
- 已实现：hotpath benchmark 与 hybrid benchmark
- 代码：
  - `src/apps/hotpath_benchmark_main.cpp`
  - `src/apps/hotpath_hybrid_main.cpp`
- 构建目标：
  - `hotpath_benchmark`
  - `hotpath_hybrid`

## 10. 非范围说明
- 以下不计入“已实现（生产主链路）”：
  - 路线图和阶段性规划中的未来项（例如 `docs/ROADMAP_24W.md`）
  - 明确占位/模板化环境配置且尚未接入真实执行链路的部分
