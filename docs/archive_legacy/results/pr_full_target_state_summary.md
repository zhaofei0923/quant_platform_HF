# PR Summary - feature/full-target-state

## 变更目标
本次提交将交易核心从“可运行”推进到“可交付”状态，重点覆盖：
- ExecutionEngine 统一风控入口与异步下单/撤单流程
- CTP 交易链路稳定性增强（登录/确认/查询回调与重连恢复）
- 日终结算流程实装（价格源、结算循环、对账阻断、差异报告）
- 存储层能力补齐（幂等、增量更新、事务/回滚、领域读写接口）
- 测试与部署配套（systemd 定时、单测扩展、路径鲁棒性修复）

## 变更范围（统计）
- 45 files changed
- 5233 insertions(+), 578 deletions(-)

## 核心模块变更
### 1) 执行与风控主链路
- `src/services/order/execution_engine.cpp`
  - 新增 `PlaceOrderAsync` / `CancelOrderAsync` / 查询异步接口
  - 集成 `RiskManager` 订单/撤单校验
  - 引入撤单重试与等待 ACK 机制，失败写入重试计数
  - 统一订单事件回流到 `OrderManager` + `PositionManager`
- `src/apps/core_engine_main.cpp`
  - 清理旧风控重复路径，主流程统一走 ExecutionEngine 风控
  - 替换弃用调用：查询改 `Enqueue*`，下单/撤单改异步接口

### 2) CTP 连接与会话恢复
- `src/core/ctp/ctp_gateway_adapter.cpp`
  - 增加连接状态、登录响应、查询完成、结算确认回调
  - 查询回调补齐请求完成信号；连接状态变更回调外抛
- `src/core/ctp/ctp_trader_adapter.cpp`
  - 支持网关注入，新增 `LoginAsync` / `RecoverOrdersAndTrades`
  - 自动重连后执行：登录 -> 结算确认 -> 订单/成交恢复
  - 增强请求 ID 管理与 Promise 生命周期管理
- `src/core/ctp/ctp_config_loader.cpp` / `src/core/ctp/ctp_config.cpp`
  - 增加撤单重试相关配置与校验

### 3) 结算闭环能力
- `src/services/settlement/daily_settlement_service.cpp`
  - 扩展运行状态机：`RECONCILING/CALCULATED/BLOCKED/FAILED/COMPLETED`
  - 新增价格加载、结算循环、持仓 rollover、资金重建
  - 新增与 CTP 对账校验，不一致则阻断并写差异
  - 支持输出对账 diff JSON 报告
- `src/services/settlement/settlement_price_provider.cpp`
  - 新增生产价格提供器，优先级：人工覆盖 > API JSON > 缓存
  - 本地缓存持久化（动态加载 sqlite）
- `src/apps/daily_settlement_main.cpp`
  - 接入价格提供器与差异报告路径参数

### 4) 存储与幂等
- `src/core/storage/trading_domain_store_client_adapter.cpp`
  - 增加 `processed_order_events` 幂等标记/查询
  - 增加持仓明细开平仓处理与撤单重试更新
- `src/core/storage/settlement_store_client_adapter.cpp`
  - 增加结算读写接口：持仓、资金、汇总、差异、系统配置等
  - 增加事务 begin/commit/rollback 封装
- `src/core/storage/redis_hash_client.cpp` / `src/core/storage/tcp_redis_hash_client.cpp`
  - 增加 `HIncrBy`，支持仓位增量同步

### 5) 测试与运维
- 单测新增/增强（含风险、执行、结算、存储、CTP、价格源）
  - 重点：`tests/unit/services/execution_engine_test.cpp`
  - 重点：`tests/unit/services/daily_settlement_service_test.cpp`
  - 重点：`tests/unit/risk/risk_manager_test.cpp`
- 新增 systemd 用户态定时任务
  - `infra/systemd/quant-hft-daily-settlement.service`
  - `infra/systemd/quant-hft-daily-settlement.timer`

## 兼容性与清理
- 移除 C++20 designated initializer 写法，统一为 C++17 兼容构造
- 清理 ExecutionEngine 在主流程中的弃用接口调用
- 修复 `RiskManagerTest.RiskRuleLoadFromYamlSuccess` 在 `ctest --test-dir build` 场景下的配置路径问题

## 验证记录
- 全量测试：
  - `ctest --test-dir build --output-on-failure`
  - 结果：`100% tests passed, 0 failed out of 202`
- 最近一次终端状态：exit code `0`

## 风险与回滚建议
- 风险点：
  - 结算阻断策略更严格，生产环境首次上线需重点关注 `BLOCKED` 触发率
  - CTP 重连后自动恢复依赖回调顺序，建议灰度观察日志
- 回滚建议：
  - 若出现异常，可先回退到旧结算触发链路与旧查询路径
  - 保留数据库 schema 变更，业务回滚优先通过应用版本回退实现

## 建议的 PR 标题
`feat(core): complete execution/risk/settlement target state with CTP recovery and reconciliation guardrails`

## 建议的 PR 描述结尾（可选）
- [x] Build passed
- [x] All tests passed (202/202)
- [x] Deprecated calls cleaned in core path
- [x] C++17 compatibility warnings removed
