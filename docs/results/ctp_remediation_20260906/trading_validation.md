# 交易域与主入口离线验证

日期：2026-09-06。此记录对应 verified accounting policy 入口接入前的工作区，基线提交见 [working_tree_base.txt](trading_artifacts/working_tree_base.txt)。它不是该基线提交本身的构建结果。二进制摘要见 [binary_hashes.txt](trading_artifacts/binary_hashes.txt)，复制证据摘要见 [trading_evidence.sha256](trading_evidence.sha256)。

## 已确认结果

| 验证 | 结果 | 证据 |
|---|---|---|
| 首轮交易、策略、CTP adapter、持久化定向回归 | 136/136 通过 | [targeted_136.log](trading_artifacts/targeted_136.log) |
| 主入口 smoke 发现问题并修复后的扩展回归 | 180/180 通过 | [targeted_180.log](trading_artifacts/targeted_180.log) |
| 独立 PostgreSQL 14 真实连接事务集成 | 5/5 通过 | [postgres_5.log](trading_artifacts/postgres_5.log) |
| 全新运行目录首次启动、运行、正常退出 | 退出码 0 | [smoke_first.log](trading_artifacts/smoke_first.log) |
| 同身份重启、运行、正常退出 | 退出码 0 | [smoke_restart.log](trading_artifacts/smoke_restart.log) |
| 同账户同时启动第二实例 | 退出码 7，账户锁拒绝 | [smoke_second_instance.log](trading_artifacts/smoke_second_instance.log) |
| 前后身份清单比较 | cmp 返回 0 | [smoke_outcomes.txt](trading_artifacts/smoke_outcomes.txt)、[identity](trading_artifacts/smoke_identity.manifest) |
| 曾导致启动失败的本地拒单 WAL 再次恢复 | 两次启动均 0 | [recovery](trading_artifacts/local_wal_recovery_after_fix.log)、[second restart](trading_artifacts/local_wal_second_restart_after_fix.log) |

离线日志确认恢复查询完成、回调排空、交易日为显式注入的 `20260906`，最终输出 `core_engine stopped cleanly`。最终 readiness 保留 `shutdown` 的 Blocked 状态，同时记录 `recovery_complete=true`。这不表示已经验收实盘自动交易。

## 本轮回归关注点

- 订单先终态、成交后到时，实际 ExecutionEngine 仍执行原子入账；重复事件可修复之前失败的 CTP 持仓投影，并复用原始 CloseAllocation。
- 事务回滚、跨连接并发、唯一成交身份冲突、收据与水位、独立 outbox consumer ack、跨日持仓转移。
- Unknown 提交保留预注册和资金/持仓预留；账户串行准入中的活动订单数上限覆盖并发和 Unknown。
- 策略 outbox 回调或快照失败可以重投；持久快照成功后才 ack。
- 行情缺口通过缺口后的完整 Bar 重新预热，拒绝部分 Bar，防止旧代际 ack 清除新缺口；保留已持仓数量、归属、追踪止损和日风险状态。
- 完成退出前 join trader 订单、查询与快照回调，避免捕获的主入口局部对象提前析构。

## 实际执行命令

在 WSL Ubuntu-22.04 的 `/home/kevin/quant_platform_HF` 中执行；构建目录与其它代理隔离。

```bash
cmake -S . -B /tmp/quant-hft-trading-build -DQUANT_HFT_BUILD_TESTS=ON \
  -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src

cmake --build /tmp/quant-hft-trading-build --target core_engine order_manager_test \
  strategy_engine_test composite_strategy_test atomic_strategies_test \
  ctp_trader_adapter_test execution_engine_test trading_domain_store_client_adapter_test \
  state_persistence_test risk_manager_test ctp_gateway_adapter_test -j4

ctest --test-dir /tmp/quant-hft-trading-build \
  -R '^(StrategyEngineTest|CompositeStrategyTest|AtomicStrategiesTest|CTPTraderAdapterTest|ExecutionEngineTest|TradingDomainStoreClientAdapterTest|StatePersistenceTest|RiskManagerTest|CtpGatewayAdapterTest|OrderManagerTest)\.' \
  --output-on-failure

g++ -std=c++17 -pthread -Iinclude \
  -I/tmp/quant-hft-build/_deps/googletest-src/googletest/include \
  tests/integration/atomic_trade_postgres_test.cpp \
  src/core/storage/libpq_timescale_sql_client.cpp \
  src/core/storage/trading_domain_store_client_adapter.cpp \
  /tmp/quant-hft-trading-build/lib/libgtest_main.a \
  /tmp/quant-hft-trading-build/lib/libgtest.a -ldl \
  -o /tmp/codex-trade-atomic-postgres-test

LD_LIBRARY_PATH=/home/kevin/.cache/quant-hft-deps/postgres/usr/lib/x86_64-linux-gnu \
QUANT_HFT_TEST_POSTGRES_DSN='host=/tmp/quant-hft-postgres-socket port=55439 dbname=codex_trade_atomic_final user=kevin' \
  /tmp/codex-trade-atomic-postgres-test

bash --noprofile --norc /tmp/codex-trading-smoke-20260906-verified/run.sh
bash --noprofile --norc /tmp/codex-trading-smoke-20260906-final/run.sh
```

PostgreSQL 使用专用临时数据库与 Unix socket，未监听 TCP；已应用 `004_trading_core_domain_tables.sql`、`007_atomic_trade_ledger.sql`，并为 orders、trades、position_detail 建立 default 分区。该测试实际运行 BEGIN/COMMIT/ROLLBACK 和跨连接串行化，不是仅依赖 fake client 的协议测试。

[smoke_run.sh](trading_artifacts/smoke_run.sh) 和 [smoke_config.yaml](trading_artifacts/smoke_config.yaml) 保留完整启动参数。脚本以 `env -i` 清空继承环境，设置 `TEST_BROKER` / `TEST_ACCOUNT`；`enable_real_api=false`，Redis/Timescale/ClickHouse 为内存模式，market bus 与 metrics 服务关闭。没有读取或 source `.env`，没有真实柜台登录或真实下单。运行产物全部位于 `/tmp/codex-trading-smoke-20260906-verified`；测试结束后只复制上述最小证据进入本目录。

## 先失败再修复的证据

1. 活动订单上限未接入时，并发提交穿透限制；[红测试](trading_artifacts/active_orders_red.log)。修复位于 ExecutionEngine 的串行准入区。
2. 初次完整 main smoke 的 45 秒 watchdog 超时后强制结束，返回 137。[调用栈](trading_artifacts/shutdown_before_fix_backtrace.log) 显示主线程等待 RiskManager 重载线程的不可中断 60 秒 sleep。改用 condition variable 唤醒后退出正常；没有通过扩大 watchdog 掩盖退出延迟。
3. 全新实例产生的本地资金拒单尚无柜台 OrderRef，旧 OrderManager 将其视为空事件键，随后 WAL 重启失败；[失败日志](trading_artifacts/restart_before_local_key_fix.log)、[红测试](trading_artifacts/local_rejection_red.log)。新增账户和 client_order_id 隔离的备用键后，原失败 WAL 无需删除或改写即可恢复。
4. 扩展测试曾出现相对 TTL 过期失败；内存 Redis 的相对 TTL 改为单调时钟，避免系统时钟回拨改变已设置的相对有效期。最终 180 项结果包含该用例。

## 验收边界

- 此 smoke 使用 demo 策略和人工构造的模拟行情；实际订单由本地准入拒绝或被完整行情门禁阻断。它验证构造、身份、恢复、调度与退出生命周期，不能证明真实撮合、断网重连或经纪商规则。
- 内存领域 store 在重启后从 WAL 恢复；真实外部领域 store 的事务正确性由独立 PostgreSQL 集成测试覆盖。本轮没有联网交易验证。
- 完整 Bar 预热采用缺口后的有效 Bar；不宣称已从外部历史服务即时回填缺口，也不以不完整 Bar 解锁。
- 截至此冻结点，真实 API 的已验证 accounting policy 装配仍属于后续增量；没有柜台样本的费用、非 SHFE 平今平昨顺序和费用日期分配不得当作已确认事实。本记录不授予自动交易放行结论。
