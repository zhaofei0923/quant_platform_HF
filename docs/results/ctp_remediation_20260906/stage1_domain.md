# 阶段 1：交易领域基础验收

本提交建立交易领域事务和版本化投影基础；在线接收、执行调度和启动恢复接线在后续
运行时提交中完成。不可把本提交单独运行旧在线入口当成完整治理已生效。

变更包含版本化成交身份、同连接原子 ApplyTrade、日期桶共用 CloseAllocation、载荷
冲突判定、已提交成交和独立投影 outbox、账户/环境绑定、交易日滚动、订单恢复、
持仓冻结及风险成交统计。SQL 迁移为 `infra/timescale/init/007_atomic_trade_ledger.sql`。
已有 CLI 字段保留，新增协议字段采用追加编号。

从暂存树 `f2292844fafea2afbeee982a7ccb1782773cc168` 导出独立目录
`/tmp/quant-hft-stage1-snapshot`，不是从包含其他阶段未提交改动的工作区构建。
沿用基线 CMake 聚合目标，C++17 构建 `core_engine`、六个定向测试二进制和
`verify_contract_sync_cli` 均通过。

| 测试组 | 通过 |
|---|---:|
| TradingDomainStoreClientAdapterTest | 21 |
| StorageClientPoolTest | 7 |
| OrderManagerTest | 10 |
| PositionManagerTest | 5 |
| CtpPositionLedgerTest | 16 |
| RiskManagerTest | 13 |
| 合计 | 72 |

协议同步检查通过。日志为 `stage1_domain_build.log`、`stage1_domain_tests.log`。
此处只记录阶段快照的验收；实际 PostgreSQL 集成结果在总体验证中单独列出。

```bash
cmake -S /tmp/quant-hft-stage1-snapshot -B /tmp/quant-hft-stage1-build \
  -DQUANT_HFT_BUILD_TESTS=ON \
  -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src
cmake --build /tmp/quant-hft-stage1-build -j8 --target core_engine \
  trading_domain_store_client_adapter_test storage_client_pool_test order_manager_test \
  position_manager_test ctp_position_ledger_test risk_manager_test verify_contract_sync_cli
```

测试从快照源码根目录逐个运行对应二进制，以免旧 CMake 的测试工作目录缺陷干扰
交易领域验证。该目录问题在后续构建目标治理中修复。
