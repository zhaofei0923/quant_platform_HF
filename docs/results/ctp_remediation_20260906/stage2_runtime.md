# 阶段 2：可靠接收、调度和恢复验收

以阶段 1 的 `42c931a` 为基础，独立暂存树
`5cdc26d289b04fe7388304cf3668df4228ceb989` 导出至
`/tmp/quant-hft-stage2-snapshot`。暂存版本仍使用单库 CMake，显式补入运行时源文件和
测试；工作区最终模块拆分保留给后续提交。快照并未依赖未暂存的研究实现。

本阶段包括查询 request/generation 隔离、带校验和的同步 WAL 收据、可靠 FIFO inbox、
账户执行调度、完整 Bar 行情缺口恢复、成交 outbox 投递确认、稳定目录及身份锁、
回调线程有序退出，以及对应启动/监督/监控路径。共享时钟、风控和市场合同作为
研究一致性接入的基础也在此阶段落地。

没有已验证的交易费用和日期分配政策时，本阶段真实 API 一律保持
`Blocked(trade_semantics_unverified)`。后续政策装配是附加提交，不能通过清空状态
或改一个放行布尔值绕过。

## 独立验证

`core_engine`、只读 `runtime_paths_cli` 和 17 个定向测试二进制在 C++17 下构建通过。
逐个从快照源码目录运行，共 198 项全部通过，覆盖 CTP、查询、WAL、队列、目录、
成交执行、策略投影、缺口、持久化及风控。证据为 `stage2_runtime_tests.log`，构建日志
为 `stage2_runtime_build.log` 和 `stage2_runtime_gate_build.log`。

完整主入口另在 `/tmp/quant-hft-stage2-smoke` 使用 `env -i`、TEST 账户、内存存储和
模拟适配器运行；没有读取 `.env` 或进行柜台连接。

| 检查 | 结果 |
|---|---|
| 全新身份启动、运行、退出 | 0 |
| 同身份重启、运行、退出 | 0 |
| 同账户并行第二实例 | 7，按账户锁拒绝 |
| 前后身份清单比较 | 0，一致 |

完整脚本、测试配置及输出在 `stage2_smoke/`。该 smoke 证明生命周期和恢复接线；
使用人工模拟行情，本地门控拒绝订单，不能作为真实撮合、持仓接管或五日 SimNow
验收的证据。曾经导致启动失败的本地拒单 WAL 的修复重放另见
[交易验证](trading_validation.md)。

## 复现

```bash
cmake -S /tmp/quant-hft-stage2-snapshot -B /tmp/quant-hft-stage2-build \
  -DQUANT_HFT_BUILD_TESTS=ON \
  -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src
cmake --build /tmp/quant-hft-stage2-build -j8 --target core_engine runtime_paths_cli \
  ctp_gateway_adapter_test ctp_trader_adapter_test ctp_md_adapter_test query_scheduler_test \
  query_batch_collector_test durable_order_event_inbox_test account_execution_scheduler_test \
  wal_replay_loader_test runtime_identity_test runtime_paths_test position_reconciliation_test \
  execution_engine_test strategy_engine_test composite_strategy_test state_persistence_test \
  order_manager_test risk_manager_test
bash --noprofile --norc /tmp/quant-hft-stage2-smoke/run.sh
```

各测试二进制从快照源码根目录运行。`stage2_tree.txt` 保存不含新增证据文档的代码树身份。
