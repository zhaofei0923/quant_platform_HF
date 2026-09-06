# Runtime / CTP quality validation — 2026-09-06

证据范围：Ubuntu 22.04 / GCC 11.4，仓外构建，源码为本次最终修复工作树。本文件记录离线验证；没有连接 SimNow、发送真实交易或完成五日运行验收。

## 最终收口结果

- 唯一一次最终默认 C++17 全量：**944 项，922 通过、20 跳过、2 失败**，保留[原始日志](quality/cpp17_final_ctest.log)与[完整构建](quality/cpp17_final_build.log)。两项失败分别是第 20 条 policy 测试混用查询代次，以及配置目录未登记新 policy 示例文件。修正后仅定向复验：[policy 20/20 通过](quality/policy_final20.log)、[配置目录覆盖 1/1 通过](quality/config_coverage_final.log)。没有重新运行全量，也不把原始全量写成零失败。
- 20 项跳过：外部 PostgreSQL 6 项（本构建关闭且未提供 DSN）、Arrow 专属 12 项（本构建关闭）、私有回测数据集缺失 2 项。独立真实 PostgreSQL 与 Arrow 验证见本批次交易/研究证据，不能把这些 skip 算作能力通过。
- 新 policy / 领域存储 / ExecutionEngine 的 ASan + UBSan：**62/62 通过**，含最新会话代次回归，见[测试](quality/asan_ubsan_policy_domain62.log)与[构建](quality/asan_ubsan_policy_domain_build.log)。没有重复此前已通过的完整 sanitizer 集合。
- 官方 Linux SDK 最终增量 `core_engine` 与 `verified_trade_accounting_policy_test` 编译链接通过，见[配置](quality/sdk_final_configure.log)、[增量构建](quality/sdk_final_incremental.log)。[core_engine ldd](quality/sdk_final_core_ldd.log) 解析两份正式 CTP 动态库，无缺项；[policy test ldd](quality/sdk_final_policy_ldd.log) 不依赖 CTP。此结论仅为编译链接兼容。
- 最后一轮[依赖审计](quality/dependency_audit_final.log)、[仓库纯度](quality/repo_purity_final.log)、[文档纯度](quality/doc_purity_final.log)均通过；可选依赖关闭项保留明确 skip。

## 已完成的定向验证

- 可靠队列/WAL/查询/账户调度器初始 ASan + UBSan：31/31 通过，见 [日志](quality/asan_ubsan_initial31.log)。最终扩展到 CTP gateway / trader 与 typed 查询回调：64/64 通过，见 [日志](quality/asan_ubsan_final64.log)。这是组件级检测，未声称整个线上进程通过消毒器验收。
- WSL TSan：编译成功，执行在测试开始前被 `ThreadSanitizer: unexpected memory mapping` 阻断，见 [日志](quality/tsan_startup_blocked.log)。不能计为通过；没有修改本机内核设置。
- RuntimePaths + 启动/监督/独立监控：21/21 通过，见 [日志](quality/runtime_paths_ops_tests.log)。覆盖不同 run_id 恢复路径稳定、跨账户分离、显式路径保留、旧 WAL 不被自动认领、监督/监控不预建未绑定恢复目录。
- 操作员 UserID / 投资者 InvestorID 混用：先有两个失败用例，见 [red](quality/account_identity_red.log)；修复后 gateway 19/19 通过，见 [日志](quality/account_identity_and_simulated_day.log)。真实 API 缺显式 InvestorID 拒绝；纯模拟保留兼容 fallback。模拟交易日期可由 `QUANT_HFT_SIMULATED_TRADING_DAY` 注入，默认上海当前日期，查询元数据明确 `source=simulated`。
- 三类核算 typed query API（instrument meta / commission / order commission）组件 33/33 通过，见 [日志](quality/typed_accounting_queries.log)，包含完整批次、请求账户/日期/source 保留和 trader 可靠回调队列 FIFO；官方 SDK 宏分支 [重编通过](quality/typed_queries_sdk_build.log)。
- GCC / CMake 依赖审计与仓库/文档纯度检查通过，见 quality 下对应日志；Arrow、外部 PG/Redis、metrics 关闭时的检查明确记为 skip。

## 编译链接与执行边界

官方 Linux SDK 目录为 `ctp_api/v6.7.11_20250617_api_traderapi_se_linux64`。真实宏分支全量编译链接成功，见 [configure](quality/sdk_configure.log)、[build](quality/sdk_build.log)。[core_engine ldd](quality/sdk_core_engine_ldd.log) 找到两份 CTP 动态库且无缺项；[backtest_cli ldd](quality/sdk_backtest_cli_ldd.log) 不依赖 CTP。没有使用假 SDK 或仅头文件检查替代 Linux 链接检查。

严格 accounting policy 增补前，默认 C++17 全量为 **919 项：900 通过、19 跳过、0 失败**，见 [历史完整日志](quality/cpp17_full_ctest.log) 和 [构建](quality/cpp17_build.log)。19 项跳过包括外部 PostgreSQL 5 项、Arrow 专属 12 项、私有回测数据集缺失 2 项。该历史结果不替代上方最终 944 项全量及定向修复结果。

## 复现命令

```bash
cmake -S . -B /tmp/quant-hft-runtime-build -DQUANT_HFT_BUILD_TESTS=ON -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src -DQUANT_HFT_ENABLE_ARROW_PARQUET=OFF -DQUANT_HFT_ENABLE_CTP_REAL_API=OFF -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build /tmp/quant-hft-runtime-build -j4
ctest --test-dir /tmp/quant-hft-runtime-build --output-on-failure
# 最终全量结束后仅定向补编与复验这两处；未重复全量。
cmake --build /tmp/quant-hft-runtime-build --target verified_trade_accounting_policy_test -j3
ctest --test-dir /tmp/quant-hft-runtime-build -R '^VerifiedTradeAccountingPolicyTest\.' --output-on-failure
ctest --test-dir /tmp/quant-hft-runtime-build -R '^QualityGateScriptsTest\.ConfigDocsCoverageCheckPassesCurrentRepository$' --output-on-failure
cmake -S . -B /tmp/quant-hft-runtime-real-build -DQUANT_HFT_BUILD_TESTS=ON -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src -DQUANT_HFT_ENABLE_ARROW_PARQUET=OFF -DQUANT_HFT_ENABLE_CTP_REAL_API=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build /tmp/quant-hft-runtime-real-build --target core_engine verified_trade_accounting_policy_test -j3
cmake -S . -B /tmp/quant-hft-runtime-sanitize-build -DQUANT_HFT_BUILD_TESTS=ON -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src -DQUANT_HFT_ENABLE_ARROW_PARQUET=OFF -DQUANT_HFT_ENABLE_CTP_REAL_API=OFF -DQUANT_HFT_SANITIZER=address-undefined
cmake --build /tmp/quant-hft-runtime-sanitize-build --target verified_trade_accounting_policy_test trading_domain_store_client_adapter_test execution_engine_test -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ctest --test-dir /tmp/quant-hft-runtime-sanitize-build -R '^(VerifiedTradeAccountingPolicyTest|TradingDomainStoreClientAdapterTest|ExecutionEngineTest)\.' --output-on-failure
```

先前 64 项组件 sanitizer 的实际命令：

```bash
cmake --build /tmp/quant-hft-runtime-sanitize-build --target ctp_gateway_adapter_test ctp_trader_adapter_test query_scheduler_test query_batch_collector_test durable_order_event_inbox_test account_execution_scheduler_test wal_replay_loader_test -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ctest --test-dir /tmp/quant-hft-runtime-sanitize-build -R '^(CtpGatewayAdapterTest|CTPTraderAdapterTest|QuerySchedulerTest|QueryBatchCollectorTest|DurableOrderEventInboxTest|AccountExecutionSchedulerTest|WalReplayLoaderTest)\.' --output-on-failure
```

CI 配置已增加 C++17、ASan/UBSan、targeted TSan、隔离 PostgreSQL 和手动启用的官方 SDK runner job，既有 CI 保留 Arrow C++20。远程 CI 尚未触发；本地 YAML 解析和脚本语法检查不能替代各 runner 的执行结果。SDK runner 只接受预配置正式 SDK，不下载替代包。

TSan 复现（本地启动阻断，不计入通过数量）：

```bash
g++ -std=c++17 -pthread -fsanitize=thread -fno-omit-frame-pointer -g -Iinclude -I/tmp/quant-hft-build/_deps/googletest-src/googletest/include src/core/ctp/query_scheduler.cpp tests/unit/core/query_scheduler_test.cpp /tmp/quant-hft-runtime-build/lib/libgtest_main.a /tmp/quant-hft-runtime-build/lib/libgtest.a -o /tmp/codex-runtime-query-tsan
TSAN_OPTIONS=halt_on_error=1 /tmp/codex-runtime-query-tsan
```
