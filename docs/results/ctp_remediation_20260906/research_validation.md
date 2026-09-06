# 研究路径修复验证与性能证据（2026-09-06）

本报告记录分支 `codex/ctp-correctness-runtime-parity` 相对原始基线 `4aaa1b7` 的研究侧修复验证。仅使用本地合成数据、模拟成交和 C++ 测试；没有连接交易前置或发送交易请求。采证时 Git HEAD、编译器及构建选项见 [环境记录](research_artifacts/environment.json) 与 [构建选项](research_artifacts/build_options.txt)。采证时工作区含尚未提交的并行修复，HEAD 不等于全部被测代码；研究源文件 SHA-256 单独记录。

## 验证结论

**最终包含风险准入对齐的版本：64 项研究/风控定向回归全部通过；Arrow 全量 CTest 919 项，905 执行通过、14 跳过、0 失败，4.53 s。** 见 [64 项定向日志](research_artifacts/focused_64.log)、[风险版全量日志](research_artifacts/risk_final_ctest.log) 和 [风险版完整构建](research_artifacts/risk_integrated_build.log)。下列 43/44/904 数字保留为实施过程中的中间证据，不代表风险对齐已在当时完成。

- 研究定向回归原 43 项全部通过；增加共享参数与在线加载结果逐项比对后，最终 **44 项全部通过**，用时 0.35 s。见 [43 项原记录](research_artifacts/focused_43.log)、[44 项最终记录](research_artifacts/focused_44.log)。
- Arrow 16.1.0 / C++20 全量构建通过，见 [完整构建日志](research_artifacts/arrow_full_build.log)。
- 首次完整 CTest 共 900 项：884 通过、14 跳过、2 失败。一个是命令未传独立安装 Arrow 的 `PKG_CONFIG_PATH`；带正确环境后依赖审计通过。另一个是旧 Arrow trace fixture 仍期望缺少基线的首个 5m 桶，实际首个完整输出为 09:05。原始失败完整保留于 [首次全量日志](research_artifacts/arrow_first_full_ctest.log)，[依赖审计重测](research_artifacts/arrow_dependency_recheck.log) 单独记录。fixture 修订后，带正确依赖环境重新完整构建、全量 CTest，最终 **904 项：890 执行通过、14 跳过、0 失败**，4.84 s。最终构建期间集成侧新增 4 项测试，因此总数由 900 变为 904；并非剔除失败项。见 [最终构建日志](research_artifacts/arrow_final_build.log)、[最终全量日志](research_artifacts/arrow_final_ctest.log)。
- 14 个跳过包含 PostgreSQL 集成环境未配置的 5 项、与 Arrow 开启互斥的关闭路径测试、缺少外部数据/产物的脚本测试。跳过不计作功能验证通过，具体原因见日志。

## 已复现的原错误

| 原问题 | 原版本证据 | 修复后覆盖 |
|---|---|---|
| 多周期共享 detector：首个 5m 桶研究侧累计 7 次、在线仅 1 次；缺失 09:02 的桶仍进入研究策略 | [原 probe](research_artifacts/baseline_fanout_probe.cpp)、[原结果](research_artifacts/baseline_fanout_result.txt) | 多周期独立 detector、完整 bar、首尾残缺桶抑制，研究与在线复用 MarketBarPipeline |
| OOS 修改文件内容但大小/mtime 不变仍命中旧缓存 | [原缓存测试失败](research_artifacts/baseline_oos_cache.log) | 内容签名、日期边界、结果回执摘要、指标方向及训练/验证角色测试 |
| 优化试参被 sim override 遮蔽：试参 default_volume=9，实际仍为 2 | [基线代码](research_artifacts/baseline_temp_config_generator.cpp)、[原测试失败](research_artifacts/baseline_parameter_profile.log) | 试参同时更新目标参数 profile，保留其他 profile |

其余新增/修正验证包括：分析价格变换 checkpoint 及不匹配拒绝；同 timestamp 不成交、严格更晚 tick 成交；状态/完整 bar 先于本 tick 风控；显式 UTC 与 legacy_exchange_local 输入等价；夜盘 TradingDay 分区保留 ActionDay；主力候选缺数据拒绝 online_parity；预热完成后才开放；Parquet 非单调分区拒绝、稳定归并和 streaming/materialized 前缀一致；gap 后残缺 bar 不解禁；任务并发与显式输入预算；结束前最后成交费用纳入净值极值。旧 replay 回归由集成测试一起覆盖，完整测试日志用于核查具体断言。

## 最后发现的默认风险差异及修复

最后只读交叉检查发现，先前仅记录边界不足以满足默认准入一致性；以下两条已按实际 `CreateRiskManager` 做对照，先复现失败再修复。原失败日志 [risk_parity_red.log](research_artifacts/risk_parity_red.log) 保留。

- 默认规则文件的 `max_order_volume=100` 优先于 CTP YAML 的默认 200。合成 101 手订单在在线 RiskManager 被拒、原回测却成交。
- 现有共享 `MAX_ORDER_NOTIONAL` 定义是 `abs(price) × volume`；原回测另外乘合约乘数。合成 `price=101, volume=3, multiplier=10, limit=2000` 在在线允许、原回测却拒。修复通过复用同一规则执行器消除两套单位公式；这里记录现有项目契约，不自行重定义经济学名义金额。

修复后的回测以提交时的价格/同账户持仓和资金上下文调用同一 RiskManager；pending 订单进入 OrderManager，因此活动订单和自成交检查可见。TokenBucket 通过同一 refill 实现注入虚拟单调时钟，不依赖回放耗费的真实时间。新的 28 项风险/行情定向集合全部通过，见 [定向记录](research_artifacts/risk_parity_28.log)。

`risk_rule_file_path` 默认为 `configs/risk_rules.yaml`，文件内容摘要进入有效参数和输入/OOS 签名。回测固定起始规则快照；运行期间检测到配置或规则变化会报错，历史热更新需要额外事件输入。启用 Sim 子账户预算在读取行情前明确拒绝；适用但缺少模型上下文的日结亏损、总仓位/杠杆、时间范围规则及需要真实回报序列的网关拥塞明确报错，均不靠报告声明后继续模拟。

新测试同时覆盖：规则同大小/mtime 内容变化使签名改变；运行中规则变化拒绝；同账户相反 pending 委托被共享 STP 拒绝；启用 Sim 预算提前拒绝；虚拟时钟限频；60 秒规则重载周期的对象销毁可在 1 秒内结束。重载线程改为可通知的条件变量等待，解决已观测的停机等待。

本轮首次全量测试还碰到并行新增的 OrderManager 本地拒单恢复红测试；交易模块完成修复后增量重链，再跑全量得到 919 项无失败。保留 [首次风险版全量日志](research_artifacts/risk_integrated_first_ctest.log) 及 [增量构建](research_artifacts/order_manager_incremental_build.log)。

## 可复核命令

从仓库根目录执行；以下只用现有依赖，不安装或运行交易环境。

```bash
export PKG_CONFIG_PATH=/home/kevin/.cache/quant-hft-deps/pkgconfig
cmake -S . -B /tmp/quant-hft-research-build \
  -DQUANT_HFT_ENABLE_ARROW_PARQUET=ON \
  -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src
cmake --build /tmp/quant-hft-research-build -j8
ctest --test-dir /tmp/quant-hft-research-build \
  -R '^(ReplayMarketParityTest|MarketBarPipelineTest|ParquetDataFeedTest|OosTop10ValidationTest|TempConfigGeneratorTest|TaskSchedulerTest|ParameterSpaceTest|RollingConfigTest|RiskManagerTest)\.' \
  -E LoadsRepoOpsRollingOptimizeConfig --output-on-failure -j4
ctest --test-dir /tmp/quant-hft-research-build --output-on-failure -j8
```

定向命令排除的 repo rolling 配置测试需要本机不存在的外部行情清单；全量 CTest 显式记录其 skip，未伪造输入或把 skip 改为 pass。

## Parquet 输入内存与扫描基准

工具源码：[cursor_benchmark.cpp](research_artifacts/cursor_benchmark.cpp)。先生成文件，再用独立进程扫描。每个文件只有一个 row group，覆盖大 column chunk 场景；扫描时间包含打开、解码与遍历。RSS 来自 Linux `getrusage(RUSAGE_SELF).ru_maxrss`，不是估计值。

| 实现 | 行数 | Parquet 字节 | 驻留输入批次行数峰值 | 进程峰值 RSS (KiB) | 扫描时间 (ms) |
|---|---:|---:|---:|---:|---:|
| 初始批次 reader，尚未缓存列指针 | 100,000 | 2,357,557 | 4,096 | 35,544 | 204.023 |
| 初始批次 reader，尚未缓存列指针 | 1,000,000 | 19,822,891 | 4,096 | 52,960 | 1,961.330 |
| 最终 buffered stream / 缓存批次列 | 100,000 | 2,357,557 | 4,096 | 34,396 | 123.231 |
| 最终 buffered stream / 缓存批次列 | 1,000,000 | 19,822,891 | 4,096 | 41,112 | 1,154.730 |

原始 JSON：[small-final](research_artifacts/small-final.json)、[large-final](research_artifacts/large-final.json)。输入行数增长 10 倍、文件大小约 8.4 倍，reader 行缓存保持 4,096，进程 RSS 增长约 19.5%。这说明本次样本没有随总行数线性保留所有输入；**不等于全进程恒定内存**。RSS 还包括 Arrow、压缩页/字典、元数据与分配器；多分区归并的输入批次数随同时活跃分区数增加。完整回测的成交、权益、trace、canonical checkpoint 去重状态等另有内存成本，未由该指标证明有界。

每个样本仅测一次，存在热文件缓存和并行构建干扰。时间用于复核量级，不能据此承诺生产延迟、整体策略吞吐或统计意义上的加速比。

```bash
E=docs/results/ctp_remediation_20260906/research_artifacts
B=/tmp/codex-research-cursor-benchmark
A=/home/kevin/.cache/quant-hft-deps/pyarrow16/pyarrow
mkdir -p "$B"
g++ -std=c++20 -O2 -DQUANT_HFT_ENABLE_ARROW_PARQUET=1 \
  -Iinclude -Itests/unit/backtest -I"$A/include" "$E/cursor_benchmark.cpp" \
  /tmp/quant-hft-research-build/libquant_hft_backtest.a \
  /tmp/quant-hft-research-build/libquant_hft_common.a \
  -L"$A" -Wl,-rpath,"$A" -larrow -lparquet -lpthread -ldl -o "$B/cursor_benchmark"
"$B/cursor_benchmark" generate "$B/small.parquet" 100000
"$B/cursor_benchmark" generate "$B/large.parquet" 1000000
"$B/cursor_benchmark" scan "$B/small.parquet" 100000
"$B/cursor_benchmark" scan "$B/large.parquet" 1000000
```

生成器为合成数据建立 vector；生成进程与测量进程分开，生成成本不混入扫描 RSS。大数据和二进制不加入仓库，原始文件及测量二进制摘要见 [外部样本 SHA-256](research_artifacts/external_input_sha256.txt)。不同 Arrow/压缩版本重生成的字节摘要可能不同，应使用相同版本复核。

## 现有 KAMA + tick 风控基准

工具源码：[kama_tick_benchmark.cpp](research_artifacts/kama_tick_benchmark.cpp)。使用实际 CompositeStrategy/KamaTrendStrategy；先输入 100 根完整 5m bar，注入一笔本地模拟成交并验证持仓 owner，然后对已持仓策略调用 OnMarketTick。报价保持在止损/止盈阈值内，0 个 intent 为预期。仅计 callback 循环时间，没有保留全量行情向量。

| Tick callbacks | 预热完整 5m bar | 持仓手数 | 发出 intent | 峰值 RSS (KiB) | 时间 (ms) |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 100 | 1 | 0 | 4,496 | 149.714 |
| 1,000,000 | 100 | 1 | 0 | 4,496 | 1,507.540 |

原始 JSON：[kama-small](research_artifacts/kama-small.json)、[kama-large](research_artifacts/kama-large.json)。平均约 1.5 µs/callback；它是稳态持仓风控调用成本，排除 feed 解码、撮合、网关、恢复与全量 trace 留存，不是实盘吞吐或响应时间验收。这两个基准不包含独立的 RiskManager 准入路径，新增准入对齐由回归测试验证，未以此基准声称端到端性能。

```bash
g++ -std=c++20 -O2 -Iinclude "$E/kama_tick_benchmark.cpp" \
  /tmp/quant-hft-research-build/libquant_hft_strategy.a \
  /tmp/quant-hft-research-build/libquant_hft_services.a \
  /tmp/quant-hft-research-build/libquant_hft_storage.a \
  /tmp/quant-hft-research-build/libquant_hft_common.a \
  -lpthread -ldl -o "$B/kama_tick_benchmark"
"$B/kama_tick_benchmark" 100000
"$B/kama_tick_benchmark" 1000000
```

## 配置及统计边界

- `behavior_profile=online_parity`、`parameter_profile=sim` 为默认；连续价格与特殊换月要求显式 `research`。仓库原优化/滚动样例已显式声明 research/backtest，保留原实验意图。
- `input_timestamp_basis` 独立声明 `legacy_exchange_local` 或 `utc`，不可仅凭 profile 猜测输入编码；旧编码兼容显式保留。
- 从指定在线 YAML 只读纯决策 allowlist，不展开环境变量，不读取运行账户或 `.env`；报告只输出纯参数及内容身份。新增在线加载结果比较器覆盖重合参数与单位，漂移时报字段名。
- 行情、指标、策略参数、完整 bar 预热、虚拟时钟和 flat-only coordinator 共享。模拟执行仍是严格更晚 tick 的确定性成交估计；真实队列、网络时延、broker 查询/撤单回报，在线风险规则以显式内容快照复用，限频采用虚拟时间；启用 Sim 预算和缺少所需历史上下文的规则会拒绝运行，真实网络/回报时序仍未被仿真。
- OOS 若用于选赢家，报告角色为 validation_selection；滚动报告为 independent_window_validation、独立冷启动。不能把反复选择使用过的 OOS 宣称为未触碰测试集。
- 内存预算以显式 per-task 输入工作集估计限制并发，不用数据文件大小冒充实际 resident memory。完整输出及轨迹另算。

## 摘要校验

[研究源文件摘要](research_artifacts/research_source_sha256.txt) 对应本次采证工作区；[证据摘要](research_artifacts/evidence.sha256) 覆盖存档日志、JSON、工具源码、环境元数据和源摘要。可在 `research_artifacts` 中执行 `sha256sum -c evidence.sha256`。运行环境、基准范围和 skip 均应随结果一起保留。

研究代码在以上验证后冻结。主程序或其他并行模块的后续修改应重新增量构建相关目标；本次结果不自动覆盖之后的代码版本。


## 计算语义版本附加验证

根代理最后补充 `computation_semantics_version=3.0.0`，统一写入 JSON、Markdown 和
BuildBacktestSpecSignature；原 HF 报告格式版本 2.0 保留。增量重建并重跑回测报告和
OOS 84 项全部通过，见 `research_artifacts/computation_version_tests.log`。
对应两份源码的摘要为 `computation_version_source.sha256`。原 919 项运行的源码摘要
继续保留在 `research_source_sha256.txt`，不把后续元数据改动冒充此前完整矩阵的采证版本。
