# 三项目拆分交付验证

本记录保留三项目隔离代码和离线验证基线。后续按用户更新范围，腾讯云单账户HC候选监督服务已部署，原服务停用；首次柜台运行验收仍以[部署记录](tencent_simnow_split_deployment.md)为准。实盘账户未切换，WSL原源码基线未覆盖。

## 目录与固定依赖

| 项目 | 独立目录 |
|---|---|
| 在线交易 | `/home/kevin/worktrees/quant_platform_HF-split` |
| 策略与指标 | `/home/kevin/quant_strategies` |
| 回测研究 | `/home/kevin/quant_research` |

两个宿主消费同一份QuantStrategies 1.0.0静态包，包含Contracts、Model、Indicators、Strategies四个目标。

- 策略源码提交：`32cb073`。
- 发布包：`/home/kevin/quant_packages/releases/quant_strategies-1.0.0-Linux.tar.gz`。
- 包SHA256：`e4b9eefe601b351992778e5e009e7bc20ba676989f9db37b25366ec3bbcceadb`。
- Manifest SHA256：`c13b174e0aed50002c85c61026613d1937ac554255d3ae6f203159fd9fef6cd4`。
- 原始有效源码基线：`a211023`；被忽略规则遮蔽的原测试补充基线：`428e1f4`。

## 已执行验证

| 验证 | 实际结果 | 证据 |
|---|---|---|
| 在线完整构建与CTest | 503项：492通过、11跳过、0失败 | `quant_split_evidence/20260909/online_final_build.log`、`online_final_ctest.log` |
| 在线独立源码导出 | d802095源码归档与重新解压的策略包单独构建；503项中492通过、11跳过、0失败 | `quant_split_evidence/20260909/online_source_export/verification.json` |
| 真实CTP SDK编译与链接 | v6.7.11编译通过，未连接柜台 | [真实SDK与数据库验证](online_real_and_postgres_validation.md) |
| 实际PostgreSQL事务 | 8/8通过；应用004/007/008/009；临时数据库已关闭 | [事务日志](p4_postgres_atomic_tests.log) |
| 两账户三个策略实例启动 | 两个stub进程同时启动并正常退出；同名实例跨账户隔离，全天禁止开仓 | `quant_split_evidence/20260909/formal_stub_smoke/report.json` |
| 配置与状态迁移 | 真正LoadDeploymentConfig后迁移成功，原payload与水位不变；拒绝归属、来源和参数不一致 | DeploymentFixture、StateMigrationFixture |
| 策略包独立源码导出 | 47/47 CTest目标通过；99个安装文件与包逐项一致；解压后的外部消费者通过 | `quant_split_evidence/20260909/strategy_release_verification.json` |
| Tick结构迁移一致性 | 3,601 Tick；116订单记录、58成交、58持仓快照、日权益与WAL一致 | `quant_split_evidence/20260909/golden_tick/comparison.json` |
| 研究独立源码导出 | 894a554：264项C++中260通过、4跳过；Python 88/88通过；依赖审计通过 | `quant_split_evidence/20260909/research_final_verification/verification.json` |
| 原生Bar策略联调 | 同一KAMA/Composite处理675条合成1分钟Bar、4次成交；CSV/真实Parquet结果和状态一致 | 研究项目原生Bar联调证据 |
| 源码、依赖与配置边界 | purity、doc purity、dependency audit、三项目边界及23个配置的文档覆盖通过 | 构建脚本和配置目录 |

在线CTest的11个跳过为8项需外部数据库的测试及3项需指定Web基础设施的测试。前8项已在独立PostgreSQL中另行全部运行通过；权限测试也在隔离的root临时目录实际通过。Nginx/Authelia部署仍需指定目标环境。研究的4个跳过保留在其报告中，不计作相应功能验收。

在线源码导出仅为运维预检重建本仓Git元数据，没有读取其他项目源码；首次无Git元数据时的预检失败也保留在记录中。源码、依赖包的校验和和最终CTest结果分别列明。

## 本地交付包

| 包 | SHA256 |
|---|---|
| `/home/kevin/quant_packages/releases/quant-platform-hf-v1.0.0-offline.1.tar.gz` | `18b0424ed998a8199745e6b421fa15c683cc325e0bbe167a0c004c4bf0d2993e` |
| `/home/kevin/quant_packages/releases/quant_research-1.0.0-Linux.tar.gz` | `64ee661f3dee7b16fcd32bbd03b60e3caf7be0f7596c5acb8d24131bceea0442` |
| `/home/kevin/quant_packages/releases/quant_strategies-1.0.0-Linux.tar.gz` | `e4b9eefe601b351992778e5e009e7bc20ba676989f9db37b25366ec3bbcceadb` |

在线包来自d802095干净源码和stub构建，明确标记 `ctp_real_api_compiled=false`、`simnow_five_day_accepted=false`。真实SDK候选二进制及哈希另见SDK验证记录；没有将其切换到任何运行服务。研究包包含真实Arrow库，并已验证移动目录后仍能运行。

完整源码快照与补充测试均有SHA台账，原目录934项初次捕获源码复核无变化，原HEAD保持15909d4。详见证据目录的 `original_source_verification.json`、`baseline_addendum_verification.json` 和 `restored_script_test_verification.json`。

## 尚未完成的外部验收

1. **EDB样本与按需研究数据（范围已调整）**：2026-09-09用户明确只需样本，后续按回测品种下载，不再要求全市场365天分钟数据。E盘已重新挂载，12份旧原始收据校验通过。三所各十日样本共10,350根Bar通过QA；另补rb2701与主连各3,450根。玉米、螺纹钢单合约及rb2610→rb2701真实换月的CSV/Parquet回放、成交、权益和状态一致，旧仓自然退出后才开新合约。主连与实际合约79根字段差异单独保留，执行不使用主连价格。研究工具已强制按品种选择，后续范围须核验其日历、映射及费用；本次不做全市场覆盖或策略收益结论。详见研究项目 `docs/data_coverage_status.md` 与 `docs/edb_on_demand.md`。
2. **SimNow单账户单策略功能验收（范围已调整）**：用户明确只有现有一个SimNow账户，取消五日等待，并要求沿用原HC策略、资金随柜台实际权益。`simnow.3`已部署并启用会话监督服务；代码、状态和账本验证通过，尚未将盘前supervisor启动计作实际登录、对账或Ready。详见[部署记录](tencent_simnow_split_deployment.md)。
3. **现有账户状态迁移与实盘切换**：云端旧WAL、owner、策略状态、行情历史已迁至隔离新运行目录；真实PG重放恢复原HC空头1手且再次回放不重复记账。当前柜台资金和今昨仓核对待首次成功连接；实盘仍作为单独发布步骤。
4. **非投机开仓与跨主机主备**：非投机开仓在保证金口径未核验时明确拒绝；既有套保持仓和成交保留标记。首版绑定单活动主机，本地锁不替代分布式租约。
5. **研究恢复边界**：原生Bar模式当前只支持冷启动，不接受半恢复的资金/持仓账本；旧Tick的online_parity不支持项继续明确拒绝。交易状态封装的迁移测试不等于研究账户恢复验收。

回滚规则、配置命令和新账户systemd模板见[迁移和发布说明](../ops/three_project_migration.md)。离线证据、候选部署和实际柜台验收分别记录；本次没有实盘切换。
