# 腾讯云单账户HC部署记录 — 2026-09-09

截至北京时间20:19，`simnow.3` 已安装并启用新会话监督服务；旧监督服务已停止、禁用并备份。当前是盘前等待状态，尚未完成新版实际登录、行情和柜台持仓对账，不能将监督进程启动称为交易引擎Ready。

## 本次确认范围

- 现有SimNow一个账户，仅运行原 `kama_candidate_hc` 实例、`hc2701`、5分钟策略。没有观察实例或第二个账户。
- 正式资金模式为 `account_equity`，不填写初始分配、不创建固定资金分账；唯一策略使用柜台已确认账户快照。网站调整后的资金以重新查询结果为准，没有假定仍为20万元。
- 保留原算法参数、0.5%风险参数、30%账户开仓保证金上限、夜盘及09:00–09:30禁止开仓、空的强制平仓窗口。已有持仓继续按原策略自然退出。
- 取消五个交易日等待；本次在线验收范围缩为单账户单策略功能验证。多账户、同品种多策略仍以既有离线证据交付，后续另行启用。没有实盘切换、手工平仓或测试委托。

## 固定版本及运行位置

| 对象 | 已部署值 |
|---|---|
| 在线源码版本 | `6f31bc9`，打包时工作树干净 |
| 策略包 | `QuantStrategies 1.0.0`，与两宿主已锁定版本一致 |
| 云端发布目录 | `/home/ubuntu/quant_releases/quant-platform-hf-v1.0.0-simnow.3` |
| 发布归档SHA256 | `fa1e820f3e1a9ea69557498fd1146d366936ce39bd921f874e6ef5eebf076b55` |
| core_engine SHA256 | `263e242bcef47c5b6d46148123263a993d188f649796199a8d97ba4825cbacd9` |
| 新用户服务 | `quant-hft-simnow-split.service`，enabled/active，首启20:19:01 |
| 旧用户服务 | `quant-hft-simnow-trading.service`，disabled/inactive |
| 仓库外部署清单 | `/home/ubuntu/.config/quant/simnow_split_20260909/deployment.yaml` |
| 无秘密解析配置哈希 | `9c44c4c6959d371bcb68ad48c43ffe8f7ba2ea0170e56896f10c1d5bf41d50a3` |
| 账户引用 / 运行实例 | `simnow_hc` / `simnow_hc`；持仓owner仍为 `kama_candidate_hc` |
| 运行数据根 | `/home/ubuntu/quant_runtime_split` |
| 当前权威数据库 | `quant_simnow_split_20260909_v2` |

数据库使用PostgreSQL14、Unix socket `/var/run/postgresql`、55432端口与本机peer认证，不监听TCP。连接配置与凭据分开，凭据仅在云端0600受限文件中复用，没有导出到源码或本地证据包。

`.1`、`.2`中间候选包保留，均未启动交易引擎；实际启用的是包含完整会话运维流程的`.3`。首次初始化留下的 `quant_simnow_split_20260909` 数据库保留作故障证据，服务不使用它。

## 状态与账本迁移

旧服务在休市、core不存在时停止。完整冻结备份位于云端 `/home/ubuntu/quant_hft_migration_archives/20260909_split_final_before`，包括旧二进制、服务、策略、运行状态和受限连接文件。没有修改旧交易事实。

新运行身份由生产 `RuntimeDirectory::Acquire` 创建。状态转换器保留原owner、策略payload和版本水位，仅增加新版状态封装；没有复制旧身份manifest、锁、readiness或看板身份。旧市场管线、核算政策、费用缓存、WAL及8份行情文件共10,860,915字节按原字节迁入。

| 回放核对 | 结果 |
|---|---|
| 原WAL | 36条，原stream不变，next_sequence=36 |
| 原WAL SHA256 | `8e6a2f5cf809e09e5c51f391e046cfb676255cc8404263d3a5d055b770d809f9` |
| 去重后真实成交 | 3笔 |
| 历史交易手续费 | 10.129元，权威成交应用记录口径 |
| 策略经济仓 | HC空头1手，成本3373，position_version=4 |
| 资金分账 / 独立实际账 | 均为0行，适配本次单实例柜台权益模式 |
| 再次回放 | 新增成交0，11张稳定事实表内容不变 |

上述持仓是旧WAL恢复结果，不是本次最新柜台查询结果。第一次导入暴露缺少分区，第二次在序号26暴露54字符应用委托号超过旧VARCHAR(50)。已补齐004–010迁移和分区，将不透明订单引用改为TEXT；原值未截断。真实PostgreSQL回归9/9通过。在同一部分导入数据库继续提交剩余2笔成交，再次重放无变化，没有清空后伪造成功。

## 配置、会话和只读监控

最终审计加载真实受限环境并重现旧wrapper的HC订阅绑定，调用实际配置解析和验证器，79项关键字段全部相同。启动前修正了SDK交易时段标志的误配；它继续从原环境引用，账户环境始终为SimNow。

新入口 `run_packaged_supervisor.sh` 通过 `quant_config_cli supervise` 绑定正式清单和凭据，再使用原会话调度。保留08:45、13:25、20:45预热，以及原交易时段、探针、重启预算、健康检查和15:25日结。probe与日结各自使用隔离的CTP流目录；日结失败兜底只查询仓位，不执行平仓。交易包已移除日结脚本对研究benchmark的旧调用。

云端干运行检查通过：9月9日20:00不补跑日结；20:51进入9月10日交易日预热；21:01进入夜盘；9月10日08:51进入早盘预热；15:26触发日结。新调度下界为20260910，仅影响新服务的日结调度，不更改历史持仓和成交日期。

旧服务9月8日、9月9日日结只有尝试记录，没有成功标记；这项既有缺口仍保留，未复制或伪造新库日结成功记录。

只读publisher已切到新运行身份，旧state和公开历史完整备份在 `/var/lib/quant-dashboard-admin/20260909T121059Z-simnow_hc-runtime-migration`。新WAL只读组权限已核验，WAL哈希不变。Nginx与认证服务未修改，未认证JSON请求仍为401；尚无新私有快照时七类状态均明确显示missing，不显示为已清仓或健康。

## 验证状态与后续检查

- `.3`包152文件、策略包99文件校验通过；159项聚焦回归和另行9项真实PostgreSQL测试通过；发布脚本与三项目边界检查通过。
- 云端动态库加载、正式配置、状态迁移、账本幂等、五个调度边界与systemd单元验证通过。
- 20:19确认新supervisor active、重启次数0；旧服务inactive/disabled；当前core/probe/日结进程数0，符合盘前等待安排。
- 约20:00的只读探针在三个原交易时段接入地址均登录超时，未取得最新账户权益。新版首次登录、行情、柜台多空与今昨对账、Ready、带仓重启恢复以及下一次日结仍需实际运行证据；不计为已通过。

后续检查使用用户服务状态、身份目录内readiness和当前core日志，并确认正式清单仍仅一个实例、资本来源仍为柜台快照。不能为获得测试成交而强制开仓或平仓。

本地全部无秘密证据位于 `/home/kevin/quant_split_evidence/20260909/tencent_simnow_deploy`。关键文件为 `final_service_preparation.json`、`formal_effective_connection_audit.json`、`wal_domain_import_pg_v2_resumed.json`、`wal_domain_import_pg_v2_recheck.json`、`runtime_state_migration.json` 和 `dashboard_runtime_migration_report.md`。

## 回滚

停止新服务前先保存它产生的最新WAL、数据库、策略状态及未完成成交水位。旧服务保持禁用，避免两个进程共同管理账户。只有确认新版没有新增委托、成交或核算事实，且旧柜台对账仍成立，才可按迁移前版本回切；一旦存在新事实，必须使用兼容新版账本的构建恢复，不能将迁移前快照覆盖回去。不能以强制平仓完成回滚。
