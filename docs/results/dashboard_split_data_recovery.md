# 拆分部署后的看板数据恢复 — 2026-09-09

截至北京时间 22:20:37：数据链路已恢复并完成服务端连续采样验收。用户明确允许本次即时切换后，21:57:16 启用 `simnow.4` 核心修复包，core_engine（PID 3335992）持续运行，恢复状态为 Ready，未发现运行错误。随后分别部署独立发布器和监控服务，恢复三笔真实成交以及健康状态。22:18:07 最终只读验收中，七个公共数据块全部 fresh，四个私有数据块全部 ok；随后 30.04 秒内的 31 次只读采样确认健康与 pipeline 始终 fresh、night、healthy，未出现读取错误或进程切换。

此次操作沿用原单账户运行实例。原计划仍禁止 21:00–23:00 夜盘和 09:00–09:30 开仓，forced_close 为空；这不是全局 CloseOnly 模式，运行状态 Ready 也不代表策略在禁开仓窗口获得开仓许可。未发送手工委托、撤单或平仓。

## 原因与修复

1. **默认风险规则漏打包。** `simnow.3` 不含 `configs/risk_rules.yaml`，连接配置又在包外，核心报 `runtime_semantics_invalid` 后退出。线上已补入受控原文件，SHA256 为 `3e03bb6b8285469fcc03ce934f3c0efc85ef7ece2b118b7fceb0411204a9432c`。打包脚本补入规则；两种 packaged 入口固定包根目录，并保留相对部署清单路径语义。
2. **YAML 空值引号解析不一致。** 外置连接配置的 `risk_rule_groups: ''` 被旧 host 当成一个组，而 shared loader 识别为空，触发 `runtime_semantics_drift`。线上只将三个空标量等义改为双引号，YAML 解析前后完全一致；host 永久修复支持成对单双引号。
3. **凭据行内注释被当作值。** `quant_config_cli` 的 literal 凭据读取器将 AppID、认证码后的行内 `#` 注释一起传给 CTP，触发客户端认证拒绝。原用户名、密码均已存在且与迁移前相同；没有轮换凭据。线上去除两行的注释，真实 Shell 解析前后值相同。新 host 解析器仅移除引号外且空白后的注释，保留引号内或紧贴值的 `#`，不展开或执行 Shell。
4. **记录层 SQL 漏迁移。** 原初始化只应用 004–010，遗漏 003 中的五张记录/快照表。新增 PostgreSQL-only 的 `011_online_record_tables.sql`，沿用原表和索引定义，并只创建事件月分区。线上已备份后执行两次；原有 30 张 trading_core 表的行数和逐行摘要均不变。
5. **历史 WAL 被投影到尚无柜台基准的内存持仓。** 原启动流程在首次柜台持仓查询前回放跨日开平仓，昨天平仓分配无法作用于空的今昨仓账，最终 `domain_wal_recovery_failed`。修复版继续执行真实 domain commit/outbox 恢复，在首份成功、完整且核对一致的柜台快照前不建立临时柜台数量投影；保留已被快照覆盖的成交身份以避免查询重复记仓。快照之后的新成交仍执行原资金和实际平仓分配检查。另将仅有累计成交数量的委托回报限定写入 order_events，不再误写为 trade_events。
6. **发布器未识别 schema v4 的真实成交回报。** 独立修复发布器对 `OnRtnTrade` / `OnRspQryTrade` 的识别，限定真实成交身份并保持去重，不将委托累计成交量伪造为成交。备份后仅回卷八个增量游标字段，从原 36 条 WAL 中回补三笔真实成交，随后游标恢复至 36；权益历史、发布范围、归属及诊断计数均保留。
7. **独立监控服务漏迁移，HC 交易时段配置解析不兼容。** 补齐迁移缺失的监控服务，修复产品配置的中文行内注释与成对单双引号解析，按 Asia/Shanghai 解释交易时段。配置缺失或损坏仍输出 unknown，不能假定休市。新监控启动约 17 秒后产生首份有效健康状态。

未改变风险阈值、共享策略算法、账户归属、实际手续费、开仓窗口或交易许可规则。

## 线上证据与回滚材料

- 21:39:54 只读 probe：退出码 0，账户快照与 HC 行情均取得，`probe_completed=true`，客户端认证拒绝为 0，交易日 20260910；没有启动 core。
- 配置备份分别保存在云端 `quant_hft_migration_archives` 下以 `dashboard-risk-path-repair`、`dashboard-empty-scalar-repair`、`dashboard-credential-comments-repair` 结尾的受限目录。
- 数据库备份：`/home/ubuntu/quant_hft_migration_archives/20260909T213715-dashboard-record-tables-repair/database.before.dump`。原 WAL 保持 36 条和原 SHA256，不清空或改写旧事实。
- 新包：`/home/ubuntu/quant_releases/quant-platform-hf-v1.0.0-simnow.4`。归档 SHA256 `f15bf05c3703fe9f0d277de1c812780ed20cf2594f382f69b82b35ff7b8b9111`；core SHA256 `eb731a79e19c91bc359c6b5dfba62cd751aec7f519f7b08efd694460dd18832c`。
- 21:49 的候选检查确认单账户、单实例、原账户权益模式和原夜盘计划。干运行使用独立锁目录，没有运行第二个 supervisor 或 core。
- 21:57:16 已获用户明确授权，切换既有用户服务至 `.4` 核心包。core PID 3335992 持续运行，真实账户、持仓、行情和风险快照进入公共接口。
- 22:11:44 独立部署发布器：`/opt/quant-dashboard/publisher-releases/20260909-schema4-fills/dashboard_publish_cli`，SHA256 `366cced0849375daee3e084b00a3944624ece581ef181e490c35ed8e77168fd3`。备份及游标修复证据位于 `/var/lib/quant-dashboard-admin/20260909T221144-schema4-trade-backfill`；这是核心 `.4` 之后的独立更新，不属于 `.4` 归档。
- 22:16:41 独立部署监控版本 `/home/ubuntu/quant_monitor_releases/20260909-session-parser`，SHA256 `ef96352a3ddde3243fa98dbe932849fa547956cfdb4ee9f06a50e3deb4064e93`。备份位于 `/home/ubuntu/quant_hft_migration_archives/20260909T221640-dashboard-monitor-session-deploy`。22:17:14 确认健康块 fresh、night 时段、六个健康阶段、零 critical / warning；该部署同样独立于核心包。
- 22:18:07 最终验收报告 `dashboard_verification_repair_completed_fresh.json`：公共快照年龄 601 ms，账户、持仓、策略持仓、行情、订单、成交和健康均 fresh；私有账户、持仓、策略风险和行情均 ok。交易日为 20260910，权益已有 22 个采样点并继续推进。WAL 仍为 36 条、30,971 字节，SHA256 保持 `8e6a2f5cf809e09e5c51f391e046cfb676255cc8404263d3a5d055b770d809f9`，游标为 36，未验证记录、源重置及跳过记录均为 0。
- Authelia PID 2935919、Nginx PID 1114 均未重启。账户数据缺失期间没有补零或复用旧快照伪装新数据。未登录访问网页为 302、认证页为 200、JSON 为 401，私有发布标识路径为 404。新源身份、只读 ACL、公共数据筛选及私有值扫描检查通过。

旧 `.3` 包保留。核心上线替换既有用户服务的执行包路径，没有启动旧账户服务；后续发布器与监控版本独立部署并保留各自备份。回滚时保留最新 WAL、数据库、权益历史和策略状态，不能用旧数据库备份覆盖新成交等事实；对已经存在的 SQL 表回滚不作删除。

## 验证

- 真实 CTP SDK / PostgreSQL 开关均为 ON 的 Release 构建通过；71 项凭据、部署契约、CTP 配置、历史投影、记录适配器及打包测试通过。
- 历史投影测试使用真实 domain store 和 CtpPositionLedger，覆盖跨日开平、重复查询、首个完整快照之前的成交、失败/不完整/不匹配快照、新成交先于旧快照以及实时错误平仓分配仍拒绝。另有 40 项 domain store / position reconciliation 相关测试通过。
- 011 在隔离 PostgreSQL 14.24 真库验证：重复迁移、五表写入、原行含 created_at 保留、当前/历史月界分区路由、空日期拒绝均通过。`tests/integration/online_record_schema_test.sql` 仅允许测试数据库运行。
- schema v4 发布器修复通过 28 项发布器测试及 6 项 WAL 校验测试，线上确认三笔真实成交、增量游标 36，以及委托累计回报不生成假成交。
- HC 产品交易时段解析与监控修复通过 14 项相关测试；缺失、损坏配置不被解释为休市，线上健康和 night 状态已恢复。
- 22:20:37 完成 30.04 秒监控连续验证，共 31 个样本：源时间戳更新五次，间隔为 6、5、5、6、5 秒；最大源年龄 5.489 秒、公共 pipeline 数据年龄 6.483 秒，均低于现有健康陈旧阈值 10 秒，最大公共快照年龄为 0.468 秒。所有样本的健康与 pipeline 质量均 fresh，时段均 night，状态均 healthy，critical / warning 均为 0；core PID 3335992 不变，无读取错误。该结果仅覆盖这次 30.04 秒采样，不构成全天延迟保证。
- 使用当前前端 `src/api.ts` 的 `getCurrent`、`getDays`、`getArchive` 验证当前快照、日期索引及全部九个日档 JSON 文件均通过；日期索引一致，三笔成交去重后仍为三笔。这是实际接口解析验证，不替代浏览器目视检查。
- 生产构建目录的 dependency_audit、three_project_boundary_check，以及 repo/doc purity、diff-check 通过。
- 浏览器控制连接当前不可用；本轮通过服务端文件质量、读权限及 HTTPS 认证边界验证，尚未进行登录后的页面验收。

无秘密本地证据：`/home/kevin/quant_split_evidence/20260909/tencent_simnow_deploy/dashboard_recovery_validation`，其中 `remote_core_activation.json`、`remote_publisher_backfill.json`、`remote_monitor_session_deploy.json` 分别记录核心切换、发布器回补和监控部署，`monitor_continuous_verification.json` 记录 31 次连续采样。最终只读验收报告已核实位于其上级目录 `/home/kevin/quant_split_evidence/20260909/tencent_simnow_deploy/dashboard_verification_repair_completed_fresh.json`。核心持续运行、真实快照和权益时间推进、行情与风控独立质量、成交和健康数据恢复均已有线上证据，来源身份、ACL、私有值筛查及 HTTPS 认证边界均通过。浏览器控制连接不可用，因此不将服务端恢复或接口解析检查表述为登录后目视验收，也不宣称已目视验证网页的 2 秒刷新效果。
