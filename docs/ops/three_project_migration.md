# 三项目迁移和发布

## 交付范围

迁移隔离工作树：`/home/kevin/worktrees/quant_platform_HF-split`；研究仓：`/home/kevin/quant_research`；策略仓：`/home/kevin/quant_strategies`。三者独立Git、构建、测试、配置和文档。两宿主只消费同一个已安装静态策略包。

基线来自原工作树有效内容，包含原有未提交源码变更，冻结提交为a211023；原始HEAD为15909d4。文件SHA/排除项见 `/home/kevin/quant_split_evidence/20260909/baseline.json`，路径归属见 `migration/path_mapping.json`。源码基线快照未混入凭据、SDK、运行状态及历史数据。后续经用户授权完成腾讯云候选部署，独立运行数据迁移及实际验收状态见[部署记录](../results/tencent_simnow_split_deployment.md)。

全量构建还发现原目录的 `tests/unit/build` 被旧忽略规则遮蔽：六个Shell测试及两个已修改C++测试未进入首次快照。已按原文件逐字保存补充基线提交，独立记录发现时间与SHA；见同证据目录的 `baseline_addendum_commit.json`、`baseline_addendum_verification.json`。其余934项快照源码与原目录逐项复核一致，原HEAD未变化。

## 配置所有权

| 对象 | 唯一所有者 |
|---|---|
| 参数schema、正式参数集、算法/指标实现、状态格式 | quant_strategies |
| 数据快照、撮合规则、试验参数和研究结果 | quant_research |
| 账户、部署实例、初始资金/划拨、账户及实例风险限制 | quant_platform_HF |
| 密码/认证 | 仓库外受限文件，部署只保存引用 |

正式部署拒绝重复键、未知字段、重复身份、复用资金分配、参数/包版本及哈希不匹配。实例ID只在其账户内唯一，可跨账户复用；物理账户只能出现一次。`resolve`输出无秘密的最终参数、字段来源、哈希及状态命名空间。schema必须属于策略包manifest。

在线CLI和core_engine还将部署manifest与编译时绑定的静态包校验和比较。即使版本号相同，二进制与部署引用不同包也会拒绝启动。文件SHA用于验证来源字节；最终参数hash来自补齐默认值后的规范参数，与文件换行及YAML排版无关。

账户进程使用 `scripts/ops/run_account_deployment.sh` 或 `infra/systemd/quant-hft-account@.service`。密码由部署清单的受限credential_ref读取。SimNow会话部署使用发布包 `run_packaged_supervisor.sh` 和 `quant_config_cli supervise`，仓库外环境文件指定账户引用、部署清单、数据服务、日历和会话调度；不读取旧混合.env或旧universe策略映射。`--check-only`只校验配置和调度，不连接柜台。腾讯云部署已经启用新监督服务，旧服务保留禁用供受控回滚。

每个参数集用独立ID和算法版本；参数变更发布新参数文件，不需要重新编译算法。算法静态包升级必须重新构建两宿主。参数schema或状态不兼容在启动前阻止恢复，不能丢弃已有状态后自动开仓。

旧参数迁移示例：

```bash
build/quant_config_cli migrate /snapshot/main.yaml sim hc_5m_v001 kama_trend@1.0.0 /installed/share/quant_strategies/1.0.0/schemas/atomic_parameters.yaml /review/new-parameters
```

迁移器物化所选模式覆盖，保留组件ID、入场窗口、止盈止损及风险参数，生成来源SHA与旧ID映射。原代码从未读取的allowed_regimes参数在报告中列为忽略项，实际entry_market_regimes保持原值。正式参数拒绝params.id和模式overrides。之后只维护正式参数集；历史混合配置作为研究基线fixture保存。

## 两本账及资金

策略经济账保存实例自己的开仓批次/成本/实现与浮动盈亏/实际成交费用；账户实际账保存柜台多空及今昨桶。经济开仓日期与实际今昨扣减分开，平仓仅减少所属实例的经济数量。例：A昨多、B今多，B普通平仓实际扣昨仓时，B经济今仓减少、A成本不变，真实平仓费用归B。

成交身份去重与事务提交是记账唯一入口。订单回报只改变生命周期和冻结，已报但未记成交的数量不能被撤单释放后再次使用。资金划拨有持久journal与幂等身份，不能重新写initial额度覆盖历史。

策略资金为初始分配+划拨+自身实现/浮动盈亏−费用。账户实际可用、总保证金和总风险仍约束每笔开仓。各实例gross多空之和分别对账；净仓为零不是无风险。无法归属的账户差额不会自动分配给同品种策略，也不会自动补充策略预算。

仅运行一个策略实例且希望资金随柜台实际权益变化时，可使用正式部署资金模式：

```yaml
capital_allocations:
  hc_account_equity:
    account_ref: simnow_hc
    mode: account_equity
```

该模式禁止填写 `initial_capital`，同一物理账户必须恰好一个资金引用和一个策略实例，不能混用固定额度。省略 `mode` 仍为既有 `fixed` 模式。`resolve/list` 明确显示 `account_equity` 和 `confirmed_broker_account_snapshot`；宿主原样向唯一实例传递经身份、完整性检查的柜台账户快照，不创建固定资金分账。实例持仓解析、缺状态带仓启动拒绝、正式部署风险和账户冻结继续生效。切换模式前须确认持久账本没有旧固定分账记录；本次部署采用独立数据库重放原交易事实，不覆盖资金历史。

相反方向保留独立外部委托；自成交防护拒绝较新委托。实例退出只等待自己的持仓、冻结和待提交成交完成。账户品种换月仍为flat_only，任何实例有仓位或委托都不切换全账户主合约。

## 状态迁移与回滚

生产迁移前停止新增开仓，保留自然退出方式，不用强制平仓解决归属问题。复制受控状态/WAL快照，保存水位；迁移报告须证明前后持仓、费用、权益、成交身份一致。旧单实例账显式迁入同owner，未知人工持仓留待核对。

新状态封装记录账户/实例、算法版本、参数hash、格式版本并保留原处理水位。任何不兼容状态、无法归属持仓或不完整对账均阻止开仓。已有运行状态绝不由示例部署覆盖。

状态迁移命令如下，输出目录必须不存在且与原状态目录隔离：

```bash
build/strategy_state_migrate_cli --deployment /review/instances.yaml --account-ref sim_a \
  --instance-id stable_instance --legacy-instance-id stable_instance \
  --legacy-state-dir /snapshot/state --legacy-main /snapshot/main.yaml \
  --parameter-migration-report /review/new-parameters/hc_5m_v001.migration.json \
  --output-dir /review/new-state
```

迁移器重新计算旧配置实际参数、校验来源文件、验证原策略状态、保存封装并回读核对；报告包括新旧状态和参数SHA。自动变更持仓owner被拒绝，交易WAL与数据库不会被该命令修改。

回滚不得覆盖新版本已产生的成交。兼容新账本的旧构建才可直接恢复；不兼容版本需自然清仓、委托及成交对账完成后再切回。数据库和WAL事实保持原始顺序，不重写成交identity。

## 验收阶段

离线证据分别由三仓测试和 `/home/kevin/quant_split_evidence/20260909` 保存。Tick golden对比必须有非空成交，检查信号、订单、成交、持仓、费用和权益，而不是两个测试命令各自PASS。

EDB数据由研究仓采集到 `E:\quant_data\china_futures\edb_1m`。raw CSV.gz、Parquet、日历、历史主力映射及覆盖清单独立冻结；没有日历/映射/权限时明确报缺口，不能用当前主力回填历史。原生Bar收盘信号最早下一根实际可交易Bar成交，费用/滑点显式填写。

本次 SimNow 部署按用户更新后的范围使用现有单账户、原 HC 单策略和柜台实际权益，取消五个交易日等待，执行登录、行情、状态、账本对账及重启恢复功能验收。多账户、多策略能力保留离线证据，本次在线运行不宣称验证这些场景。实盘切换仍属于独立发布操作，不随本次仿真部署自动执行。
