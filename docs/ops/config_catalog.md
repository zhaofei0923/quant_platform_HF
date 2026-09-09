# 在线配置目录

算法正式参数由quant_strategies维护，研究配置由quant_research维护。

## `configs/accounting_policy.example.json`

- Purpose: 真实 CTP 成交核算规则的未核验模板，按环境、经纪商、账户、交易日、
  完整合约、交易所与套保类型限定适用范围。
- 使用方式: 复制为本地私有文件，填写已核验事实及来源，通过
  `QUANT_HFT_ACCOUNTING_POLICY_FILE` 设置绝对路径，重启载入。
- 示例默认 `verified=false`；占位值不能直接用于交易。新会话还需三类当日柜台
  查询匹配及证据可靠保存才能解除对应阻断。
- 完整字段、支持的费用模型和历史恢复边界见[核算规则说明](verified_accounting_policy.md)。

## `configs/data_lifecycle/policies.yaml`

- Purpose: 数据生命周期策略。
- Consumer: 数据生命周期任务（按前缀执行冷热分层与删除）。
- 说明方式: 文件内容为 JSON 结构（保持原样，不加注释）。

字段表（每个策略块如 `market`）：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `<policy>.base_prefix` | string | 是 | 无 | 非空路径 | 对象存储前缀 | `parquet/market_snapshots` |
| `<policy>.hot_retention_days` | int | 是 | 无 | `>=0` | 热层保留天数 | `7` |
| `<policy>.warm_retention_days` | int | 是 | 无 | `>= hot` | 温层保留天数 | `90` |
| `<policy>.cold_retention_days` | int | 是 | 无 | `>= warm` | 冷层保留天数 | `365` |
| `<policy>.delete_after_days` | int | 是 | 无 | `>= cold` | 删除阈值天数 | `365` |

## `configs/deploy/connections.example.yaml`

正式部署配置模型的参考模板；需填写真实引用并通过quant_config_cli validate。

## `configs/deploy/environments/prodlike_multi_host.yaml`

- Purpose: 多主机拟生产故障切换流程模板。
- Consumer: 多主机部署/故障切换演练流程。

字段表：

| 字段 | 类型 | 必填 | 含义 | 示例 |
|---|---|---|---|---|
| `environment` | string | 是 | 环境标识 | `prodlike-multi-host` |
| `deploy_target` | string | 是 | 部署目标标识 | `prodlike-multi-host` |
| `precheck_cmd` | string | 是 | 切换前检查 | `echo precheck_prodlike_multi_host` |
| `backup_sync_check_cmd` | string | 是 | 备节点同步检查 | `echo backup_sync_check_prodlike_multi_host` |
| `demote_primary_cmd` | string | 是 | 主节点降级 | `echo demote_primary_prodlike_multi_host` |
| `promote_standby_cmd` | string | 是 | 备节点提升 | `echo promote_standby_prodlike_multi_host` |
| `verify_cmd` | string | 是 | 切换后验证 | `echo verify_prodlike_multi_host` |
| `data_sync_lag_events` | string/int | 否 | 允许的同步延迟事件数 | `0` |

## `configs/deploy/environments/sim.yaml`

- Purpose: 模拟环境一键部署流程命令模板。
- Consumer: 运维编排脚本。

字段表：

| 字段 | 类型 | 必填 | 含义 | 示例 |
|---|---|---|---|---|
| `environment` | string | 是 | 环境标识 | `sim` |
| `deploy_target` | string | 是 | 目标部署集群/主机标签 | `local-sim` |
| `precheck_cmd` | string | 是 | 预检查命令 | `echo precheck_sim` |
| `deploy_cmd` | string | 是 | 部署命令 | `echo deploy_sim` |
| `fault_inject_cmd` | string | 否 | 故障注入命令 | `echo inject_fault_sim` |
| `rollback_cmd` | string | 是 | 回滚命令 | `echo rollback_sim` |
| `verify_cmd` | string | 是 | 验证命令 | `echo verify_sim` |

## `configs/deploy/environments/staging.yaml`

- Purpose: 预发环境部署流程模板。
- Consumer: 运维编排脚本。
- 字段说明: 同 `configs/deploy/environments/sim.yaml`。

## `configs/deploy/instances.example.yaml`

正式部署配置模型的参考模板；需填写真实引用并通过quant_config_cli validate。

## `configs/dev/ctp.yaml`

- Purpose: 开发环境 CTP 仿真配置。
- Consumer: `core_engine` / `CtpConfigLoader`。
- 覆盖关系: CLI `--config` > 文件值 > 代码默认。
- 常见错误: 环境变量未注入导致登录失败。
- 最小运行: `./build/core_engine configs/dev/ctp.yaml`。
- 字段说明: 见“CTP 通用字段字典”；本文件重点启用 `run_type=sim` 与 `metrics_enabled=false`。

## `configs/market/products.json`

在线宿主配置或市场参考数据；以实际柜台核验结果约束交易。

## `configs/ops/ctp_cutover.template.env`

- Purpose: 一次性切换演练模板变量。
- Consumer: `ctp_one_shot_cutover` 相关脚本。

关键变量：

| 变量 | 必填 | 风险级别 | 含义 | 示例 |
|---|---|---|---|---|
| `CUTOVER_ENV_NAME` | 是 | 中 | 演练环境标识 | `single-host-ubuntu` |
| `CUTOVER_WINDOW_LOCAL` | 是 | 高 | 切换窗口时间 | `2026-02-13T09:00:00+08:00` |
| `CTP_CONFIG_PATH` | 是 | 高 | 切换时使用配置文件 | `configs/prod/ctp.yaml` |
| `OLD_CORE_ENGINE_STOP_CMD` | 是 | 高 | 旧进程停止命令 | `bash -lc '...'` |
| `BOOTSTRAP_INFRA_CMD` | 是 | 高 | 基础设施准备命令 | `bash scripts/infra/bootstrap_prodlike.sh ...` |
| `INIT_KAFKA_TOPIC_CMD` | 否 | 中 | Kafka topic 初始化 | `bash scripts/infra/init_kafka_topics.sh ...` |
| `INIT_CLICKHOUSE_SCHEMA_CMD` | 否 | 中 | ClickHouse schema 初始化 | `bash scripts/infra/init_clickhouse_schema.sh ...` |
| `INIT_DEBEZIUM_CONNECTOR_CMD` | 否 | 中 | Debezium 初始化 | `bash scripts/infra/init_debezium_connectors.sh ...` |
| `NEW_CORE_ENGINE_START_CMD` | 是 | 高 | 新进程启动命令 | `bash -lc 'nohup ./build/core_engine ...'` |
| `PRECHECK_CMD` | 是 | 高 | 切换前联通/健康检查 | `bash -lc './build/reconnect_evidence_cli ...'` |
| `WARMUP_QUERY_CMD` | 否 | 低 | 预热检查命令 | `bash -lc 'sleep 1; ...'` |
| `POST_SWITCH_MONITOR_MINUTES` | 是 | 中 | 切换后观察分钟数 | `30` |
| `MONITOR_KEYS` | 否 | 中 | 监控指标列表 | `order_latency_p99_ms,...` |
| `CUTOVER_EVIDENCE_OUTPUT` | 是 | 中 | 证据输出文件路径 | `docs/results/ctp_cutover_result.env` |

## `configs/ops/ctp_rollback_drill.template.env`

- Purpose: 回滚演练模板变量。
- Consumer: rollback drill 脚本。

关键变量：

| 变量 | 必填 | 风险级别 | 含义 | 示例 |
|---|---|---|---|---|
| `ROLLBACK_ENV_NAME` | 是 | 中 | 演练环境标识 | `single-host-ubuntu` |
| `ROLLBACK_TRIGGER_CONDITION` | 是 | 高 | 触发回滚条件 | `order_latency_p99_ms_gt_5ms...` |
| `NEW_CORE_ENGINE_STOP_CMD` | 是 | 高 | 新进程停止命令 | `bash -lc '...'` |
| `RESTORE_PREVIOUS_BINARIES_CMD` | 否 | 中 | 旧版本恢复命令 | `bash -lc 'echo ...'` |
| `RESTORE_STRATEGY_ENGINE_COMPAT_CMD` | 否 | 中 | 兼容链路恢复命令 | `bash -lc 'echo ...'` |
| `PREVIOUS_CORE_ENGINE_START_CMD` | 是 | 高 | 旧进程重启命令 | `bash -lc 'nohup ./build/core_engine ...'` |
| `POST_ROLLBACK_VALIDATE_CMD` | 是 | 高 | 回滚后验证命令 | `bash -lc './build/reconnect_evidence_cli ...'` |
| `MAX_ROLLBACK_SECONDS` | 是 | 中 | 最大回滚耗时 | `180` |
| `ROLLBACK_EVIDENCE_OUTPUT` | 是 | 中 | 回滚证据输出路径 | `docs/results/ctp_rollback_result.env` |

## `configs/perf/baseline.json`

- Purpose: 热路径基准阈值配置。
- Consumer: `hotpath_benchmark` 性能门禁脚本。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `benchmark` | string | 是 | 无 | 非空 | 基准名称 | `hotpath_benchmark` |
| `iterations` | int | 是 | 无 | `>0` | 迭代次数 | `100000` |
| `buffer_size` | int | 是 | 无 | `>0` | 缓冲区大小 | `256` |
| `pool_capacity` | int | 是 | 无 | `>0` | 对象池容量 | `1024` |
| `baseline_ns_per_op` | double | 是 | 无 | `>0` | 基线单次耗时（ns） | `10000.0` |
| `max_regression_ratio` | double | 是 | 无 | `>=0` | 最大允许退化比例 | `0.10` |

## `configs/prod/ctp.yaml`

- Purpose: 生产环境 CTP 配置基线。
- Consumer: `core_engine` / 运维脚本。
- 常见错误: `enable_real_api` 与凭据不一致。
- 最小运行: `./build/core_engine configs/prod/ctp.yaml`。
- 字段说明: 见“CTP 通用字段字典”；本文件重点字段：
  - `run_type=live`
  - 结算扩展字段（`settlement_*`）
  - `metrics_enabled=true`

## `configs/risk_rules.yaml`

- Purpose: 风控规则模板。
- Consumer: 风控加载与策略执行。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `global.max_loss_per_order` | double | 否 | 代码默认 | `>=0` | 单笔最大亏损阈值 | `5000` |
| `global.max_order_volume` | int | 否 | 代码默认 | `>=0` | 单笔最大手数 | `100` |
| `global.max_order_rate` | int | 否 | 代码默认 | `>=0` | 报单速率阈值 | `50` |
| `global.max_cancel_rate` | int | 否 | 代码默认 | `>=0` | 撤单速率阈值 | `20` |
| `global.self_trade_prevention` | bool | 否 | `true` | `true/false` | 是否启用自成交防护 | `true` |
| `strategies` | list | 否 | 空 | 数组 | 策略级覆盖规则 | `- id: trend_001` |
| `strategies[].id` | string | 是 | 无 | 非空 | 策略标识 | `trend_001` |
| `strategies[].max_position_per_instrument` | int | 否 | 继承 global | `>=0` | 单标的仓位上限 | `200` |
| `strategies[].max_total_position` | int | 否 | 继承 global | `>=0` | 总仓位上限 | `500` |
| `strategies[].self_trade_prevention` | bool | 否 | 继承 global | `true/false` | 策略级自成交防护开关 | `false` |

## `configs/sim/calendars/hc_sessions_2026.csv`

- Purpose: `hc:SHFE` 的显式会话日历，仅适用于文件列出的自然日期与会话，不将工作日自动视为可交易日。
- Consumer: `run_tencent_simnow_schedule.sh` / `supervise_simnow_trading.sh`。
- 字段说明: `natural_date` 为会话所在自然日，`session` 为 `day_am/day_pm/night`，`trading_day` 为对应交易日；`exchange` 和 `product` 与 `# product_scope=hc:SHFE` 保持一致。缺少日期、会话或产品范围时禁止新会话启动。

## `configs/sim/ctp.yaml`

- Purpose: SimNow 生产 KAMA 运行配置，加载 `kama_trend_production`；需要通过 `--config configs/sim/ctp.yaml` 显式指定。
- Consumer: `start_simnow_trading.sh` / `supervise_simnow_trading.sh` / `core_engine` / `simnow_compare_cli`。
- 常见错误: `risk_rule_groups` 与对应 `risk_rule_<group>_*` 键不匹配。
- 最小运行: `./build/core_engine configs/sim/ctp.yaml`。
- 字段说明: 见“CTP 通用字段字典”；本文件重点字段：
  - 生产 KAMA 策略入口（`strategy_ids` / `strategy_composite_config`）
  - 生产交易品种（`product_ids`，默认启用 `c,hc`）
  - 安全空仓换月（`active_contract_mode=dominant_open_interest`、
    `dominant_contract_switch_mode=flat_only`）
  - 候选/切换门控（15% 领先、3 个 60 秒窗口、15 分钟最短持有、6 秒 Tick 新鲜度、
    完整深度行情基线和至少 30 根 canonical 5m 无信号暖机）
  - 执行算法参数（`execution_*`）
  - 默认风控模板（`risk_default_*`）
  - 自定义风控组模板（`risk_rule_<group>_*`）

## `configs/sim/ctp_sim_trade_candidates.yaml`

- Purpose: SimNow 默认多品种候选参数联调配置，当前运行 `c/hc` 两个独立 Composite 实例。
- Consumer: `core_engine` / `simnow_compare_cli`。
- 字段说明: 见“CTP 通用字段字典”；重点字段为 `strategy_composite_config_map.<strategy_id>`、`product_ids` 与行情按品种分区开关。

## `configs/sim/ctp_sim_trade_hc.yaml`

- Purpose: 腾讯云 SimNow 受控合约范围的运行观察配置；订阅合约由受控清单通过 `CTP_SIM_INSTRUMENTS` 注入。
- Consumer: `run_tencent_simnow_schedule.sh` / `start_simnow_trading.sh` / `supervise_simnow_trading.sh` / `core_engine`。
- 字段说明: 见“CTP 通用字段字典”；连接身份引用环境变量，凭据不写入本文件。运行仍须通过会话、恢复对账、核算政策与风控门禁。

## `configs/sim/ctp_trading_hours.yaml`

- Purpose: SimNow 交易时段组 1 前置配置。
- Consumer: `simnow_probe` / `core_engine`。
- 常见错误: `password_env` 指向的环境变量不存在。
- 字段说明: 见“CTP 通用字段字典”；本文件仅保留连接与基础频控字段。

## `configs/sim/ctp_trading_hours_group2.yaml`

- Purpose: SimNow 交易时段组 2 前置配置。
- Consumer: `simnow_probe`。
- 字段说明: 同 `ctp_trading_hours.yaml`，仅前置地址不同。

## `configs/sim/ctp_trading_hours_group3.yaml`

- Purpose: SimNow 交易时段组 3 前置配置。
- Consumer: `simnow_probe`。
- 字段说明: 同 `ctp_trading_hours.yaml`，仅前置地址不同。

## `configs/sim/empty_delegated_runtime.env`

- Purpose: 调度入口已经加载受控运行环境后，供下层脚本引用的空环境文件，避免重复加载可变环境覆盖。
- Consumer: `run_tencent_simnow_schedule.sh` 委派的启动与监督脚本。
- 字段说明: 本文件有意不包含变量赋值；不得加入凭据、连接参数或策略覆盖。

## `configs/trading_sessions.yaml`

- Purpose: 交易所/品种交易时段规则。
- Consumer: 交易时段判定模块与回测时段过滤。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `sessions` | list | 是 | 无 | 非空数组 | 交易时段规则集合 | `- exchange: SHFE ...` |
| `sessions[].exchange` | string | 是 | 无 | 交易所编码 | 规则所属交易所 | `SHFE` |
| `sessions[].instrument_prefix` | string | 否 | 空 | 品种前缀 | 对某些品种做精细化覆盖 | `rb` |
| `sessions[].day` | string | 是 | 无 | `HH:MM-HH:MM[,HH:MM-HH:MM...]` | 日盘时段，可用逗号分隔小节 | `09:00-10:15,10:30-11:30,13:30-15:00` |
| `sessions[].night` | string/null | 否 | `null` | `HH:MM-HH:MM[,HH:MM-HH:MM...]`/`null` | 夜盘时段 | `21:00-23:00` |
