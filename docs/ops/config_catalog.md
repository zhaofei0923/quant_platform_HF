# Quant HFT 配置字典（Config Catalog）

本文档是 `configs/` 下运行配置的统一解释说明与维护基线。

- 目标：保证每个配置文件都有可追溯的字段解释。
- 约束：不改变运行时语义，不改变 JSON 格式。
- 校验：由 `python3 scripts/build/verify_config_docs_coverage.py` 验证覆盖完整性。

## 使用规则

1. 新增或重命名 `configs/` 下配置文件时，必须同步更新本文档。
2. JSON 配置（`.json` 或 JSON 结构 YAML）在本文档做字段映射说明，不在源文件内加注释。
3. 若同一字段在多个环境文件复用，字段定义以“CTP 通用字段字典”为准。
4. 字段默认值优先级：CLI > 配置文件 > 程序默认。

## 覆盖清单

- `configs/dev/ctp.yaml`
- `configs/prod/ctp.yaml`
- `configs/sim/ctp.yaml`
- `configs/sim/ctp_trading_hours.yaml`
- `configs/sim/ctp_trading_hours_group2.yaml`
- `configs/sim/ctp_trading_hours_group3.yaml`
- `configs/trading_sessions.yaml`
- `configs/risk_rules.yaml`
- `configs/strategies/main_backtest_strategy.yaml`
- `configs/strategies/products_info.yaml`
- `configs/strategies/sub/kama_trend_1.yaml`
- `configs/strategies/sub/trend_1.yaml`
- `configs/strategies/instrument_info.json`
- `configs/perf/baseline.json`
- `configs/perf/backtest_benchmark_baseline.json`
- `configs/data_lifecycle/policies.yaml`
- `configs/deploy/environments/sim.yaml`
- `configs/deploy/environments/staging.yaml`
- `configs/deploy/environments/prodlike_multi_host.yaml`
- `configs/ops/ctp_cutover.template.env`
- `configs/ops/ctp_rollback_drill.template.env`
- `configs/ops/backtest_run.yaml`

---

## CTP 通用字段字典（适用于 `configs/*/ctp*.yaml`）

### 核心运行字段

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.profile` | string | 是 | 文件显式配置 | 任意标识字符串 | `CtpConfigLoader` | `simnow_7x24` |
| `ctp.environment` | string | 是 | 文件显式配置 | `sim`/`dev`/`prod` 等 | `CtpConfigLoader` | `sim` |
| `ctp.run_type` | string | 否 | 程序默认 | `live`/`sim`/`backtest` | `CtpConfigLoader` + 引擎启动校验 | `sim` |
| `ctp.is_production_mode` | bool | 否 | 程序默认 | `true`/`false` | CTP 连接模式 | `false` |
| `ctp.enable_real_api` | bool | 否 | 程序默认 | `true`/`false` | 交易接口开关 | `false` |
| `ctp.enable_terminal_auth` | bool | 否 | 程序默认 | `true`/`false` | 终端认证流程 | `true` |
| `ctp.strategy_factory` | string | 否 | `demo` | `demo`/`composite` | 策略工厂选择 | `composite` |
| `ctp.strategy_composite_config` | string | 条件必填 | 无 | 路径（相对 `ctp.yaml`） | `strategy_factory=composite` 时使用 | `../strategies/main_backtest_strategy.yaml` |

### 连接与鉴权

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.broker_id` | string | 是 | 环境变量占位符 | 非空 | CTP 登录 | `${CTP_SIM_BROKER_ID}` |
| `ctp.user_id` | string | 是 | 环境变量占位符 | 非空 | CTP 登录 | `${CTP_SIM_USER_ID}` |
| `ctp.investor_id` | string | 是 | 环境变量占位符 | 非空 | CTP 登录 | `${CTP_SIM_INVESTOR_ID}` |
| `ctp.market_front` | string | 是 | 环境变量或显式值 | `tcp://host:port` | 行情前置连接 | `tcp://182.254.243.31:30011` |
| `ctp.trader_front` | string | 是 | 环境变量或显式值 | `tcp://host:port` | 交易前置连接 | `tcp://182.254.243.31:30001` |
| `ctp.password` | string | 条件必填 | 环境变量占位符 | 非空 | CTP 登录 | `${CTP_SIM_PASSWORD}` |
| `ctp.password_env` | string | 条件必填 | 无 | 环境变量名 | 若未直接给 `password` 则从该 env 读取 | `CTP_SIM_PASSWORD` |
| `ctp.auth_code` | string | 条件必填 | 环境变量占位符 | 非空 | CTP 认证 | `${CTP_SIM_AUTH_CODE}` |
| `ctp.app_id` | string | 条件必填 | 环境变量占位符 | 非空 | CTP 认证 | `${CTP_SIM_APP_ID}` |
| `ctp.connect_timeout_ms` | int | 否 | 程序默认 | `>0` | 连接超时 | `10000` |
| `ctp.reconnect_max_attempts` | int | 否 | 程序默认 | `>=0` | 重连次数 | `8` |
| `ctp.reconnect_initial_backoff_ms` | int | 否 | 程序默认 | `>=0` | 重连初始退避 | `500` |
| `ctp.reconnect_max_backoff_ms` | int | 否 | 程序默认 | `>=0` | 重连最大退避 | `8000` |

### 频控与重试

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.query_rate_limit_qps` | int | 否 | 程序默认 | `>0` | 查询频控 | `10` |
| `ctp.query_rate_per_sec` | int | 否 | 程序默认 | `>0` | 查询令牌速率 | `5` |
| `ctp.order_insert_rate_per_sec` | int | 否 | 程序默认 | `>0` | 报单令牌速率 | `50` |
| `ctp.order_cancel_rate_per_sec` | int | 否 | 程序默认 | `>0` | 撤单令牌速率 | `50` |
| `ctp.order_bucket_capacity` | int | 否 | 程序默认 | `>0` | 报单桶容量 | `20` |
| `ctp.cancel_bucket_capacity` | int | 否 | 程序默认 | `>0` | 撤单桶容量 | `20` |
| `ctp.query_bucket_capacity` | int | 否 | 程序默认 | `>0` | 查询桶容量 | `5` |
| `ctp.cancel_retry_max` | int | 否 | 程序默认 | `>=0` | 撤单重试次数 | `3` |
| `ctp.cancel_retry_base_ms` | int | 否 | 程序默认 | `>=0` | 撤单重试基准间隔 | `1000` |
| `ctp.cancel_retry_max_delay_ms` | int | 否 | 程序默认 | `>=0` | 撤单重试最大间隔 | `5000` |
| `ctp.cancel_wait_ack_timeout_ms` | int | 否 | 程序默认 | `>=0` | 撤单 ACK 等待超时 | `1200` |

### 结算扩展（主要在 prod）

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.settlement_query_rate_per_sec` | int | 否 | 程序默认 | `>0` | 结算查询频控 | `2` |
| `ctp.settlement_query_bucket_capacity` | int | 否 | 程序默认 | `>0` | 结算查询桶容量 | `2` |
| `ctp.settlement_retry_max` | int | 否 | 程序默认 | `>=0` | 结算重试次数 | `3` |
| `ctp.settlement_retry_backoff_initial_ms` | int | 否 | 程序默认 | `>=0` | 结算重试初始退避 | `1000` |
| `ctp.settlement_retry_backoff_max_ms` | int | 否 | 程序默认 | `>=0` | 结算重试最大退避 | `5000` |
| `ctp.settlement_running_stale_timeout_ms` | int | 否 | 程序默认 | `>=0` | 结算任务陈旧阈值 | `300000` |
| `ctp.settlement_shadow_enabled` | bool | 否 | 程序默认 | `true`/`false` | 影子结算开关 | `false` |
| `ctp.settlement_confirm_required` | bool | 否 | 程序默认 | `true`/`false` | 启动前是否要求结算确认 | `true` |

### 可观测性与熔断

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.metrics_enabled` | bool | 否 | 程序默认 | `true`/`false` | 指标暴露开关 | `false` |
| `ctp.metrics_port` | int | 否 | 程序默认 | `1-65535` | 指标端口 | `8080` |
| `ctp.log_level` | string | 否 | 程序默认 | `debug/info/warn/error` | 日志级别 | `info` |
| `ctp.log_sink` | string | 否 | 程序默认 | `stderr/file` 等 | 日志输出目标 | `stderr` |
| `ctp.breaker_failure_threshold` | int | 否 | 程序默认 | `>0` | 熔断阈值 | `5` |
| `ctp.breaker_timeout_ms` | int | 否 | 程序默认 | `>=0` | 熔断冷却时间 | `1000` |
| `ctp.breaker_half_open_timeout_ms` | int | 否 | 程序默认 | `>=0` | 半开探测窗口 | `5000` |
| `ctp.breaker_strategy_enabled` | bool | 否 | 程序默认 | `true`/`false` | 策略链路熔断 | `true` |
| `ctp.breaker_account_enabled` | bool | 否 | 程序默认 | `true`/`false` | 账户链路熔断 | `true` |
| `ctp.breaker_system_enabled` | bool | 否 | 程序默认 | `true`/`false` | 系统链路熔断 | `true` |
| `ctp.recovery_quiet_period_ms` | int | 否 | 程序默认 | `>=0` | 恢复静默窗口 | `3000` |

### 数据与审计

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.kafka_bootstrap_servers` | string | 否 | 环境变量占位符 | host 列表 | Kafka 输出 | `${KAFKA_BOOTSTRAP_SERVERS}` |
| `ctp.kafka_topic_ticks` | string | 否 | 程序默认 | 非空 | Tick 主题名 | `market.ticks.v1` |
| `ctp.clickhouse_dsn` | string | 否 | 环境变量占位符 | DSN | ClickHouse 存储 | `${CLICKHOUSE_DSN}` |
| `ctp.audit_hot_days` | int | 否 | 程序默认 | `>=0` | 热数据保留天数 | `7` |
| `ctp.audit_cold_days` | int | 否 | 程序默认 | `>=0` | 冷数据保留天数 | `180` |

### 执行与风控扩展（主要在 sim）

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.instruments` | string | 否 | 程序默认 | 逗号分隔标的列表 | 订阅标的 | `SHFE.ag2406,SHFE.rb2405` |
| `ctp.strategy_ids` | string | 否 | 程序默认 | 逗号分隔策略 ID | 运行策略集合 | `demo` |
| `ctp.strategy_queue_capacity` | int | 否 | 程序默认 | `>0` | 策略事件队列容量 | `8192` |
| `ctp.account_id` | string | 否 | 程序默认 | 非空 | 策略账户上下文 | `sim-account` |
| `ctp.execution_mode` | string | 否 | 程序默认 | `direct/sliced` | 执行模式 | `direct` |
| `ctp.execution_algo` | string | 否 | 程序默认 | `direct/sliced/twap/vwap_lite` | 执行算法 | `direct` |
| `ctp.slice_size` | int | 否 | 程序默认 | `>0` | 分片手数 | `2` |
| `ctp.slice_interval_ms` | int | 否 | 程序默认 | `>=0` | 分片间隔 | `120` |
| `ctp.twap_duration_ms` | int | 否 | 程序默认 | `>=0` | TWAP 时长 | `0` |
| `ctp.vwap_lookback_bars` | int | 否 | 程序默认 | `>0` | VWAP 回看 bar 数 | `20` |
| `ctp.throttle_reject_ratio` | double | 否 | 程序默认 | `[0,1]` | 拒单节流比例 | `0.0` |
| `ctp.cancel_after_ms` | int | 否 | 程序默认 | `>=0` | 超时撤单 | `0` |
| `ctp.cancel_check_interval_ms` | int | 否 | 程序默认 | `>=0` | 撤单检查周期 | `200` |
| `ctp.risk_default_max_order_volume` | int | 否 | 程序默认 | `>=0` | 默认单笔手数上限 | `200` |
| `ctp.risk_default_max_order_notional` | double | 否 | 程序默认 | `>=0` | 默认单笔名义金额上限 | `1000000` |
| `ctp.risk_default_max_active_orders` | int | 否 | 程序默认 | `>=0` | 默认活跃委托上限 | `0` |
| `ctp.risk_default_max_position_notional` | double | 否 | 程序默认 | `>=0` | 默认持仓名义上限 | `0` |
| `ctp.risk_default_rule_group` | string | 否 | 程序默认 | 非空 | 默认规则组 | `default` |
| `ctp.risk_default_rule_version` | string | 否 | 程序默认 | 非空 | 默认规则版本 | `v1` |
| `ctp.risk_default_policy_id` | string | 否 | 程序默认 | 非空 | 默认策略 ID | `policy.global` |
| `ctp.risk_default_policy_scope` | string | 否 | 程序默认 | `global/instrument/...` | 默认策略作用域 | `global` |
| `ctp.risk_default_decision_tags` | string | 否 | 程序默认 | 逗号分隔 | 决策标签 | `default-risk` |
| `ctp.risk_rule_groups` | string | 否 | 程序默认 | 逗号分隔组名 | 自定义风控组列表 | `ag_open` |
| `ctp.risk_rule_<group>_*` | 模板字段 | 否 | 程序默认 | 按组定义 | 对应组覆盖默认风控限制 | `risk_rule_ag_open_max_order_volume: 20` |

### 市场状态检测器

| 字段 | 类型 | 必填 | 默认值来源 | 取值/范围 | 生效入口 | 示例 |
|---|---|---|---|---|---|---|
| `ctp.market_state_detector.adx_period` | int | 否 | 程序默认 | `>0` | ADX 周期 | `14` |
| `ctp.market_state_detector.adx_strong_threshold` | double | 否 | 程序默认 | `>0` | 强趋势阈值 | `40.0` |
| `ctp.market_state_detector.adx_weak_lower` | double | 否 | 程序默认 | `>=0` | 弱趋势下界 | `25.0` |
| `ctp.market_state_detector.adx_weak_upper` | double | 否 | 程序默认 | `>= adx_weak_lower` | 弱趋势上界 | `40.0` |
| `ctp.market_state_detector.kama_er_period` | int | 否 | 程序默认 | `>0` | KAMA ER 周期 | `10` |
| `ctp.market_state_detector.kama_fast_period` | int | 否 | 程序默认 | `>0` | KAMA 快线周期 | `2` |
| `ctp.market_state_detector.kama_slow_period` | int | 否 | 程序默认 | `>0` | KAMA 慢线周期 | `30` |
| `ctp.market_state_detector.kama_er_strong` | double | 否 | 程序默认 | `[0,1]` | ER 强趋势阈值 | `0.6` |
| `ctp.market_state_detector.kama_er_weak_lower` | double | 否 | 程序默认 | `[0,1]` | ER 弱趋势下界 | `0.3` |
| `ctp.market_state_detector.atr_period` | int | 否 | 程序默认 | `>0` | ATR 周期 | `14` |
| `ctp.market_state_detector.atr_flat_ratio` | double | 否 | 程序默认 | `>=0` | 平缓波动阈值 | `0.001` |
| `ctp.market_state_detector.require_adx_for_trend` | bool | 否 | 程序默认 | `true/false` | 趋势判断是否必须 ADX | `true` |
| `ctp.market_state_detector.use_kama_er` | bool | 否 | 程序默认 | `true/false` | 是否启用 ER 辅助判断 | `true` |
| `ctp.market_state_detector.min_bars_for_flat` | int | 否 | 程序默认 | `>0` | 平缓判定最少 bar | `20` |

---

## 文件级说明

## `configs/dev/ctp.yaml`

- Purpose: 开发环境 CTP 仿真配置。
- Consumer: `core_engine` / `CtpConfigLoader`。
- 覆盖关系: CLI `--config` > 文件值 > 代码默认。
- 常见错误: 环境变量未注入导致登录失败。
- 最小运行: `./build/core_engine configs/dev/ctp.yaml`。
- 字段说明: 见“CTP 通用字段字典”；本文件重点启用 `run_type=sim` 与 `metrics_enabled=false`。

## `configs/prod/ctp.yaml`

- Purpose: 生产环境 CTP 配置基线。
- Consumer: `core_engine` / 运维脚本。
- 常见错误: `enable_real_api` 与凭据不一致。
- 最小运行: `./build/core_engine configs/prod/ctp.yaml`。
- 字段说明: 见“CTP 通用字段字典”；本文件重点字段：
  - `run_type=live`
  - 结算扩展字段（`settlement_*`）
  - `metrics_enabled=true`

## `configs/sim/ctp.yaml`

- Purpose: SimNow 7x24 回归/联调配置。
- Consumer: `core_engine` / `simnow_compare_cli`。
- 常见错误: `risk_rule_groups` 与对应 `risk_rule_<group>_*` 键不匹配。
- 最小运行: `./build/core_engine configs/sim/ctp.yaml`。
- 字段说明: 见“CTP 通用字段字典”；本文件重点字段：
  - 执行算法参数（`execution_*`）
  - 默认风控模板（`risk_default_*`）
  - 自定义风控组模板（`risk_rule_<group>_*`）

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

## `configs/trading_sessions.yaml`

- Purpose: 交易所/品种交易时段规则。
- Consumer: 交易时段判定模块与回测时段过滤。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `sessions` | list | 是 | 无 | 非空数组 | 交易时段规则集合 | `- exchange: SHFE ...` |
| `sessions[].exchange` | string | 是 | 无 | 交易所编码 | 规则所属交易所 | `SHFE` |
| `sessions[].instrument_prefix` | string | 否 | 空 | 品种前缀 | 对某些品种做精细化覆盖 | `rb` |
| `sessions[].day` | string | 是 | 无 | `HH:MM-HH:MM` | 日盘时段 | `09:00-15:00` |
| `sessions[].night` | string/null | 否 | `null` | `HH:MM-HH:MM`/`null` | 夜盘时段 | `21:00-23:00` |

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

## `configs/strategies/main_backtest_strategy.yaml`

- Purpose: Composite V2 回测主策略入口。
- Consumer: `strategy_main_config_loader` + `composite_config_loader` + `backtest_replay_support`。
- 覆盖关系: `CLI > 主配置 > 默认值`（仅保留字段）。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `run_type` | string | 是 | 无 | `backtest` | 运行类型校验 | `backtest` |
| `market_state_mode` | bool | 否 | `true` | `true/false` | 是否启用市场状态门控 | `true` |
| `backtest.initial_equity` | double | 是 | 无 | `>0` | 回测初始权益 | `200000` |
| `backtest.symbols` | list[string] | 是 | 无 | 品种或合约 | 数据选择范围 | `[c]` |
| `backtest.start_date` | string | 是 | 无 | `YYYYMMDD` | 回测起始日期 | `20240101` |
| `backtest.end_date` | string | 是 | 无 | `YYYYMMDD` | 回测结束日期 | `20240131` |
| `backtest.product_config_path` | string | 是 | 无 | 路径 | 产品费率/保证金配置 | `./instrument_info.json` |
| `composite.merge_rule` | string | 否 | `kPriority` | `kPriority` | 信号合并规则 | `kPriority` |
| `composite.enable_non_backtest` | bool | 否 | `false` | `true/false` | 是否允许 `sim/live` 模式运行 Composite | `false` |
| `composite.sub_strategies[]` | list | 是 | 空 | 数组 | 完整子策略列表 | 见示例 |
| `composite.sub_strategies[].id` | string | 是 | 无 | 非空 | 子策略实例 ID | `kama_trend_1` |
| `composite.sub_strategies[].enabled` | bool | 否 | `true` | `true/false` | 子策略开关 | `true` |
| `composite.sub_strategies[].type` | string | 是 | 无 | 注册类型名 | 子策略类型 | `KamaTrendStrategy` |
| `composite.sub_strategies[].config_path` | string | 条件必填 | 无 | 路径 | 子策略参数配置路径 | `./sub/kama_trend_1.yaml` |
| `composite.sub_strategies[].params` | map | 条件必填 | 无 | 键值对 | 内联参数 | `risk_per_trade_pct: 0.01` |
| `composite.sub_strategies[].entry_market_regimes` | list[string] | 否 | 空 | 枚举集合 | 仅开仓信号的市场状态门控 | `[kStrongTrend, kWeakTrend]` |
| `composite.sub_strategies[].overrides` | map | 否 | 空 | `backtest/sim/live` | 运行模式参数覆盖容器 | 见下方示例 |
| `composite.sub_strategies[].overrides.<run_mode>.params` | map | 否 | 空 | 标量键值对 | 覆盖对应 run_mode 下原子策略 `params` | `default_volume: 2` |

说明：

- 旧字段 `opening_strategies/stop_loss_strategies/take_profit_strategies/time_filters/risk_control_strategies` 在 V2 直接报错。
- `backtest.max_loss_percent` 已移除，风险预算下沉到子策略参数 `risk_per_trade_pct`。
- 当 `run_type != backtest` 且 `composite.enable_non_backtest=false` 时，初始化会 fail-fast。
- `overrides` 仅允许键 `backtest|sim|live`，且 `params` 仅允许标量值；非法键会 fail-fast。
- 参数合并顺序：`base params + overrides[run_mode].params`（后者覆盖前者）。
- `entry_market_regimes` 仅影响 `kOpen`；`StopLoss/TakeProfit/Close/ForceClose` 不受该门控影响。

升级后使用示例：

示例 A（回测兼容模式，默认行为不变）：

```yaml
run_type: backtest
market_state_mode: true
backtest:
  initial_equity: 200000
  symbols: [c]
  start_date: 20240101
  end_date: 20240131
  product_config_path: ./instrument_info.json
composite:
  merge_rule: kPriority
  enable_non_backtest: false
  sub_strategies:
    - id: kama_trend_1
      enabled: true
      type: KamaTrendStrategy
      config_path: ./sub/kama_trend_1.yaml
      entry_market_regimes: [kStrongTrend, kWeakTrend]
```

示例 B（开启 sim/live 并按运行模式覆盖参数）：

```yaml
run_type: sim
market_state_mode: true
backtest:
  initial_equity: 200000
  symbols: [c]
  start_date: 20240101
  end_date: 20240131
  product_config_path: ./instrument_info.json
composite:
  merge_rule: kPriority
  enable_non_backtest: true
  sub_strategies:
    - id: kama_trend_1
      enabled: true
      type: KamaTrendStrategy
      config_path: ./sub/kama_trend_1.yaml
      overrides:
        backtest:
          params:
            take_profit_atr_multiplier: 20.0
        sim:
          params:
            default_volume: 2
        live:
          params:
            risk_per_trade_pct: 0.005
```

## `configs/strategies/products_info.yaml`

- Purpose: 产品信息 YAML 镜像（与 `instrument_info.json` 对齐）。
- Consumer: `product_fee_config_loader` / 同步校验脚本。
- 维护顺序: 先改 `instrument_info.json`，再执行 `verify_products_info_sync.py`。

字段表（单产品）：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `products` | map | 是 | 无 | 产品字典 | 所有产品配置容器 | `products: {RB: ...}` |
| `products.<P>.product` | string | 否 | `<P>` | 非空 | 产品代码 | `RB` |
| `products.<P>.volume_multiple` | double | 是 | 无 | `>0` | 合约乘数 | `10` |
| `products.<P>.long_margin_ratio` | double | 是 | 无 | `>0` | 多头保证金率 | `0.16` |
| `products.<P>.short_margin_ratio` | double | 是 | 无 | `>0` | 空头保证金率 | `0.16` |
| `products.<P>.trading_sessions[]` | list[string] | 否 | 空 | 时段字符串 | 交易时段列表 | `21:00:00-23:00:00` |
| `products.<P>.commission.open_ratio_by_money` | double | 是 | 无 | `>=0` | 开仓按金额费率 | `0.0001` |
| `products.<P>.commission.open_ratio_by_volume` | double | 是 | 无 | `>=0` | 开仓按手费率 | `0` |
| `products.<P>.commission.close_ratio_by_money` | double | 是 | 无 | `>=0` | 平仓按金额费率 | `0.0001` |
| `products.<P>.commission.close_ratio_by_volume` | double | 是 | 无 | `>=0` | 平仓按手费率 | `0` |
| `products.<P>.commission.close_today_ratio_by_money` | double | 是 | 无 | `>=0` | 平今按金额费率 | `0.0001` |
| `products.<P>.commission.close_today_ratio_by_volume` | double | 是 | 无 | `>=0` | 平今按手费率 | `0` |

## `configs/strategies/sub/kama_trend_1.yaml`

- Purpose: `KamaTrendStrategy` 完整子策略参数（entry + sizing + stop/take）。
- Consumer: `atomic_factory` / `KamaTrendStrategy::Init`。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `params.id` | string | 否 | 策略实例 ID | 非空 | 子策略 ID | `kama_trend_1` |
| `params.er_period` | int | 否 | `10` | `>0` | KAMA ER 周期 | `10` |
| `params.fast_period` | int | 否 | `2` | `>0` | KAMA 快周期 | `2` |
| `params.slow_period` | int | 否 | `30` | `>0` | KAMA 慢周期 | `30` |
| `params.std_period` | int | 否 | `20` | `>0` | KAMA 标准差窗口 | `20` |
| `params.kama_filter` | double | 否 | `0.5` | `>=0` | 趋势过滤阈值系数 | `0.5` |
| `params.risk_per_trade_pct` | double | 否 | `0.01` | `(0,1]` | 单次风险资金比例 | `0.01` |
| `params.default_volume` | int | 否 | `1` | `>0` | 默认开仓手数 | `1` |
| `params.stop_loss_mode` | string | 否 | `trailing_atr` | `trailing_atr/none` | 止损模型 | `trailing_atr` |
| `params.stop_loss_atr_period` | int | 否 | `14` | `>0` | 止损 ATR 周期 | `14` |
| `params.stop_loss_atr_multiplier` | double | 否 | `2.0` | `>0` | 止损 ATR 倍数 | `2.0` |
| `params.take_profit_mode` | string | 否 | `atr_target` | `atr_target/none` | 止盈模型 | `atr_target` |
| `params.take_profit_atr_period` | int | 否 | `14` | `>0` | 止盈 ATR 周期 | `14` |
| `params.take_profit_atr_multiplier` | double | 否 | `3.0` | `>0` | 止盈 ATR 倍数 | `20.0` |

说明：

- `contract_multiplier` 由 `product_config_path` 对应产品信息注入，不在子策略配置里维护。
- 回测标的由主策略 `backtest.symbols` 控制，子策略默认按 `state.instrument_id` 工作。

## `configs/strategies/sub/trend_1.yaml`

- Purpose: `TrendStrategy` 完整子策略参数（entry + sizing + stop/take）。
- Consumer: `atomic_factory` / `TrendStrategy::Init`。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `params.id` | string | 否 | 策略实例 ID | 非空 | 子策略 ID | `trend_1` |
| `params.er_period` | int | 否 | `10` | `>0` | KAMA ER 周期 | `10` |
| `params.fast_period` | int | 否 | `2` | `>0` | KAMA 快周期 | `2` |
| `params.slow_period` | int | 否 | `30` | `>0` | KAMA 慢周期 | `30` |
| `params.kama_filter` | double | 否 | `0.0` | `>=0` | 开仓阈值系数 | `0.0` |
| `params.risk_per_trade_pct` | double | 否 | `0.01` | `(0,1]` | 单次风险资金比例 | `0.01` |
| `params.default_volume` | int | 否 | `1` | `>0` | 默认开仓手数 | `1` |
| `params.stop_loss_mode` | string | 否 | `trailing_atr` | `trailing_atr/none` | 止损模型 | `trailing_atr` |
| `params.stop_loss_atr_period` | int | 否 | `14` | `>0` | 止损 ATR 周期 | `14` |
| `params.stop_loss_atr_multiplier` | double | 否 | `2.0` | `>0` | 止损 ATR 倍数 | `2.0` |
| `params.take_profit_mode` | string | 否 | `atr_target` | `atr_target/none` | 止盈模型 | `atr_target` |
| `params.take_profit_atr_period` | int | 否 | `14` | `>0` | 止盈 ATR 周期 | `14` |
| `params.take_profit_atr_multiplier` | double | 否 | `3.0` | `>0` | 止盈 ATR 倍数 | `3.0` |

说明：

- 回测标的由主策略 `backtest.symbols` 控制，子策略默认按 `state.instrument_id` 工作。

## `configs/strategies/instrument_info.json`

- Purpose: 产品主数据源（推荐维护入口）。
- Consumer: `product_fee_config_loader`。
- 说明方式: 本节文档映射，不修改 JSON 本体。

字段表（每个产品对象）：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `<P>.product` | string | 否 | `<P>` | 非空 | 产品代码 | `RB` |
| `<P>.volume_multiple` | double | 是 | 无 | `>0` | 合约乘数 | `10` |
| `<P>.long_margin_ratio` | double | 是 | 无 | `>0` | 多头保证金率 | `0.16` |
| `<P>.short_margin_ratio` | double | 是 | 无 | `>0` | 空头保证金率 | `0.16` |
| `<P>.trading_sessions[]` | list[string] | 否 | 空 | 时段字符串 | 交易时段列表 | `21:00:00-23:00:00` |
| `<P>.commission.open_ratio_by_money` | double | 是 | 无 | `>=0` | 开仓按金额费率 | `0.0001` |
| `<P>.commission.open_ratio_by_volume` | double | 是 | 无 | `>=0` | 开仓按手费率 | `0` |
| `<P>.commission.close_ratio_by_money` | double | 是 | 无 | `>=0` | 平仓按金额费率 | `0.0001` |
| `<P>.commission.close_ratio_by_volume` | double | 是 | 无 | `>=0` | 平仓按手费率 | `0` |
| `<P>.commission.close_today_ratio_by_money` | double | 是 | 无 | `>=0` | 平今按金额费率 | `0.0001` |
| `<P>.commission.close_today_ratio_by_volume` | double | 是 | 无 | `>=0` | 平今按手费率 | `0` |

单产品示例解剖（`RB`）：

- `volume_multiple=10`: 每 1 点价格波动对应 10 元/手。
- `long_margin_ratio=0.16`, `short_margin_ratio=0.16`: 用于回测保证金占用。
- `commission.*`: 决定开平仓手续费计提口径。
- `trading_sessions`: 交易时段过滤基础数据。

## `configs/perf/baseline.json`

- Purpose: 热路径基准阈值配置。
- Consumer: `run_hotpath_bench.py`。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `benchmark` | string | 是 | 无 | 非空 | 基准名称 | `hotpath_benchmark` |
| `iterations` | int | 是 | 无 | `>0` | 迭代次数 | `100000` |
| `buffer_size` | int | 是 | 无 | `>0` | 缓冲区大小 | `256` |
| `pool_capacity` | int | 是 | 无 | `>0` | 对象池容量 | `1024` |
| `baseline_ns_per_op` | double | 是 | 无 | `>0` | 基线单次耗时（ns） | `10000.0` |
| `max_regression_ratio` | double | 是 | 无 | `>=0` | 最大允许退化比例 | `0.10` |

## `configs/perf/backtest_benchmark_baseline.json`

- Purpose: 回测性能门禁基线。
- Consumer: `check_backtest_baseline.sh` / 回测性能脚本。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `benchmark` | string | 是 | 无 | 非空 | 基准名称 | `backtest_deterministic` |
| `max_ticks` | int | 是 | 无 | `>0` | 最大回放 tick 数 | `120000` |
| `runs` | int | 是 | 无 | `>0` | 正式跑次数 | `5` |
| `warmup_runs` | int | 是 | 无 | `>=0` | 预热次数 | `1` |
| `min_ticks_read` | int | 是 | 无 | `>=0` | 最少读取 ticks | `1000` |
| `max_p95_ms` | double | 是 | 无 | `>0` | P95 门限（ms） | `2500.0` |
| `dataset_signature` | string | 是 | 无 | 非空 | 数据集签名 | `rb_perf_large_csv_v1` |

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

## `configs/deploy/environments/prodlike_multi_host.yaml`

- Purpose: 多主机拟生产故障切换流程模板。
- Consumer: `failover_orchestrator.py`。

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

## `configs/ops/backtest_run.yaml`

- Purpose: 一键编译 + parquet 回测运行配置。
- Consumer: `scripts/build/run_backtest_from_config.sh`。
- 约束: `engine_mode` 必须为 `parquet`（parquet-only policy）。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `build_dir` | string | 否 | `build-gcc` | 目录路径 | cmake 构建目录 | `build-gcc` |
| `cmake_build_type` | string | 否 | `Release` | `Debug/Release/...` | CMake 构建类型 | `Release` |
| `build_tests` | bool | 否 | `false` | `true/false` | 是否开启测试目标 | `false` |
| `enable_arrow_parquet` | bool | 否 | `true` | `true/false` | 是否开启 Arrow/Parquet 编译开关 | `true` |
| `auto_install_arrow_parquet_deps` | bool | 否 | `true` | `true/false` | 构建失败时自动安装依赖并重试一次 | `true` |
| `engine_mode` | string | 是 | 无 | `parquet` | 回测数据引擎模式 | `parquet` |
| `dataset_root` | string | 是 | 无 | 目录路径 | Parquet 数据根目录 | `backtest_data/parquet_v2` |
| `strategy_main_config_path` | string | 是 | 无 | 文件路径 | 主策略配置文件 | `configs/strategies/main_backtest_strategy.yaml` |
| `output_json` | string | 是 | 无 | 文件路径 | 回测 JSON 输出路径 | `docs/results/backtest_auto.json` |
| `output_md` | string | 是 | 无 | 文件路径 | 回测 Markdown 输出路径 | `docs/results/backtest_auto.md` |
| `export_csv_dir` | string | 否 | 空 | 目录路径 | 回测明细 CSV 导出目录 | `docs/results/backtest_auto_csv` |
| `run_id` | string | 否 | 空 | 任意字符串 | 回测运行 ID | `bt-20260221-001` |
| `max_ticks` | int | 否 | 空 | `>0` | 最大回放 tick 数 | `5000` |
| `start_date` | string | 否 | 空 | `YYYYMMDD` | 回测开始日期 | `20240101` |
| `end_date` | string | 否 | 空 | `YYYYMMDD` | 回测结束日期 | `20240131` |
| `deterministic_fills` | bool | 否 | `true` | `true/false` | 是否开启确定性成交 | `true` |
| `strict_parquet` | bool | 否 | `true` | `true/false` | parquet 严格模式 | `true` |
| `rollover_mode` | string | 否 | `strict` | `strict/carry` | 换月模式 | `strict` |
| `rollover_price_mode` | string | 否 | `bbo` | `bbo/mid/last` | 换月价格模式 | `bbo` |
| `rollover_slippage_bps` | double | 否 | `0` | `>=0` | 换月滑点（bps） | `0` |
| `emit_trades` | bool | 否 | `true` | `true/false` | 是否输出 trades 明细 | `true` |
| `emit_orders` | bool | 否 | `true` | `true/false` | 是否输出 orders 明细 | `true` |
| `emit_position_history` | bool | 否 | `false` | `true/false` | 是否输出持仓快照明细 | `false` |

## `configs/ops/rolling_backtest.yaml`

- Purpose: 滚动窗口回测与滚动优化运行配置。
- Consumer: `rolling_backtest_cli`。
- 约束: parquet-only + manifest required。

字段表：

| 字段 | 类型 | 必填 | 默认值 | 取值 | 含义 | 示例 |
|---|---|---|---|---|---|---|
| `mode` | string | 是 | 无 | `fixed_params/rolling_optimize` | 运行模式 | `rolling_optimize` |
| `backtest_base.engine_mode` | string | 是 | `parquet` | `parquet` | 数据引擎 | `parquet` |
| `backtest_base.dataset_root` | string | 是 | 无 | 目录路径 | 数据根目录 | `backtest_data/parquet_v2` |
| `backtest_base.dataset_manifest` | string | 否 | `${dataset_root}/_manifest/partitions.jsonl` | 文件路径 | manifest 路径 | `backtest_data/parquet_v2/_manifest/partitions.jsonl` |
| `backtest_base.symbols` | list[string] | 否 | 空 | 合约或品种列表 | 回放标的过滤 | `[rb]` |
| `backtest_base.strategy_factory` | string | 否 | `composite` | 已注册策略工厂名 | 策略工厂 | `composite` |
| `backtest_base.strategy_composite_config` | string | `strategy_factory=composite` 时必填 | 空 | 文件路径 | 组合策略配置 | `configs/strategies/main_backtest_strategy.yaml` |
| `backtest_base.emit_trades` | bool | 否 | `false` | `true/false` | 是否输出 trades | `false` |
| `backtest_base.emit_orders` | bool | 否 | `false` | `true/false` | 是否输出 orders | `false` |
| `backtest_base.emit_position_history` | bool | 否 | `false` | `true/false` | 是否输出持仓快照 | `false` |
| `window.type` | string | 是 | `rolling` | `rolling/expanding` | 窗口类型 | `rolling` |
| `window.train_length_days` | int | 是 | `180` | `>0` | 训练窗口交易日长度 | `180` |
| `window.test_length_days` | int | 是 | `30` | `>0` | 测试窗口交易日长度 | `30` |
| `window.step_days` | int | 是 | `30` | `>0` | 滑动步长（交易日） | `30` |
| `window.min_train_days` | int | expanding 模式建议必填 | `180` | `>0` | expanding 最小训练长度 | `180` |
| `window.start_date` | string | 是 | 无 | `YYYYMMDD` | 回测起始交易日 | `20230101` |
| `window.end_date` | string | 是 | 无 | `YYYYMMDD` | 回测结束交易日 | `20241231` |
| `optimization.algorithm` | string | `rolling_optimize` 必填 | `grid` | `grid` | 优化算法 | `grid` |
| `optimization.metric` | string | `rolling_optimize` 必填 | `hf_standard.profit_factor` | 指标路径/别名 | 优化目标 | `hf_standard.profit_factor` |
| `optimization.maximize` | bool | 否 | `true` | `true/false` | 最大化或最小化 | `true` |
| `optimization.max_trials` | int | `rolling_optimize` 必填 | `100` | `>0` | 每窗口 trial 上限 | `100` |
| `optimization.parallel` | int | `rolling_optimize` 必填 | `1` | `>0` | 窗口内并发 trial 数 | `2` |
| `optimization.param_space` | string | `rolling_optimize` 必填 | 无 | 文件路径 | 参数空间配置 | `runtime/optim/param_space.yaml` |
| `optimization.target_sub_config_path` | string | 否 | 空 | 文件路径 | 目标子策略路径（与 param_space 一致性校验） | `configs/strategies/sub/kama_trend_1.yaml` |
| `output.report_json` | string | 是 | 无 | 文件路径 | 汇总 JSON 报告路径 | `docs/results/rolling_backtest_report.json` |
| `output.report_md` | string | 是 | 无 | 文件路径 | 汇总 Markdown 报告路径 | `docs/results/rolling_backtest_report.md` |
| `output.best_params_dir` | string | 否 | 空 | 目录路径 | 每窗口 best params 输出目录 | `runtime/rolling/best_params` |
| `output.keep_temp_files` | bool | 否 | `false` | `true/false` | 是否保留临时 trial 产物 | `false` |
| `output.window_parallel` | int | 否 | `1` | `>0` | 窗口并发数（`rolling_optimize` 强制降级到 1） | `1` |

---

## 常见错误与排查

1. `strategy_factory=composite` 但未配置 `strategy_composite_config`。
2. `backtest.initial_equity <= 0`，或传入了已移除的 `max_loss_percent` 参数。
3. `product_config_path` 中缺少目标 `instrument_id`/`symbol` 映射。
4. `instrument_info.json` 与 `products_info.yaml` 漂移（运行 `verify_products_info_sync.py`）。
5. 新增配置文件未更新本文档（运行 `verify_config_docs_coverage.py`）。

## 最小可运行示例

### 运行 core_engine（模拟）

```bash
./build/core_engine configs/sim/ctp.yaml
```

### 运行回测（主策略配置）

```bash
./build/factor_eval_cli \
  --factor_id demo \
  --engine_mode parquet \
  --dataset_root backtest_data/parquet_v2 \
  --strategy_main_config_path configs/strategies/main_backtest_strategy.yaml
```

### 配置驱动一键回测

```bash
bash scripts/build/run_backtest_from_config.sh \
  --config configs/ops/backtest_run.yaml
```

### 配置覆盖校验

```bash
python3 scripts/build/verify_products_info_sync.py
python3 scripts/build/verify_config_docs_coverage.py
```
