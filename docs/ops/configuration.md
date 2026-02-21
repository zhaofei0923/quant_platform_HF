# Quant HFT 配置与环境变量（Pure C++）

## 目标

- 所有敏感信息通过环境变量注入
- 配置文件支持 `${VAR_NAME}` 占位符
- 运行时入口统一为 C++ 可执行文件

## 配置字典

- 全量字段解释与文件覆盖清单：`docs/ops/config_catalog.md`
- 配置文档覆盖校验：`python3 scripts/build/verify_config_docs_coverage.py`

## 配置优先级

1. 命令行参数（如 `--config`）
2. 环境变量（如 `CTP_CONFIG_PATH`、`QUANT_ROOT`）
3. YAML 中 `${VAR}` 占位符解析
4. 代码默认值（仅本地兜底）

## 市场状态检测器配置

- 配置块：`ctp.market_state_detector`
- 默认平缓阈值：`atr_flat_ratio: 0.001`（0.1%）
- 兼容策略：同时支持历史平铺键（如 `adx_period`），但若同时配置，嵌套键优先。

示例：

```yaml
ctp:
  market_state_detector:
    adx_period: 14
    adx_strong_threshold: 40.0
    adx_weak_lower: 25.0
    adx_weak_upper: 40.0
    kama_er_period: 10
    kama_fast_period: 2
    kama_slow_period: 30
    kama_er_strong: 0.6
    kama_er_weak_lower: 0.3
    atr_period: 14
    atr_flat_ratio: 0.001
    require_adx_for_trend: true
    use_kama_er: true
    min_bars_for_flat: 20
```

## Composite 策略插件配置

- `ctp.run_type`: `live|sim|backtest`。`core_engine` 仅允许 `live|sim`，`backtest` 仅用于回测链路。
- `ctp.strategy_factory`: 选择策略工厂。默认 `demo`。
- `ctp.strategy_composite_config`: 仅当 `strategy_factory: composite` 时必填。
- 路径解析规则：相对路径按 `ctp.yaml` 所在目录解析，启动时会转换为规范路径。

示例：

```yaml
ctp:
  run_type: "sim"
  strategy_factory: "composite"
  strategy_composite_config: "../strategies/composite_strategy.yaml"
```

### 策略状态持久化与指标采集

新增配置项（均为可选）：

- `ctp.strategy_state_persist_enabled`：是否开启策略状态快照（默认 `false`）
- `ctp.strategy_state_snapshot_interval_ms`：快照周期（默认 `60000`，`0` 表示关闭周期保存）
- `ctp.strategy_state_ttl_seconds`：状态 TTL（默认 `86400`，`0` 表示不设置 TTL）
- `ctp.strategy_state_key_prefix`：Redis key 前缀（默认 `strategy_state`）
- `ctp.strategy_metrics_emit_interval_ms`：主循环输出策略指标周期（默认 `1000`，`0` 表示关闭）

示例：

```yaml
ctp:
  strategy_state_persist_enabled: true
  strategy_state_snapshot_interval_ms: 5000
  strategy_state_ttl_seconds: 3600
  strategy_state_key_prefix: "hf_strategy_state"
  strategy_metrics_emit_interval_ms: 2000
```

## 回测主策略与资金配置

回测支持三层配置：

1. 主策略配置（`strategy_main_config_path`）
2. 子策略配置（各子策略 `config_path`）
3. 产品信息配置（`product_config_path`，示例文件 `instrument_info.json`）

推荐主策略配置字段：

- `run_type: backtest`（必需）
- `market_state_mode: true|false`
- `backtest.initial_equity`
- `backtest.symbols/start_date/end_date`
- `backtest.product_config_path`
- `composite.enable_non_backtest`（默认 `false`，仅在 `sim/live` 时置 `true`）
- `composite.sub_strategies[]`（完整子策略）
- `composite.sub_strategies[].overrides.{backtest|sim|live}.params`（按运行模式覆盖参数）

`backtest.symbols` 在 Parquet 回测支持两种输入：

- 品种（如 `c`）：按 `source=<品种>` 自动选择区间内相关合约分区
- 合约（如 `rb2405`）：按 `instrument_id` 精确选择，兼容历史配置

Parquet 回测链路现为纯 Parquet 读取，不再依赖 `*.parquet.ticks.csv` sidecar 文件。  
若二进制未启用 Arrow/Parquet（`QUANT_HFT_ENABLE_ARROW_PARQUET=OFF`），回测入口会直接报错并提示重编译。

CLI 优先级：`CLI 参数 > strategy_main_config > 默认值`。

当 `strategy_main_config_path` 提供且 `run_type != backtest` 时，回测入口会直接报错。

示例（开启 sim 并做参数覆盖）：

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
        sim:
          params:
            default_volume: 2
```

## 产品信息配置（YAML/JSON）

支持 `instrument_id` 精确匹配和 `symbol` 前缀回退匹配。配置同时包含手续费、每手乘数、保证金率。
手续费模式：

- `rate`: 按成交额比例
- `per_lot`: 按手固定

示例：

```yaml
products:
  rb2405:
    symbol: rb
    contract_multiplier: 10
    long_margin_ratio: 0.16
    short_margin_ratio: 0.16
    open_mode: rate
    open_value: 0.0001
    close_mode: per_lot
    close_value: 2
    close_today_mode: per_lot
    close_today_value: 3
```

默认推荐直接使用 `instrument_info.json`（`volume_multiple + commission.*_ratio_by_*`）自动映射。
JSON 形态支持两种写法：

- 包装写法：`{ "products": { ... } }`
- 原始写法：`{ "RB": {...}, "MA": {...} }`（与 `instrument_info.json` 根结构一致）

仍兼容 `products_info.yaml`（内容与 `instrument_info.json` 对齐）。

回测中若配置了 `product_config_path` 但找不到当前 `instrument_id` 的产品项，会 fail-fast 退出。

## 回测资金与结果口径

- 开仓风险资金：由各子策略 `risk_per_trade_pct` 自行计算
- 保证金约束：`available_margin = max(0, account_equity - used_margin_total)`
- 开仓自动缩量：`max_openable = floor(available_margin / per_lot_margin)`，`volume=min(requested,max_openable)`
- 手续费口径：按产品费率配置计入 `total_commission`
- 权益口径：`final_equity = initial_equity + total_pnl_after_cost`
- `total_pnl_after_cost = total_pnl - total_commission`

结果 JSON 的 `deterministic.performance` 中包含：

- `initial_equity`
- `final_equity`
- `total_commission`
- `total_pnl_after_cost`
- `max_margin_used`
- `final_margin_used`
- `margin_clipped_orders`
- `margin_rejected_orders`

## 配置驱动一键编译+回测脚本

新增脚本：`scripts/build/run_backtest_from_config.sh`  
新增配置：`configs/ops/backtest_run.yaml`

脚本能力：

- 读取单一 YAML 运行配置（顶层 `key: value`）。
- 自动执行增量构建并仅编译 `backtest_cli`（可 `--skip-build`）。
- 固定 parquet-only 回测，自动传入 `strategy_main_config_path`。
- 构建失败时可自动执行 `scripts/build/install_arrow_parquet_deps.sh` 后重试一次。

示例：

```bash
bash scripts/build/run_backtest_from_config.sh \
  --config configs/ops/backtest_run.yaml
```

调试模式：

```bash
bash scripts/build/run_backtest_from_config.sh \
  --config configs/ops/backtest_run.yaml \
  --dry-run
```

常用开关：

- `--config <path>`：指定运行配置文件（默认 `configs/ops/backtest_run.yaml`）。
- `--dry-run`：仅打印将执行的命令。
- `--skip-build`：跳过 cmake 构建，直接运行已有 `backtest_cli`。

关键配置字段：

- 构建字段：`build_dir`、`cmake_build_type`、`build_tests`、`enable_arrow_parquet`、`auto_install_arrow_parquet_deps`
- 回测必填：`engine_mode=parquet`、`dataset_root`、`strategy_main_config_path`
- 输出必填：`output_json`、`output_md`
- 可选透传：`max_ticks`、`start_date`、`end_date`、`run_id`、`export_csv_dir`、`emit_*`

## 滚动回测 CLI（库内执行）

入口：

```bash
./build-gcc/rolling_backtest_cli --config configs/ops/rolling_backtest.yaml
```

关键行为：

- 仅支持 `parquet`，且要求 manifest 存在。
- 窗口按 `trading_day` 生成。
- 尾部不完整窗口自动丢弃。
- `mode=rolling_optimize` 时窗口串行执行，窗口内 trial 并行由 `optimization.parallel` 控制。
- 指标默认 `hf_standard.profit_factor`（映射到 `hf_standard.advanced_summary.profit_factor`）。

## 研究回放指标轨迹落盘

- 作用范围：仅 `backtest/factor_eval` 研究回放链路。
- 输出格式：Parquet（单 run 单文件）。
- 默认关闭：需显式开启对应开关。
- 市场状态指标默认输出路径：`runtime/research/indicator_trace/<run_id>.parquet`。
- 子策略指标默认输出路径：`runtime/research/sub_strategy_indicator_trace/<run_id>.parquet`。
- 覆盖策略：目标文件已存在时直接报错退出（fail-fast）。

支持参数：

- `emit_indicator_trace` / `emit-indicator-trace`
- `indicator_trace_path` / `indicator-trace-path`
- `strategy_factory` / `strategy-factory`（默认 `demo`，子策略 trace 需 `composite`）
- `strategy_composite_config` / `strategy-composite-config`（`strategy_factory=composite` 时必填）
- `emit_sub_strategy_indicator_trace` / `emit-sub-strategy-indicator-trace`
- `sub_strategy_indicator_trace_path` / `sub-strategy-indicator-trace-path`

落盘字段（每根 bar 一行）：

- `instrument_id`, `ts_ns`
- `bar_open`, `bar_high`, `bar_low`, `bar_close`, `bar_volume`
- `kama`, `atr`, `adx`, `er`
- `market_regime`

子策略 trace 额外字段（每根 bar × 每个子策略一行）：

- `strategy_id`, `strategy_type`
- `kama`, `atr`, `adx`, `er`（不可用字段为 `NULL`）
- `stop_loss_price`, `take_profit_price`（nullable）

示例：

```bash
./build/factor_eval_cli \
  --factor_id trend_kama \
  --csv_path runtime/benchmarks/backtest/rb_ci_sample.csv \
  --run_id exp-kama-atr-adx \
  --strategy_factory composite \
  --strategy_composite_config configs/strategies/composite_strategy.yaml \
  --emit_indicator_trace true \
  --indicator_trace_path runtime/research/indicator_trace/exp-kama-atr-adx.parquet \
  --emit_sub_strategy_indicator_trace true \
  --sub_strategy_indicator_trace_path runtime/research/sub_strategy_indicator_trace/exp-kama-atr-adx.parquet
```

## 核心环境变量

### 路径与运行

- `QUANT_ROOT`
- `CTP_CONFIG_PATH`
- `RISK_RULE_FILE_PATH`
- `SETTLEMENT_PRICE_CACHE_DB`

### CTP

- `CTP_SIM_PASSWORD`
- `CTP_SIM_BROKER_ID`
- `CTP_SIM_USER_ID`
- `CTP_SIM_INVESTOR_ID`
- `CTP_SIM_MARKET_FRONT`
- `CTP_SIM_TRADER_FRONT`

### 存储外部模式（可选）

- `QUANT_HFT_REDIS_MODE` (`in_memory|external`)
- `QUANT_HFT_REDIS_HOST` / `QUANT_HFT_REDIS_PORT`
- `QUANT_HFT_TIMESCALE_MODE` (`in_memory|external`)
- `QUANT_HFT_TIMESCALE_DSN`

## 推荐检查

```bash
bash scripts/build/dependency_audit.sh --build-dir build
bash scripts/build/repo_purity_check.sh --repo-root .
python3 scripts/build/verify_products_info_sync.py
python3 scripts/build/verify_config_docs_coverage.py
```

## 安全建议

- 不在 YAML 中写明文密码
- 不将凭据提交到仓库
- 生产环境使用最小权限账号
