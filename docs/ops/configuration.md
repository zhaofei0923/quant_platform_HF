# Quant HFT 配置与环境变量（Pure C++）

## 目标

- 所有敏感信息通过环境变量注入
- 配置文件支持 `${VAR_NAME}` 占位符
- 运行时入口统一为 C++ 可执行文件

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

- `ctp.strategy_factory`: 选择策略工厂。默认 `demo`。
- `ctp.strategy_composite_config`: 仅当 `strategy_factory: composite` 时必填。
- 路径解析规则：相对路径按 `ctp.yaml` 所在目录解析，启动时会转换为规范路径。

示例：

```yaml
ctp:
  strategy_factory: "composite"
  strategy_composite_config: "../strategies/composite_strategy.yaml"
```

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
```

## 安全建议

- 不在 YAML 中写明文密码
- 不将凭据提交到仓库
- 生产环境使用最小权限账号
