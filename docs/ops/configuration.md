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
