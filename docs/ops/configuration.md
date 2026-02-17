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
