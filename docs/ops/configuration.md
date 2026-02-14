# Quant HFT 配置与环境变量

## 目标
- 所有敏感信息通过环境变量注入
- 配置文件支持 `${VAR_NAME}` 占位符
- 避免机器绑定路径（如用户家目录）

## 配置优先级
1. 命令行参数（如 `--config`）
2. 环境变量（如 `CTP_CONFIG_PATH`、`QUANT_ROOT`）
3. YAML 配置中的 `${VAR}` 占位符解析
4. 代码默认值（仅用于本地开发兜底）

## 核心环境变量

### 路径与运行
- `QUANT_ROOT`：项目根目录
- `CTP_CONFIG_PATH`：CTP 配置文件路径
- `RISK_RULE_FILE_PATH`：风控规则文件路径
- `SETTLEMENT_PRICE_CACHE_DB`：结算价缓存库路径

### CTP（生产）
- `CTP_PROD_BROKER_ID`
- `CTP_PROD_USER_ID`
- `CTP_PROD_INVESTOR_ID`
- `CTP_PROD_MARKET_FRONT`
- `CTP_PROD_TRADER_FRONT`
- `CTP_PROD_PASSWORD`
- `CTP_PROD_AUTH_CODE`
- `CTP_PROD_APP_ID`

### CTP（仿真/开发）
- `CTP_SIM_BROKER_ID` / `CTP_DEV_BROKER_ID`
- `CTP_SIM_USER_ID` / `CTP_DEV_USER_ID`
- `CTP_SIM_INVESTOR_ID` / `CTP_DEV_INVESTOR_ID`
- `CTP_SIM_MARKET_FRONT` / `CTP_DEV_MARKET_FRONT`
- `CTP_SIM_TRADER_FRONT` / `CTP_DEV_TRADER_FRONT`
- `CTP_SIM_PASSWORD` / `CTP_DEV_PASSWORD`
- `CTP_SIM_AUTH_CODE` / `CTP_DEV_AUTH_CODE`
- `CTP_SIM_APP_ID` / `CTP_DEV_APP_ID`

### SimNow 自动选组（30001/30011, 30002/30012, 30003/30013）
- 使用 `scripts/ops/select_ctp_front_group.py` 自动探测三组前置可达性，并生成可 `source` 的环境文件。
- 示例：
	- `python3 scripts/ops/select_ctp_front_group.py --user-id 191202 --password '<password>' --broker-id 9999 --app-id simnow_client_test --auth-code 0000000000000000 --json`
	- 默认输出环境文件：`/tmp/ctp_sim_selected.env`
	- 生效方式：`set -a && source /tmp/ctp_sim_selected.env && set +a`
- 选组策略：按组号顺序选择首个“交易前置 + 行情前置”均可连通的组，并导出 `CTP_SIM_SELECTED_GROUP`。

### 外部依赖
- `KAFKA_BOOTSTRAP_SERVERS`
- `CLICKHOUSE_DSN`

## systemd 建议
在 `/etc/quant/platform.conf` 管理环境变量，service 使用：
- `EnvironmentFile=-/etc/quant/platform.conf`
- `ExecStart` 通过 `${QUANT_ROOT}` 组装可执行路径

## 安全建议
- 不在 YAML 中写明文密码
- 不将凭据提交到仓库
- 生产环境使用最小权限账号与只读配置文件权限
