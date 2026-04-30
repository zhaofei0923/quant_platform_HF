# SimNow 无人值守仿真交易启动与监控

本文档说明如何启动 SimNow 仿真交易、如何启用无人值守 supervisor、如何配置系统重启自动恢复，以及自动执行期间如何监控运行状态。

## 前置条件

确认本地 `.env` 已配置真实 SimNow 交易时段前置和账号信息：

```bash
CTP_SIM_IS_PRODUCTION_MODE=true
CTP_SIM_ENABLE_REAL_API=true
CTP_SIM_MARKET_FRONT=tcp://182.254.243.31:30011
CTP_SIM_TRADER_FRONT=tcp://182.254.243.31:30001
```

默认 SimNow 配置使用 `${CTP_SIM_INVESTOR_ID}` 作为策略账户上下文，并关闭 20 万逻辑子账户资金池风控：

```yaml
account_id: "${CTP_SIM_INVESTOR_ID}"
risk_sim_subaccount_enabled: false
```

因此交易资金约束来自 CTP 返回的真实 SimNow 账户资金、持仓、保证金和本地 CTP 账本，而不是固定 20 万子账户。

`core_engine` 和 `simnow_probe` 需要已经构建完成：

```bash
test -x build/core_engine
test -x build/simnow_probe
```

默认配置文件为：

```bash
configs/sim/ctp.yaml
```

## 启动方式选择

有两种推荐启动方式：

- 单次启动：使用 `scripts/ops/start_simnow_trading.sh`，适合手动开始一次仿真交易。
- 无人值守：使用 `scripts/ops/supervise_simnow_trading.sh`，适合按交易时段自动启停、崩溃重启、日终结算和持续监控。

生产化无人值守场景推荐使用 supervisor，并通过 systemd 管理。

## 单次启动脚本

先 dry-run，确认配置和命令不会误启动：

```bash
scripts/ops/start_simnow_trading.sh --dry-run --run-id simnow-dry-run --run-seconds 60
```

只运行安全探针，不启动交易主进程：

```bash
scripts/ops/start_simnow_trading.sh --probe-only --run-id simnow-probe-check
```

后台启动一次仿真交易：

```bash
scripts/ops/start_simnow_trading.sh --run-id "simnow-$(date +%Y%m%d-%H%M%S)"
```

短时仿真，例如运行 30 分钟后自动退出：

```bash
scripts/ops/start_simnow_trading.sh --run-id simnow-smoke --run-seconds 1800
```

前台运行并同时写日志：

```bash
scripts/ops/start_simnow_trading.sh --foreground --run-id simnow-foreground
```

如果已有 `core_engine` 正在运行，脚本默认会拒绝重复启动。可选择：

```bash
# 已有进程存活时视为成功退出
scripts/ops/start_simnow_trading.sh --allow-existing

# 启动前停止已有进程
scripts/ops/start_simnow_trading.sh --stop-existing
```

单次启动会写入：

```text
runtime/simnow_trading/<run_id>/simnow_probe.log
runtime/simnow_trading/<run_id>/core_engine.log
runtime/simnow_trading/<run_id>/core_engine.pid
runtime/simnow_trading/<run_id>/run_summary.env
runtime/simnow_trading/current_core_engine.pid
runtime/simnow_trading/current_run_dir
runtime/simnow_trading/current_core_engine_log
```

## 合约元数据查询

主力合约模式下，启动时会优先读取产品级合约缓存：

```text
runtime/ctp_instruments/<product>_contracts.json
runtime/ctp_instruments/<product>_dominant_contract.json
```

如果缓存存在，`simnow_probe` 和 `core_engine` 不再每次启动全量查询所有 CTP 合约；它们只用缓存中的候选合约订阅行情并选择主力。选出交易合约后，仅对当前交易合约执行一次 `ReqQryInstrument`，并继续查询该合约的保证金率、手续费率和报单手续费率。运行中发生主力切换时，也只对新主力合约补查一次合约元数据。

如果产品缓存不存在或为空，会做一次全量合约查询作为兜底，并重新写入 `<product>_contracts.json`。首次接入新品种或交易所合约规则变更后，可先运行一次：

```bash
scripts/ops/start_simnow_trading.sh --probe-only --run-id simnow-refresh-contracts
```

## 无人值守 Supervisor

先 dry-run 查看当前调度决策：

```bash
scripts/ops/supervise_simnow_trading.sh --dry-run
```

验证启动分支，不真正启动：

```bash
scripts/ops/supervise_simnow_trading.sh --dry-run --windows test=00:00-23:59
```

直接在当前终端运行 supervisor：

```bash
scripts/ops/supervise_simnow_trading.sh
```

默认交易窗口来自 `.env`：

```bash
SIMNOW_TRADING_WINDOWS=night=20:55-02:35,day_am=08:55-11:35,day_pm=13:25-15:20
```

默认行为：

- 在交易窗口内自动启动 `core_engine`。
- 不在交易窗口内自动停止 `core_engine`。
- 每 `SIMNOW_CHECK_INTERVAL_SECONDS` 秒检查一次进程和健康状态。
- 进程崩溃后自动重启。
- 每个交易窗口最多重启 `SIMNOW_MAX_RESTARTS_PER_WINDOW` 次。
- 收盘后按 `SIMNOW_EOD_TIME` 执行日终流程。

常用 supervisor 参数：

```bash
# 自定义交易窗口
scripts/ops/supervise_simnow_trading.sh --windows '09:00-11:30,13:30-15:00'

# 指定交易日历文件，每行一个 YYYYMMDD 或 YYYY-MM-DD
scripts/ops/supervise_simnow_trading.sh --trading-days-file configs/sim/trading_days.txt

# 不执行日终流程
scripts/ops/supervise_simnow_trading.sh --no-eod

# 只检查一次后退出
scripts/ops/supervise_simnow_trading.sh --once
```

## Systemd 开机恢复

安装用户级 systemd unit，但不启用：

```bash
scripts/ops/install_simnow_systemd_user.sh
```

安装并设置为用户会话启动时自动恢复：

```bash
scripts/ops/install_simnow_systemd_user.sh --enable
```

安装、启用并立即启动：

```bash
scripts/ops/install_simnow_systemd_user.sh --enable-now
```

如果希望机器重启后即使用户没有登录也自动恢复，需要启用 linger：

```bash
scripts/ops/install_simnow_systemd_user.sh --enable-linger --enable
```

停止并禁用服务：

```bash
scripts/ops/install_simnow_systemd_user.sh --disable
```

服务文件为：

```text
infra/systemd/quant-hft-simnow-trading.service
```

## 监控总览

自动执行时有三层监控：

1. systemd 监控 supervisor 是否存活。
2. supervisor 监控 `core_engine` 是否存活并负责崩溃重启。
3. supervisor 检查 tick、bar、成交回报、磁盘、日志、日终流程状态。

常用监控命令：

```bash
systemctl --user status quant-hft-simnow-trading.service
journalctl --user -u quant-hft-simnow-trading.service -f
tail -f runtime/simnow_trading/supervisor.log
tail -f "$(cat runtime/simnow_trading/current_core_engine_log)"
```

确认 `core_engine` 是否存活：

```bash
pid=$(cat runtime/simnow_trading/current_core_engine.pid)
kill -0 "${pid}" && echo alive || echo dead
```

查看当前运行目录：

```bash
cat runtime/simnow_trading/current_run_dir
```

查看当前运行摘要：

```bash
cat "$(cat runtime/simnow_trading/current_run_dir)/run_summary.env"
```

## 交易健康检查

### Tick 和 Bar

行情 CSV 默认目录：

```text
runtime/market_data/simnow
```

supervisor 会检查最新文件更新时间：

```text
runtime/market_data/simnow/**/ticks.csv
runtime/market_data/simnow/**/bars_1m.csv
```

默认阈值：

```bash
SIMNOW_TICK_STALE_SECONDS=180
SIMNOW_BAR_STALE_SECONDS=240
```

手动查看最新 tick 和 bar 文件：

```bash
find runtime/market_data/simnow -name 'ticks.csv' -printf '%T@ %p\n' | sort -nr | head -1
find runtime/market_data/simnow -name 'bars_1m.csv' -printf '%T@ %p\n' | sort -nr | head -1
```

### 成交回报

supervisor 会扫描当前 `core_engine.log`，检查下单活动之后是否长期没有新的成交回报。

默认阈值：

```bash
SIMNOW_FILL_STALE_SECONDS=900
```

如果希望即使没有下单也要求成交回报心跳，可开启：

```bash
SIMNOW_REQUIRE_FILL_HEARTBEAT=1
```

默认不启用成交心跳要求，因为没有成交本身可能是正常交易状态。

## 日终流程与日报

日终触发时间：

```bash
SIMNOW_EOD_TIME=15:25
```

日终流程默认会：

1. 停止 `core_engine`。
2. 执行 `scripts/ops/run_daily_settlement.sh`。
3. 生成 `ops_health_report`。
4. 生成 `ops_alert_report`。
5. 生成 `simnow_daily_report`。
6. 如果设置了 `SIMNOW_ANALYSIS_COMMAND`，执行自定义分析命令。

日终产物目录：

```text
runtime/simnow_trading/eod/<YYYYMMDD>/
```

主要文件：

```text
daily_settlement.log
daily_settlement_evidence.json
settlement_diff.json
ops_health_report.json
ops_health_report.md
ops_alert_report.json
ops_alert_report.md
simnow_daily_report.json
simnow_daily_report.md
```

查看当天日报：

```bash
cat runtime/simnow_trading/eod/$(date +%Y%m%d)/simnow_daily_report.md
```

## 告警配置

支持三种告警钩子：

```bash
SIMNOW_ALERT_WEBHOOK_URL=
SIMNOW_ALERT_EMAIL_TO=
SIMNOW_ALERT_COMMAND=
```

典型触发条件：

- `simnow_probe` 启动前检查失败。
- `core_engine` 启动失败或启动后秒退。
- `core_engine` 运行中崩溃。
- 重启次数超过限制。
- tick 长时间不更新。
- bar 长时间不更新。
- 下单后长时间没有成交回报。
- 磁盘空间低于阈值。
- 日终结算失败。
- 自定义日终分析命令失败。

自定义命令告警示例：

```bash
SIMNOW_ALERT_COMMAND='printf "%s %s\n" "$ALERT_SEVERITY" "$ALERT_MESSAGE" >> runtime/simnow_trading/alerts.log'
```

## 日常排查命令

查看服务状态：

```bash
systemctl --user status quant-hft-simnow-trading.service
```

查看 systemd 日志：

```bash
journalctl --user -u quant-hft-simnow-trading.service -n 200 --no-pager
```

查看 supervisor 日志：

```bash
tail -n 200 runtime/simnow_trading/supervisor.log
```

查看当前 core 日志：

```bash
tail -n 200 "$(cat runtime/simnow_trading/current_core_engine_log)"
```

查看安全探针日志：

```bash
tail -n 200 "$(cat runtime/simnow_trading/current_run_dir)/simnow_probe.log"
```

查看是否有重复 `core_engine`：

```bash
ps -eo pid=,args= | grep -E '(^|/)core_engine( |$)' | grep -v grep || true
```

查看磁盘空间：

```bash
df -h runtime/simnow_trading runtime/market_data/simnow
```

查看最新行情文件：

```bash
find runtime/market_data/simnow -type f \( -name 'ticks.csv' -o -name 'bars_1m.csv' \) -printf '%TY-%Tm-%Td %TH:%TM:%TS %p\n' | sort | tail -20
```

## 停止与重启

停止 systemd 管理的 supervisor：

```bash
systemctl --user stop quant-hft-simnow-trading.service
```

重启 supervisor：

```bash
systemctl --user restart quant-hft-simnow-trading.service
```

手动停止当前 `core_engine`：

```bash
pid=$(cat runtime/simnow_trading/current_core_engine.pid)
kill -TERM "${pid}"
```

如果 supervisor 仍在运行且当前处于交易窗口内，它会按策略自动重启 `core_engine`。需要完全停止自动恢复时，应先停止 supervisor。

## 安全注意事项

- 不要把真实账号密码写入文档或 YAML，统一放在本地 `.env`。
- 不要在 7x24 `400xx` 前置上运行无人值守交易脚本。
- 正式无人值守前先执行 `--dry-run` 和 `--probe-only`。
- 修改交易窗口、日终时间、告警阈值后，先用 `scripts/ops/supervise_simnow_trading.sh --dry-run` 验证调度决策。
- 若没有配置真实交易日历，supervisor 默认按工作日近似判断，节假日需要用 `SIMNOW_TRADING_DAYS_FILE` 覆盖。