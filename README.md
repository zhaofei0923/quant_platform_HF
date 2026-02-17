# quant_platform_HF

Quantitative trading platform bootstrap using a pure C++ execution and strategy stack.

## Quick start

```bash
./scripts/build/bootstrap.sh
```

在 Ubuntu 新机器上，脚本会自动安装缺失依赖并完成构建与测试。

如需禁止自动安装（仅在依赖已齐全时使用）：

```bash
./scripts/build/bootstrap.sh --skip-install-deps
```

## Migration Note

- 仓库已切换为纯 C++：`python/`、`.py` 脚本与 pybind 绑定已移除。
- 运行入口统一为 `build/` 下的 C++ CLI（如 `backtest_cli`、`simnow_compare_cli`、`reconnect_evidence_cli`）。
- CI 与本地统一使用 `scripts/build/dependency_audit.sh` + `scripts/build/repo_purity_check.sh` 作为硬门禁。

## Quality gates

```bash
bash scripts/build/dependency_audit.sh --build-dir build
bash scripts/build/repo_purity_check.sh --repo-root .
bash scripts/build/run_consistency_gates.sh --build-dir build --results-dir docs/results
bash scripts/build/run_preprod_rehearsal_gate.sh --build-dir build --results-dir docs/results
```

## SimNow profiles

- `configs/sim/ctp.yaml`: 7x24 SimNow (`182.254.243.31:40011/40001`)
- `configs/sim/ctp_trading_hours.yaml`: trading-hours-aligned SimNow (`182.254.243.31:30011/30001`)
- `configs/sim/ctp_trading_hours_group2.yaml`: trading-hours group2 (`182.254.243.31:30012/30002`)
- `configs/sim/ctp_trading_hours_group3.yaml`: trading-hours group3 (`182.254.243.31:30013/30003`)
- `ctp_trading_hours` timeout in preflight may indicate out-of-session hours (service window follows production)
- reconnect knobs are configurable in YAML:
  - `connect_timeout_ms`
  - `reconnect_max_attempts`
  - `reconnect_initial_backoff_ms`
  - `reconnect_max_backoff_ms`

## 使用 .env 注入 CTP_SIM_*（推荐）

1) 复制模板并填写账号：
```bash
cp .env.example .env
```
2) 加载到当前 shell：
```bash
set -a && source .env && set +a
```
3) 启动前快速检查：
```bash
env | grep '^CTP_SIM_'
```

也可直接通过系统环境变量注入（CI/systemd/k8s），YAML 中 `${CTP_SIM_*}` 会在加载时自动替换。

## Core Engine config loading

- `core_engine` now loads CTP runtime config from YAML at startup:
```bash
export CTP_SIM_PASSWORD='your_password'
./build/core_engine configs/sim/ctp.yaml
```
- Required config items are validated by `CtpConfigLoader + CtpConfigValidator`.
- `is_production_mode` must be explicit in YAML.
- `CtpGatewayAdapter` automatically retries known SimNow trading-hours front groups
  (`30001/11 -> 30002/12 -> 30003/13`) on the same host when real-api connect fails.
- terminal auth is configurable via `enable_terminal_auth` (default `true`).
  When look-through front rejects handshake, try `enable_terminal_auth: false`.
- Password resolution order:
  1. `password` in YAML
  2. `password_env` in YAML (fallback default `CTP_SIM_PASSWORD`)

## Strategy Engine Closed Loop

纯 C++ 主链路中，策略闭环已改为进程内执行：
- `core_engine` 生成并分发 `StateSnapshot7D`
- `StrategyEngine` 将状态与订单事件派发到 `ILiveStrategy` 实例
- `SignalIntent` 进入既有 `ExecutionPlanner + Risk + ExecutionEngine`

需要外部 Redis 存储时可开启 external 模式：

```bash
docker run --rm -p 6379:6379 redis:7-alpine

cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_REDIS_EXTERNAL=ON
cmake --build build -j

export CTP_SIM_PASSWORD='your_password'
export QUANT_HFT_REDIS_MODE=external
export QUANT_HFT_REDIS_HOST=127.0.0.1
export QUANT_HFT_REDIS_PORT=6379

./build/core_engine configs/sim/ctp.yaml --run-seconds 30
```

### Strategy path smoke

```bash
ctest --test-dir build -R "(StrategyRegistryTest|StrategyEngineTest|DemoLiveStrategyTest|CallbackDispatcherTest)" --output-on-failure
```

## Real CTP probe (optional)

```bash
cmake -S . -B build-real -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_CTP_REAL_API=ON
cmake --build build-real -j
export CTP_SIM_PASSWORD='your_password'

LD_LIBRARY_PATH=$PWD/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64:$LD_LIBRARY_PATH \
  ./build-real/simnow_probe configs/sim/ctp_trading_hours.yaml
```

Optional probe runtime flags:
- `--monitor-seconds` (default `300`, use `<0` for no time limit)
- `--health-interval-ms` (default `1000`)
- Probe emits `[health] ts_ns=... state=healthy|unhealthy` lines for SLO analysis.
- On connect failure, probe prints `Connect diagnostic:` with per-front attempt detail
  and CTP `ErrorID/ErrorMsg` (when available).

## Reconnect Fault Injection (SimNow)

- Runbook: `docs/CTP_SIMNOW_RECONNECT_FAULT_INJECTION_RUNBOOK.md`
- Result template: `docs/templates/RECONNECT_FAULT_INJECTION_RESULT.md`

One-shot evidence generation:
```bash
mkdir -p docs/results
./build/reconnect_evidence_cli \
  --config-profile configs/sim/ctp.yaml \
  --report_file docs/results/reconnect_fault_result.md \
  --health_json_file docs/results/ops_health_report.json \
  --health_markdown_file docs/results/ops_health_report.md \
  --alert_json_file docs/results/ops_alert_report.json \
  --alert_markdown_file docs/results/ops_alert_report.md
```

Independent health/alert reports:
```bash
./build/ops_health_report_cli \
  --strategy-engine-chain-status complete \
  --output_json docs/results/ops_health_report.json \
  --output_md docs/results/ops_health_report.md

./build/ops_alert_report_cli \
  --health-json-file docs/results/ops_health_report.json \
  --output_json docs/results/ops_alert_report.json \
  --output_md docs/results/ops_alert_report.md
```

## WAL Replay Recovery

- Loader: `quant_hft::WalReplayLoader`
- CLI verification tool:
```bash
./build/wal_replay_tool runtime_events.wal
```
- `core_engine` startup automatically attempts WAL replay before live processing.

## Backtest Replay Harness

- Runbook: `docs/BACKTEST_REPLAY_HARNESS.md`
- CLI:
```bash
mkdir -p docs/results
./build/backtest_cli \
  --engine_mode csv \
  --csv_path backtest_data/rb.csv \
  --max_ticks 5000 \
  --output_json docs/results/backtest_cli_smoke.json \
  --output_md docs/results/backtest_cli_smoke.md
```

## Architecture

- `core_engine` (C++): market data, risk, order, portfolio, regulatory sink, market state rule engine.
- `strategy_engine` (C++): strategy APIs (`Initialize`, `OnState`, `OnOrderEvent`, `OnTimer`) and in-process orchestration.
- `proto/`: cross-process contracts with versioned Protobuf schema.

## Release packaging (non-hotpath)

- Build portable release bundle (tar + sha256):
```bash
bash scripts/build/package_nonhotpath_release.sh v0.1.0
```
- Output:
  - `dist/quant-hft-nonhotpath-v0.1.0.tar.gz`
  - `dist/quant-hft-nonhotpath-v0.1.0.tar.gz.sha256`
- GitHub Actions workflow:
  - `/.github/workflows/release-package.yml`
  - triggers on `v*` tag push or manual `workflow_dispatch`

## Data Adapters (Baseline)

- Realtime cache adapter:
  - `IRealtimeCache` + `RedisRealtimeStore` (`trade:order:*`, `market:tick:*`, `trade:position:*` key schema)
- Timeseries adapter:
  - `ITimeseriesStore` + `TimescaleEventStore` (market/order/risk decision append + query)
- Client-backed path:
  - `RedisRealtimeStoreClientAdapter` + `IRedisHashClient`
  - `TimescaleEventStoreClientAdapter` + `ITimescaleSqlClient`
  - shared `StorageRetryPolicy` for retry/backoff.
- Pooling + async write path:
  - `PooledRedisHashClient` / `RedisHashClientPool`
  - `PooledTimescaleSqlClient` / `TimescaleSqlClientPool`
  - `TimescaleBufferedEventStore` for batch flush in background thread.
- Factory + runtime config:
  - `StorageConnectionConfig::FromEnvironment()`
  - `StorageClientFactory` with `in-memory`/`external` mode selection and optional fallback.
- Build flags:
  - `-DQUANT_HFT_ENABLE_REDIS_EXTERNAL=ON`
  - `-DQUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=ON`
- Current stage defaults to in-memory clients for deterministic tests.
- Redis external mode now uses `TcpRedisHashClient` (`PING/HSET/HGETALL` + optional `AUTH`).
- Timescale external mode now uses `LibpqTimescaleSqlClient` (runtime `libpq` dynamic load + `SELECT 1` health check).
- If external drivers are unavailable/unhealthy, factory fallback behavior is controlled by `QUANT_HFT_STORAGE_ALLOW_FALLBACK`.

## Storage Env Vars

- `QUANT_HFT_REDIS_MODE` = `in_memory|external`
- `QUANT_HFT_REDIS_HOST`, `QUANT_HFT_REDIS_PORT`, `QUANT_HFT_REDIS_USER`, `QUANT_HFT_REDIS_PASSWORD`
- `QUANT_HFT_REDIS_TLS` = `true|false`
- `QUANT_HFT_REDIS_CONNECT_TIMEOUT_MS`, `QUANT_HFT_REDIS_READ_TIMEOUT_MS`
- `QUANT_HFT_TIMESCALE_MODE` = `in_memory|external`
- `QUANT_HFT_TIMESCALE_DSN` (or host/port/db/user/password fields)
- `QUANT_HFT_TIMESCALE_HOST`, `QUANT_HFT_TIMESCALE_PORT`, `QUANT_HFT_TIMESCALE_DB`
- `QUANT_HFT_TIMESCALE_USER`, `QUANT_HFT_TIMESCALE_PASSWORD`
- `QUANT_HFT_TIMESCALE_SSLMODE`
- `QUANT_HFT_TIMESCALE_CONNECT_TIMEOUT_MS`
- `QUANT_HFT_STORAGE_ALLOW_FALLBACK` = `true|false`
