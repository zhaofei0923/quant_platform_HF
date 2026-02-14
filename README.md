# quant_platform_HF

Quantitative trading platform bootstrap using C++ core execution and Python strategy orchestration.

## Quick start

```bash
./scripts/build/bootstrap.sh
```

在 Ubuntu 新机器上，脚本会自动安装缺失依赖并完成构建、测试、Python 环境准备。

如需禁止自动安装（仅在依赖已齐全时使用）：

```bash
./scripts/build/bootstrap.sh --skip-install-deps
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

## Redis Strategy Bridge (Strategy Closed Loop)

Protocol: `docs/STRATEGY_BRIDGE_REDIS_PROTOCOL.md`

This bridge connects:
- C++ `core_engine`: publishes `market:state7d:*`, consumes `strategy:intent:*`, writes `trade:order:*`
- Python strategy runner: consumes `market:state7d:*`, writes `strategy:intent:*`, consumes `trade:order:*`

Run with external Redis:

```bash
docker run --rm -p 6379:6379 redis:7-alpine

cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_REDIS_EXTERNAL=ON
cmake --build build -j

export CTP_SIM_PASSWORD='your_password'
export QUANT_HFT_REDIS_MODE=external
export QUANT_HFT_REDIS_HOST=127.0.0.1
export QUANT_HFT_REDIS_PORT=6379

./build/core_engine configs/sim/ctp.yaml --run-seconds 30
.venv/bin/python scripts/strategy/run_strategy.py --config configs/sim/ctp.yaml --strategy-id demo --run-seconds 30
```

### Bar 分发端到端自检

在发布前建议执行以下用例，验证 C++ 侧写入 `strategy:bar:*` 后，Python `strategy_runner` 能消费 bar 并触发 `on_bar` 产出 intent：

```bash
.venv/bin/python -m pytest python/tests/test_bar_dispatch_e2e.py python/tests/test_strategy_runner.py -q
```

通过标准：
- 输出包含 `5 passed`
- `test_bar_dispatch_e2e.py` 通过（键格式兼容）
- `test_strategy_runner.py` 通过（单策略与多策略同合约分发）

## Real CTP probe (optional)

```bash
cmake -S . -B build-real -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_CTP_REAL_API=ON
cmake --build build-real -j
export CTP_SIM_PASSWORD='your_password'

# preflight (config / password / CTP libs / TCP reachability)
.venv/bin/python scripts/ops/ctp_preflight_check.py \
  --config configs/sim/ctp_trading_hours.yaml

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
- Planner/executor CLI: `scripts/ops/ctp_fault_inject.py`
- SLO report generator: `scripts/ops/reconnect_slo_report.py`
- Result template: `docs/templates/RECONNECT_FAULT_INJECTION_RESULT.md`

Typical evidence flow:
```bash
# 1) run probe and record health timeline
LD_LIBRARY_PATH=$PWD/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64:$LD_LIBRARY_PATH \
  ./build-real/simnow_probe configs/sim/ctp.yaml --monitor-seconds 900 | tee runtime/reconnect_probe.log

# 2) run fault injections and record apply/clear events
sudo scripts/ops/ctp_fault_inject.py run --scenario disconnect --duration-sec 20 --execute \
  --event-log-file runtime/fault_events.jsonl

# 3) generate markdown evidence with p99 result
.venv/bin/python scripts/ops/reconnect_slo_report.py \
  --fault-events-file runtime/fault_events.jsonl \
  --probe-log-file runtime/reconnect_probe.log \
  --output-file docs/results/reconnect_fault_result.md \
  --config-profile configs/sim/ctp.yaml
```

One-shot orchestrator (probe + fault + report):
```bash
.venv/bin/python scripts/ops/run_reconnect_evidence.py \
  --config configs/sim/ctp_trading_hours.yaml \
  --execute-faults \
  --use-sudo \
  --build "build-real-$(date +%Y%m%d)"
```

When `ctp_trading_hours` group1 times out and preflight reports reachable alternate groups,
the runner auto-falls back to `ctp_trading_hours_group2.yaml` then `group3` by default.
Disable auto-fallback explicitly:
```bash
.venv/bin/python scripts/ops/run_reconnect_evidence.py \
  --config configs/sim/ctp_trading_hours.yaml \
  --no-auto-fallback-trading-groups
```

If host lacks `iptables`, run non-disconnect scenarios first:
```bash
.venv/bin/python scripts/ops/run_reconnect_evidence.py \
  --execute-faults \
  --use-sudo \
  --scenarios jitter,loss,combined
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
scripts/backtest/replay_csv.py --csv backtest_data/rb.csv --max-ticks 5000
```
- Python deterministic fill API:
  - `replay_csv_with_deterministic_fills(...)`
  - optional WAL JSONL output + PnL/position invariant report

## Architecture

- `core_engine` (C++): market data, risk, order, portfolio, regulatory sink, market state rule engine.
- `strategy_runtime` (Python): strategy APIs (`on_bar`, `on_state`, `on_order_event`) and orchestration.
- `proto/`: cross-process contracts with versioned Protobuf schema.

## Data Pipeline Adapters (Python)

- `quant_hft.data_pipeline.DuckDbAnalyticsStore`
  - append/query market/order records
  - prefers `duckdb` module, auto-falls back to sqlite in local/dev env
  - supports table export to CSV for offline research flow
- `quant_hft.data_pipeline.MinioArchiveStore`
  - object put/get/list
  - supports real MinIO SDK mode
  - supports local filesystem fallback mode (`bucket/object` path layout)

## Data Pipeline Process (Python)

- standalone process module:
  - `quant_hft.data_pipeline.DataPipelineProcess`
  - exports `market_snapshots`/`order_events` from analytics DB to per-run CSV bundle
  - writes `manifest.json` and archives bundle to MinIO (or local fallback)
  - emits in-memory observability records:
    - metrics (`runs_total`, `run_latency_ms`, table rows/latency)
    - traces (`data_pipeline.run_once`, `data_pipeline.export_table`)
    - alerts (`PIPELINE_EXPORT_EMPTY`, `PIPELINE_ARCHIVE_INCOMPLETE`, `PIPELINE_RUN_SLOW`, `PIPELINE_RUN_FAILED`)
- CLI:
```bash
.venv/bin/python scripts/data_pipeline/run_pipeline.py \
  --analytics-db runtime/analytics.duckdb \
  --export-dir runtime/exports \
  --archive-local-dir runtime/archive \
  --run-once
```

The CLI also supports archive env defaults:
- `QUANT_HFT_ARCHIVE_ENDPOINT`
- `QUANT_HFT_ARCHIVE_ACCESS_KEY`
- `QUANT_HFT_ARCHIVE_SECRET_KEY`
- `QUANT_HFT_ARCHIVE_BUCKET`
- `QUANT_HFT_ARCHIVE_LOCAL_DIR`
- `QUANT_HFT_ARCHIVE_PREFIX`

## Systemd deployment artifacts

- Render ready-to-install unit files and env templates:
```bash
.venv/bin/python scripts/ops/render_systemd_units.py \
  --repo-root . \
  --output-dir deploy/systemd \
  --service-user "$USER"
```
- Generated files:
  - `deploy/systemd/quant-hft-core-engine.service`
  - `deploy/systemd/quant-hft-data-pipeline.service`
  - `deploy/systemd/quant-hft-core-engine.env.example`
  - `deploy/systemd/quant-hft-data-pipeline.env.example`
- `quant-hft-core-engine.service` uses YAML config path and env-file injected credentials.
- `quant-hft-data-pipeline.service` runs loop mode (`--iterations 0`) with configurable interval.
- Detailed runbook: `docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md`

## Kubernetes deployment artifacts (non-hotpath)

- Render Kubernetes manifests for `data_pipeline`:
```bash
.venv/bin/python scripts/ops/render_k8s_manifests.py \
  --repo-root . \
  --output-dir deploy/k8s \
  --namespace quant-hft \
  --image-repository ghcr.io/<org>/quant-hft \
  --image-tag v0.1.0
```
- Generated files:
  - `deploy/k8s/namespace.yaml`
  - `deploy/k8s/configmap-data-pipeline.yaml`
  - `deploy/k8s/secret-archive.example.yaml`
  - `deploy/k8s/persistentvolumeclaim-runtime.yaml`
  - `deploy/k8s/deployment-data-pipeline.yaml`
  - `deploy/k8s/kustomization.yaml`
- Detailed runbook: `docs/K8S_DEPLOYMENT_RUNBOOK.md`

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
