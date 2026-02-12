# CTP One-Shot Cutover Checklist

Generated at: 2026-02-12T06:30:25.676550+00:00
Environment: single-host-ubuntu
Cutover window(local): 2026-02-13T09:00:00+08:00
CTP config: `configs/prod/ctp.yaml`

## 1) Pre-Cutover (T-30min)

- [ ] Stop old core engine

```bash
systemctl stop quant-hft-core-engine.service
```

- [ ] Stop old strategy runner

```bash
systemctl stop quant-hft-strategy-runner.service
```

- [ ] Precheck readiness evidence

```bash
python3 scripts/ops/run_ctp_readiness_evidence.py --output-json docs/results/ctp_readiness_precheck.json --environment single-host-ubuntu --query-latency-ms 1200 --flow-control-hits 1 --disconnect-recovery-success-rate 1.0 --reject-classified-ratio 1.0 --core-process-alive true --redis-health healthy --timescale-health healthy --strategy-bridge-chain-status complete
```

- [ ] Bootstrap infra and initialize schemas/topics

```bash
bash scripts/infra/bootstrap_prodlike.sh --profile single-host --compose-file infra/docker-compose.single-host.yaml --project-name quant-hft-single-host --env-file infra/env/prodlike.env --execute --output-file docs/results/single_host_bootstrap_result.env
bash scripts/infra/init_kafka_topics.sh --compose-file infra/docker-compose.single-host.yaml --project-name quant-hft-single-host --env-file infra/env/prodlike.env --execute --output-file docs/results/kafka_topic_init_result.env
bash scripts/infra/init_clickhouse_schema.sh --compose-file infra/docker-compose.single-host.yaml --project-name quant-hft-single-host --env-file infra/env/prodlike.env --execute --output-file docs/results/clickhouse_schema_init_result.env
bash scripts/infra/init_debezium_connectors.sh --connect-url http://kafka-connect:8083 --execute --output-file docs/results/debezium_connector_init_result.env
```

## 2) Cutover Window

- [ ] Start new core engine

```bash
./build/core_engine configs/prod/ctp.yaml
```

- [ ] Start new strategy runner

```bash
python3 scripts/strategy/run_ctp_strategy.py --config configs/prod/ctp.yaml
```

- [ ] Warmup query and settlement verification

```bash
./build/simnow_probe configs/prod/ctp.yaml --monitor-seconds 30
```

## 3) Post-Cutover 30 Minutes Watch

- [ ] order_latency_p99_ms
- [ ] breaker_state
- [ ] query_rate_violation
- [ ] reconnect_p99_s
- [ ] kafka_lag_ms
- [ ] clickhouse_ingest_delay_ms

- [ ] Persist cutover evidence to:

`docs/results/ctp_cutover_result.env`

## 4) Rollback Drill Template

Trigger condition:
- `order_latency_p99_ms_gt_5ms_or_reconnect_p99_s_gt_10s`

- [ ] Stop new stack

```bash
systemctl stop quant-hft-core-engine.service
systemctl stop quant-hft-strategy-runner.service
```

- [ ] Restore previous binaries and compat bridge

```bash
rsync -av /opt/quant_hft/releases/previous/ /opt/quant_hft/current/
python3 scripts/ops/fake_redis_bridge_server.py --host 127.0.0.1 --port 6379
```

- [ ] Start previous stack

```bash
./build/core_engine configs/prod/ctp.yaml
python3 scripts/strategy/run_strategy.py --config configs/prod/ctp.yaml
```

- [ ] Validate rollback health

```bash
python3 scripts/ops/run_ctp_readiness_evidence.py --output-json docs/results/ctp_readiness_rollback.json --environment single-host-ubuntu --query-latency-ms 1200 --flow-control-hits 1 --disconnect-recovery-success-rate 1.0 --reject-classified-ratio 1.0 --core-process-alive true --redis-health healthy --timescale-health healthy --strategy-bridge-chain-status complete
```

- [ ] Rollback must finish within `180` seconds.
- [ ] Persist rollback evidence to `docs/results/ctp_rollback_result.env`.
