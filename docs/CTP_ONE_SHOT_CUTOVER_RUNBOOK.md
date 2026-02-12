# CTP One-Shot Cutover Runbook (Single-Host Ubuntu)

This runbook executes the one-shot full switch path for the upgraded CTP architecture.
It is pinned to a single-host Ubuntu production baseline.

## 1) Prepare template parameters

Fill these files first:
- `configs/ops/ctp_cutover.template.env`
- `configs/ops/ctp_rollback_drill.template.env`

## 2) Generate executable checklist

```bash
python3 scripts/ops/generate_ctp_cutover_plan.py \
  --cutover-template configs/ops/ctp_cutover.template.env \
  --rollback-template configs/ops/ctp_rollback_drill.template.env \
  --output-md docs/results/ctp_one_shot_cutover_checklist.md \
  --output-json docs/results/ctp_one_shot_cutover_checklist.json
```

Use the generated `docs/results/ctp_one_shot_cutover_checklist.md` as the operation checklist.

## 3) Execute orchestration (dry-run first)

Dry-run:

```bash
python3 scripts/ops/ctp_cutover_orchestrator.py \
  --cutover-template configs/ops/ctp_cutover.template.env \
  --rollback-template configs/ops/ctp_rollback_drill.template.env \
  --cutover-output docs/results/ctp_cutover_result.env \
  --rollback-output docs/results/ctp_rollback_result.env
```

Execute real commands:

```bash
python3 scripts/ops/ctp_cutover_orchestrator.py \
  --cutover-template configs/ops/ctp_cutover.template.env \
  --rollback-template configs/ops/ctp_rollback_drill.template.env \
  --cutover-output docs/results/ctp_cutover_result.env \
  --rollback-output docs/results/ctp_rollback_result.env \
  --execute
```

Optional rollback drill in execute mode:

```bash
python3 scripts/ops/ctp_cutover_orchestrator.py \
  --cutover-template configs/ops/ctp_cutover.template.env \
  --rollback-template configs/ops/ctp_rollback_drill.template.env \
  --cutover-output docs/results/ctp_cutover_result.env \
  --rollback-output docs/results/ctp_rollback_result.env \
  --execute \
  --force-rollback
```

## 4) Cutover gate commands

### 4.1 Infra/bootstrap gate

```bash
bash scripts/infra/bootstrap_prodlike.sh \
  --profile single-host \
  --compose-file infra/docker-compose.single-host.yaml \
  --project-name quant-hft-single-host \
  --env-file infra/env/prodlike.env \
  --execute
bash scripts/infra/init_kafka_topics.sh \
  --compose-file infra/docker-compose.single-host.yaml \
  --project-name quant-hft-single-host \
  --env-file infra/env/prodlike.env \
  --execute
bash scripts/infra/init_clickhouse_schema.sh \
  --compose-file infra/docker-compose.single-host.yaml \
  --project-name quant-hft-single-host \
  --env-file infra/env/prodlike.env \
  --execute
bash scripts/infra/init_debezium_connectors.sh \
  --connect-url http://kafka-connect:8083 \
  --execute
```

### 4.2 Readiness gate

```bash
python3 scripts/ops/run_ctp_readiness_evidence.py \
  --output-json docs/results/ctp_readiness_precheck.json \
  --environment single-host-ubuntu \
  --query-latency-ms 1200 \
  --flow-control-hits 1 \
  --disconnect-recovery-success-rate 1.0 \
  --reject-classified-ratio 1.0 \
  --core-process-alive true \
  --redis-health healthy \
  --timescale-health healthy \
  --strategy-bridge-chain-status complete
```

## 5) One-shot switch sequence

1. Stop old core engine and old strategy runner.
2. Start new core engine (`core_engine`) and new direct runner (`run_ctp_strategy.py`).
3. Verify login -> settlement confirmation -> instrument warmup query.
4. Enter 30-minute watch window for:
   - order latency p99
   - breaker states
   - query rate limit violations
   - reconnect p99
   - kafka lag
   - clickhouse ingest delay

## 6) Rollback drill sequence

1. Stop new core engine and new direct runner.
2. Restore previous binary/runtime package.
3. Switch to Redis compatibility path if required.
4. Start previous core engine and previous strategy runner.
5. Run readiness evidence command and save rollback evidence.

## 7) Mandatory evidence outputs

- `docs/results/single_host_bootstrap_result.env`
- `docs/results/kafka_topic_init_result.env`
- `docs/results/clickhouse_schema_init_result.env`
- `docs/results/debezium_connector_init_result.env`
- `docs/results/ctp_readiness_precheck.json`
- `docs/results/ctp_cutover_result.env`
- `docs/results/ctp_rollback_result.env`

## 8) Evidence verification gate

```bash
python3 scripts/ops/verify_ctp_cutover_evidence.py \
  --cutover-evidence-file docs/results/ctp_cutover_result.env \
  --rollback-evidence-file docs/results/ctp_rollback_result.env \
  --max-rollback-seconds 180
```

For rollback drills where cutover failure is intentional:

```bash
python3 scripts/ops/verify_ctp_cutover_evidence.py \
  --cutover-evidence-file docs/results/ctp_cutover_result.env \
  --rollback-evidence-file docs/results/ctp_rollback_result.env \
  --allow-cutover-rollback \
  --max-rollback-seconds 180
```

## 9) Related scripts

- `scripts/ops/generate_ctp_cutover_plan.py`
- `scripts/ops/ctp_cutover_orchestrator.py`
- `scripts/ops/verify_ctp_cutover_evidence.py`
- `scripts/strategy/run_ctp_strategy.py`
- `scripts/strategy/run_strategy.py`
- `scripts/infra/bootstrap_prodlike.sh`
- `scripts/infra/init_kafka_topics.sh`
- `scripts/infra/init_clickhouse_schema.sh`
- `scripts/infra/init_debezium_connectors.sh`
