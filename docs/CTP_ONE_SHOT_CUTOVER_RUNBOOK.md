# CTP One-Shot Cutover Runbook (Pure C++)

## Purpose

Execute dry-run or execute-mode cutover planning and emit machine-verifiable cutover/rollback evidence.

## Inputs

- CTP config: `configs/sim/ctp.yaml` (or target environment config)
- Cutover template env: `configs/ops/ctp_cutover.template.env`
- Rollback template env: `configs/ops/ctp_rollback_drill.template.env`

## Dry Run

```bash
./build/ctp_cutover_orchestrator_cli \
  --cutover-template configs/ops/ctp_cutover.template.env \
  --rollback-template configs/ops/ctp_rollback_drill.template.env \
  --cutover-output docs/results/cutover_result.env \
  --rollback-output docs/results/rollback_result.env
```

## Execute Mode

```bash
./build/ctp_cutover_orchestrator_cli \
  --cutover-template configs/ops/ctp_cutover.template.env \
  --rollback-template configs/ops/ctp_rollback_drill.template.env \
  --cutover-output docs/results/cutover_result.env \
  --rollback-output docs/results/rollback_result.env \
  --execute
```

## Required Evidence

- `docs/results/cutover_result.env`
- `docs/results/rollback_result.env`
- `docs/results/ops_health_report.json`
- `docs/results/ops_alert_report.json`
