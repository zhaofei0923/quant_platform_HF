# Multi-Host Failover Drill Report

- Date: 2026-02-11
- Environment: `prodlike-multi-host`
- Drill Type: repository orchestration dry-run (machine-verifiable contract)
- Result: PASS

## Commands

```bash
python3 scripts/ops/failover_orchestrator.py \
  --env-config configs/deploy/environments/prodlike_multi_host.yaml \
  --output-file docs/results/failover_result.env

python3 scripts/ops/verify_failover_evidence.py \
  --evidence-file docs/results/failover_result.env \
  --max-failover-seconds 300 \
  --max-data-lag-events 0
```

## Evidence Summary

- Evidence file: `docs/results/failover_result.env`
- `FAILOVER_SUCCESS=true`
- `FAILOVER_TOTAL_STEPS=5`
- Ordered steps:
  1. `precheck`
  2. `backup_sync_check`
  3. `demote_primary`
  4. `promote_standby`
  5. `verify`
- `DATA_SYNC_LAG_EVENTS=0`

## Notes

- This drill validates repository-scope failover sequencing and evidence contract.
- Execute-mode host switch operations remain opt-in (`--execute`) for controlled environments.
