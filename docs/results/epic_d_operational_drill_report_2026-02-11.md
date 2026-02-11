# Epic D Operational Drill Report (2026-02-11)

## Scope
- Deploy/Rollback drill using packaged artifacts (`v0.2.9-dev2` -> rollback to `v0.2.9-dev1`).
- WAL recovery drill using `build/wal_replay_tool` on `runtime_events.wal`.

## Drill 1: Deploy/Rollback (Systemd-style Local Drill)

### Inputs
- Candidate bundle: `dist/quant-hft-nonhotpath-v0.2.9-dev2.tar.gz`
- Rollback bundle: `dist/quant-hft-nonhotpath-v0.2.9-dev1.tar.gz`
- Evidence file: `docs/results/deploy_rollback_result.env`
- Raw log: `docs/results/deploy_rollback_drill.log`

### Procedure (executed)
1. Verify both bundles with `verify_nonhotpath_release.py`.
2. Extract both bundles to runtime drill workspace.
3. Deploy candidate bundle and run `scripts/data_pipeline/run_pipeline.py --run-once` as health check.
4. Inject failure by removing candidate `run_pipeline.py` entrypoint.
5. Confirm post-fault health check fails.
6. Roll back symlink to rollback bundle and run health check again.
7. Record rollback timing and result.

### Outcome
- Post-fault health check failed as expected (`rc=2`).
- Post-rollback health check passed.
- Measured rollback time: `0.110s`.
- Evidence verification: PASS.

## Drill 2: WAL Recovery

### Inputs
- WAL file: `runtime_events.wal`
- Tool: `build/wal_replay_tool`
- Evidence file: `docs/results/wal_recovery_result.env`
- Raw log: `docs/results/wal_recovery_drill.log`

### Procedure (executed)
1. Run `./build/wal_replay_tool runtime_events.wal`.
2. Parse replay output counters.
3. Compute measured RTO from wall-clock timestamps.
4. Compute RPO as `parse_errors + state_rejected`.
5. Verify evidence against SLO thresholds.

### Outcome
- Replay output: `lines=4 events=4 parse_errors=0 state_rejected=0 ledger_applied=4`.
- Measured RTO: `0.018s`.
- Measured RPO events: `0`.
- Evidence verification: PASS.

## Acceptance Mapping (Epic D)
- Deploy + rollback drill completed: PASS.
- WAL recovery drill with full input/output evidence: PASS.
- Automated verification scripts used in both drills: PASS.

## Verification Commands (executed)
```bash
python3 scripts/ops/verify_deploy_rollback_evidence.py \
  --evidence-file docs/results/deploy_rollback_result.env \
  --max-rollback-seconds 180

python3 scripts/ops/verify_wal_recovery_evidence.py \
  --evidence-file docs/results/wal_recovery_result.env \
  --max-rto-seconds 10 \
  --max-rpo-events 0
```
