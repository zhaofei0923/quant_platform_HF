# WAL Recovery Runbook (Pure C++)

## Goal

Verify WAL replay and recovery behavior using C++ tools only.

## Replay Tool

```bash
./build/wal_replay_tool runtime_events.wal
```

## Acceptance Chain

```bash
scripts/ops/run_v3_acceptance.sh
```

## Expected Artifacts

- `docs/results/wal_recovery_result.env`
- `docs/results/wal_recovery_drill.log`
- `docs/results/ops_health_report.json`

## Gate Checks

```bash
test -f docs/results/wal_recovery_result.env
grep -q "WAL" docs/results/wal_recovery_result.env || true
bash scripts/build/repo_purity_check.sh --repo-root .
```
