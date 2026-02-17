# WAL Recovery Drill Result

Record one drill using `key=value` lines.

```text
drill_id=<YYYYMMDD-HHMM>
release_version=<vX.Y.Z or timestamp>
wal_path=<path>
failure_start_utc=<YYYY-MM-DDTHH:MM:SSZ>
recovery_complete_utc=<YYYY-MM-DDTHH:MM:SSZ>
rto_seconds=<measured float>
rpo_events=<measured integer>
operator=<name>
result=<pass|fail>
notes=<short summary>
```

Recommended checks:

```bash
./build/wal_replay_tool <wal_path>
test -f docs/results/wal_recovery_result.env
bash scripts/build/repo_purity_check.sh --repo-root .
```
