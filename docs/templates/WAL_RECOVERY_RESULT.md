# WAL Recovery Drill Result

Record one drill using `key=value` lines so the verifier can parse it directly.

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

Verifier command:

```bash
python3 scripts/ops/verify_wal_recovery_evidence.py \
  --evidence-file docs/results/wal_recovery_result.env \
  --max-rto-seconds 10 \
  --max-rpo-events 0
```
