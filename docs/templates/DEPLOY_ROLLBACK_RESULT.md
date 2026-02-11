# Deploy/Rollback Drill Result

Record one deployment rollback drill using `key=value` lines so the verifier can parse it directly.

```text
drill_id=<YYYYMMDD-HHMM>
platform=<systemd|k8s>
release_version=<vX.Y.Z or timestamp>
rollback_target=<previous stable version>
failure_injected=<short fault label>
rollback_start_utc=<YYYY-MM-DDTHH:MM:SSZ>
rollback_complete_utc=<YYYY-MM-DDTHH:MM:SSZ>
rollback_seconds=<measured float>
health_check_passed=<true|false>
result=<pass|fail>
operator=<name>
notes=<short summary>
```

Verifier command:

```bash
python3 scripts/ops/verify_deploy_rollback_evidence.py \
  --evidence-file docs/results/deploy_rollback_result.env \
  --max-rollback-seconds 180
```
