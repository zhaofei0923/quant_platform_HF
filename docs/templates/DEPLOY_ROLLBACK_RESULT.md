# Deploy/Rollback Drill Result

Record one deployment rollback drill using `key=value` lines.

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

Recommended checks:

```bash
test -f docs/results/rollback_result.env
grep -q "ROLLBACK" docs/results/rollback_result.env || true
./build/ops_health_report_cli --output_json docs/results/ops_health_report.json --output_md docs/results/ops_health_report.md
```
