# Ops SLI/SLO Spec (Pure C++)

## Primary SLI Keys

- `quant_hft_core_process_alive`
- `quant_hft_strategy_engine_latency_p99_ms`
- `quant_hft_strategy_engine_chain_integrity`
- `quant_hft_storage_redis_health`
- `quant_hft_storage_timescale_health`
- `quant_hft_storage_postgres_health`

## Artifact Producers

- Health report: `ops_health_report_cli`
- Alert report: `ops_alert_report_cli`
- Reconnect evidence: `reconnect_evidence_cli`

## Validation Commands

```bash
./build/ops_health_report_cli --output_json docs/results/ops_health_report.json --output_md docs/results/ops_health_report.md
./build/ops_alert_report_cli --health-json-file docs/results/ops_health_report.json --output_json docs/results/ops_alert_report.json --output_md docs/results/ops_alert_report.md
```

## Acceptance Rule

Critical alerts must be empty for readiness sign-off.
