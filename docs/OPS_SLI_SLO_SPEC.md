# Ops SLI/SLO Specification

## Scope

This document defines the minimum SLI/SLO contract for repository-level ops evidence.

## Naming Convention

- All SLI names must use the `quant_hft_` prefix.
- Canonical names are defined in `python/quant_hft/ops/sli_catalog.py`.

## Minimum SLI Set

1. `quant_hft_core_process_alive`
2. `quant_hft_strategy_bridge_latency_p99_ms`
3. `quant_hft_strategy_bridge_chain_integrity`
4. `quant_hft_storage_redis_health`
5. `quant_hft_storage_timescale_health`

## Alert Policy

- `info`: all SLI healthy.
- `warn`: non-critical SLI unhealthy.
- `critical`: `core_process_alive`, chain integrity, or storage health unhealthy.

Reference implementation:
- `python/quant_hft/ops/alert_policy.py`
- `scripts/ops/render_alert_report.py`

## Evidence Artifacts

- Health JSON: `docs/results/ops_health_report.json`
- Health Markdown: `docs/results/ops_health_report.md`
- Alert JSON: `docs/results/ops_alert_report.json`
- Alert Markdown: `docs/results/ops_alert_report.md`
