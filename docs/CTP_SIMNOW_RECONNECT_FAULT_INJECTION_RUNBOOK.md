# CTP SimNow Reconnect Evidence Runbook (Pure C++)

## Goal

Generate reconnect evidence and SLI/SLO artifacts without Python orchestration.

## Build

```bash
cmake -S . -B build-real -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_ENABLE_CTP_REAL_API=ON
cmake --build build-real -j$(nproc)
```

## Probe (Optional)

```bash
export CTP_SIM_PASSWORD='your_password'
LD_LIBRARY_PATH=$PWD/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64:$LD_LIBRARY_PATH \
  ./build-real/simnow_probe configs/sim/ctp_trading_hours.yaml
```

## Evidence Generation

```bash
mkdir -p docs/results
./build/reconnect_evidence_cli \
  --config-profile configs/sim/ctp_trading_hours.yaml \
  --report_file docs/results/reconnect_fault_result.md \
  --health_json_file docs/results/ops_health_report.json \
  --health_markdown_file docs/results/ops_health_report.md \
  --alert_json_file docs/results/ops_alert_report.json \
  --alert_markdown_file docs/results/ops_alert_report.md
```

## Validation

```bash
grep -q "quant_hft_strategy_engine_chain_integrity" docs/results/ops_health_report.md
test -f docs/results/ops_alert_report.json
test -f docs/results/reconnect_fault_result.md
```

## Output Contract

- `docs/results/reconnect_fault_result.md`
- `docs/results/ops_health_report.json`
- `docs/results/ops_health_report.md`
- `docs/results/ops_alert_report.json`
- `docs/results/ops_alert_report.md`
