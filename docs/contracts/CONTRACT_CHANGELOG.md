# Contract Changelog

## 2026-02-11 M0 Contract Freeze

### RiskDecision

- Added `policy_id`
- Added `policy_scope`
- Added `observed_value`
- Added `threshold_value`
- Added `decision_tags`

### OrderEvent

- Added `execution_algo_id`
- Added `slice_index`
- Added `slice_total`
- Added `throttle_applied`

### Contract Sync Gate

- Added `scripts/build/verify_contract_sync.py`
- Added CI gate to fail when C++ / proto / Python contract fields drift

## 2026-02-11 M0+ Interface Extension

### Risk Engine Interface

- Extended `RiskContext` with cross-account exposure fields:
  - `account_cross_gross_notional`
  - `account_cross_net_notional`
- Added `IRiskEngine::ReloadPolicies(...)`
- Added `IRiskEngine::EvaluateExposure(...)`

### OrderEvent

- Added `venue`
- Added `route_id`
- Added `slippage_bps`
- Added `impact_cost`

### Execution and Governance

- Added `ExecutionRouter` for route id / slippage / impact estimation
- Added `scripts/build/verify_develop_requirements.py` and
  `docs/requirements/develop_requirements.yaml` for `develop/` requirement traceability
