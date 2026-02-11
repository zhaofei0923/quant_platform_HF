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
