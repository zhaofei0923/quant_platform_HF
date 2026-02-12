# Redis Strategy Bridge Protocol (Quant HFT)

This document freezes the cross-process contract between:
- `core_engine` (C++) publishing market/state and consuming strategy intents
- `strategy_runner` (Python) consuming state and publishing strategy intents

The bridge uses Redis Hashes (`HSET` / `HGETALL`) and polling. No pub/sub is required.

## Change Summary (2026-02-11)

1. CTP order-state enrichment fields are now persisted in `trade:order:<client_order_id>:info`:
   - `exchange_id`, `status_msg`, `order_submit_status`, `order_ref`
   - `front_id`, `session_id`, `trade_id`, `event_source`
2. Market snapshot normalization now standardizes CTP session/date semantics before write:
   - `exchange_id`, `trading_day`, `action_day`, `update_time`, `update_millisec`
   - `settlement_price`, `average_price_raw`, `average_price_norm`, `is_valid_settlement`
3. Compatibility remains unchanged:
   - existing readers can ignore unknown fields;
   - default values are written for missing upstream fields.

## Conventions

- All hash fields are stored as strings.
- All timestamps are `ts_ns` (Unix epoch nanoseconds).
- `trace_id` must be globally unique.
- `client_order_id` is set to `trace_id` by default to simplify round-tripping order events.

## Keys

### 1) Latest 7D state snapshot

Key:

`market:state7d:<instrument_id>:latest`

Fields:

- `instrument_id`
- `trend_score`, `trend_confidence`
- `volatility_score`, `volatility_confidence`
- `liquidity_score`, `liquidity_confidence`
- `sentiment_score`, `sentiment_confidence`
- `seasonality_score`, `seasonality_confidence`
- `pattern_score`, `pattern_confidence`
- `event_drive_score`, `event_drive_confidence`
- `ts_ns`

### 2) Strategy intent inbox (latest batch)

Each strategy writes a single "latest batch" inbox. The consumer uses `seq` to avoid
reprocessing the same batch.

Key:

`strategy:intent:<strategy_id>:latest`

Fields:

- `seq`: monotonically increasing integer (per `strategy_id`)
- `count`: number of intents in this batch
- `intent_0 ... intent_{count-1}`: encoded intent strings (see below)
- `ts_ns`: optional; batch creation time

`intent_i` encoding:

7 segments separated by `|` (the `|` character must not appear inside any segment):

1. `instrument_id`
2. `side`: `BUY` or `SELL`
3. `offset`: `OPEN`, `CLOSE`, `CLOSE_TODAY`, `CLOSE_YESTERDAY`
4. `volume`: integer
5. `limit_price`: float
6. `signal_ts_ns`: integer
7. `trace_id`: string

Example:

`SHFE.ag2406|BUY|OPEN|2|4500.0|1730000000000000000|trace-0001`

### 3) Latest order event (existing key schema)

Key:

`trade:order:<client_order_id>:info`

Notes:

- The producer (`core_engine`) writes this via `RedisRealtimeStoreClientAdapter`.
- The consumer (`strategy_runner`) should treat it as the latest snapshot of the order.
- Because `client_order_id == trace_id` by default, Python can locate the order event by `trace_id`.
- Optional execution metadata fields:
  - `execution_algo_id`, `slice_index`, `slice_total`, `throttle_applied`
  - `venue`, `route_id`, `slippage_bps`, `impact_cost`
  - CTP enrichment: `exchange_id`, `status_msg`, `order_submit_status`, `order_ref`,
    `front_id`, `session_id`, `trade_id`, `event_source`

## Sequencing and idempotency

- `strategy_runner` increments `seq` when it publishes a new batch.
- `core_engine` tracks the last processed `seq` per `strategy_id`.
- If the same `seq` is seen again, it is ignored.

## Data Governance Hooks

- Dictionary source: `python/quant_hft/data_pipeline/data_dictionary.py`.
- Lifecycle policy (local verifiable hot/warm/cold): `scripts/data_pipeline/run_lifecycle.py`.
- Redis/Timescale reconciliation: `scripts/data_pipeline/run_reconcile.py`.

Recommended repository-level evidence flow:

1. Export order events from analytics storage to CSV.
2. Capture Redis order snapshots into JSON.
3. Run `run_reconcile.py` and archive the JSON report under `docs/results/`.
