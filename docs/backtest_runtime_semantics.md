# Backtest runtime semantics

The research API is `quant_hft::backtest` in `include/quant_hft/backtest/replay_runtime.h`.
`src/backtest/replay_runtime.cpp` owns orchestration; `replay_helpers.cpp` owns parsing and
report helpers. The old `quant_hft::apps` headers are compatibility facades.

Reports and cache signatures carry `computation_semantics_version=3.0.0`. This identifies the
corrected bar, execution, risk and validation behavior independently of the unchanged
`hf_standard.version=2.0` report format. Older outputs without a computation version belong
to legacy behavior and cannot be treated as matching the corrected runtime.

## Defaults and explicit research mode

| Field | Default | Contract |
|---|---|---|
| `behavior_profile` | `online_parity` | Shared market pipeline, complete bars, SimNow decision configuration, flat-only selection and warmup |
| `parameter_profile` | `sim` | Selects atomic parameter overrides independently of `run_type=backtest` |
| `online_runtime_config_path` | `configs/sim/ctp.yaml` | Reads allowed pure decision fields; no environment expansion or gateway/account connection |
| `initialization_policy` | `cold_start` | Empty simulator account and reset strategy; complete bars warm the strategy before opening |
| `input_timestamp_basis` | `legacy_exchange_local` | Epoch encoding of input records; use `utc` for an already UTC-encoded dataset |
| `rollover_mode` | `flat_only` | Uses `DominantContractCoordinator`; requires explicit eligible instrument coverage |
| `product_series_mode` | `raw` | Execution and analysis prices start identical |
| `streaming` | `true` | Incremental Parquet batches and stable lazy merge |

CLI accepts underscore and hyphen forms for the new profile/time fields. Explicit
`behavior_profile=research` defaults to `parameter_profile=backtest` and `rollover_mode=strict`.
Continuous-adjusted prices and strict/carry/expiry-close rollover require explicit research
mode. Old archived OOS results without a profile retain explicit research/backtest semantics.
A live override can be selected explicitly; it does not turn the simulator into a gateway.

Online parity requires the effective runtime file to select flat-only/direct execution and
a supported risk rule snapshot. Unsupported custom execution or grouped CTP templates fail explicitly.
Runtime reports contain the effective pure parameters and source/effective content identities,
not credentials. The identities detect ordinary input changes; they are not cryptographic attestations.

## Market, clock and execution order

Online parity converts the selected input epoch encoding to UTC once, preserving exchange-local
`action_day` and `update_time`. Legacy research mode retains its exchange-local encoded internal
clock. For Parquet, `partition.trading_day` selects the date interval, so a Monday trading day
keeps its Friday night session. Raw epoch/calendar fields must describe the same physical event.

The virtual poll clock starts at the first input event and uses the configured polling interval.
Each tick first applies eligible previously submitted simulated fills, then derives canonical
states/bars through `MarketBarPipeline`, then calls the strategy tick risk callback. A signal
cannot fill at its own timestamp: execution requires a strictly later tick. Missing minutes,
unknown initial cumulative-volume baselines and shutdown partial bars cannot advance tradable
state. Shutdown preserves pending buckets in the market checkpoint without inventing future ticks.

Contract selection uses the shared coordinator with observed candidate quotes, simulator positions
and pending orders. Opens are gated through selection, generation checks and warmup. Missing
candidate coverage fails parity. A gap resets detector/bucket state and suppresses opening; only
consecutive complete, conflict-free recovery bars release pipeline suppression. Full strategy
warmup remains a separate requirement.

Execution remains deterministic next-tick simulation using the configured price policy. The
supported runtime gates include session, order size/notional, position notional, pending order
count, signal/market freshness and pending cancellation age. Historical input does not replay
broker reconciliation, exchange queue priority or measured gateway transport latency. Reports
state this execution boundary and final coordinator/warmup status.

Order admission uses the same `RiskManager`, rule-file loader, and unit definitions as online.
`risk_rule_file_path` defaults to `configs/risk_rules.yaml`; its contents participate in effective
runtime and OOS cache identities. The shared token bucket accepts a virtual monotonic clock.
Pending simulated orders are projected into `OrderManager` so self-trade and active-order checks
see the same account scope. The replay uses an immutable risk snapshot and rejects a runtime/rule
file changed during a run; it cannot reconstruct historical live rule updates without events.
Enabled Sim subaccount budgets fail before reading market input. Applicable daily settlement-loss,
total-position/leverage or timed rules without a supported replay context also fail explicitly;
gateway insert/cancel backpressure requiring broker acknowledgement history fails instead of
inventing a wait or silently ignoring the rule. The default disabled Sim budget is supported.

Research continuous adjustment implements `IBarAnalysisTransform`. Its identity and offsets are
saved and validated with the same pipeline checkpoint; execution OHLC remains raw. Recovery
cannot load an adjusted checkpoint into a raw transform silently.

## Optimization and validation

Trial generation writes optimized values into both base parameters and the selected parameter
overrides so a pre-existing `sim` override cannot mask a tested value. OOS cache identities include
validation dates, normalized spec, configuration and data bytes, archived strategy inputs and
executable content. Unsigned old caches and modified result payloads are not reused.

OOS candidate ranking and recommendation use the recorded training objective and direction.
The recommendation period is labelled `validation_selection`; it is not an untouched final test.
Rolling reports label independent windows and independent cold-start capital; they do not claim
a continuous funded equity path across windows.

Optimization configuration accepts `max_parallel`, `memory_budget_mb` and `per_task_memory_mb`.
A positive memory budget requires a positive per-task estimate no larger than that budget.
Concurrency is capped by CPU policy, requested parallelism and `floor(budget / estimate)`.
These values describe declared input working sets, not enforced process RSS limits. Full retained
trades, orders, state/indicator traces and report histories require a separate capacity allowance.

The input cursor holds one bounded record batch per simultaneously active partition. It disables
Arrow pre-buffering, uses a 64 KiB buffered input stream, and resolves columns once per batch.
Codec pages/dictionaries and manifest metadata also consume memory; their size is not represented
by the buffered-row counter. Explicit `streaming=false` retains materializing compatibility.

See the regression tests in `tests/unit/apps/replay_market_parity_test.cpp`,
`tests/unit/apps/oos_top10_validation_test.cpp`, `tests/unit/backtest/parquet_data_feed_test.cpp`,
`tests/unit/services/market_bar_pipeline_test.cpp` and `tests/unit/optim/task_scheduler_test.cpp`.
