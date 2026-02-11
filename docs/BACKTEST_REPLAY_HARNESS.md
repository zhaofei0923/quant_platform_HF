# Backtest Replay Harness

## Goal
- Replay `backtest_data/*.csv` into minute-bar events for deterministic strategy-runtime regression.

## Data Format
- CSV columns:
  - `TradingDay, InstrumentID, UpdateTime, UpdateMillisec, LastPrice, Volume, BidPrice1, BidVolume1, AskPrice1, AskVolume1, AveragePrice, Turnover, OpenInterest`

## Python API
- `quant_hft.backtest.replay.replay_csv_minute_bars(...)`
- `quant_hft.backtest.replay.replay_csv_with_deterministic_fills(...)`
- Inputs:
  - CSV path
  - `StrategyRuntime`
  - `ctx`
  - optional `max_ticks`
  - optional `wal_path` (deterministic fill mode)
- Output:
  - `ReplayReport(...)` with:
    - `ticks_read/bars_emitted/intents_emitted`
    - `first_instrument/last_instrument`
    - `instrument_count/instrument_universe`
    - `first_ts_ns/last_ts_ns`
  - `DeterministicReplayReport(...)` with:
    - replay summary
    - `instrument_bars`
    - `instrument_pnl`
    - `total_realized_pnl` / `total_unrealized_pnl`
    - `invariant_violations`
    - deterministic order-event/WAL counts

## CLI
```bash
scripts/backtest/replay_csv.py --csv backtest_data/rb.csv --max-ticks 5000
```

Optional report outputs:
```bash
scripts/backtest/replay_csv.py \
  --csv backtest_data/rb.csv \
  --scenario-template deterministic_regression \
  --report-json runtime/backtest/report.json \
  --report-md runtime/backtest/report.md
```

Example output:
```text
csv_replay_report run_id=... mode=... input_sig=... data_sig=... ticks=5000 bars=44 intents=0 instruments=1 time_range=... first_instrument=rb2305 last_instrument=rb2305
```

## Current Regression Coverage
- Synthetic fixture for minute-bar rollup and strategy intent counting.
- Real-data smoke replay on `backtest_data/rb.csv` (first 2000 ticks).
- Deterministic scenario suite:
  - multi-instrument rollup counts
  - deterministic order-event to WAL consistency
  - strategy PnL and position invariants

## Next Extensions
- Multi-day scenario packs with session boundary handling.
- Cost model extensions (commission/slippage) for deterministic fills.
- Cross-check deterministic WAL against `wal_replay_tool` replay snapshots.
