# Backtest Replay Harness

## Goal
- Replay `backtest_data/*.csv` into minute-bar events for deterministic strategy-runtime regression.

## Data Format
- CSV columns:
  - `TradingDay, InstrumentID, UpdateTime, UpdateMillisec, LastPrice, Volume, BidPrice1, BidVolume1, AskPrice1, AskVolume1, AveragePrice, Turnover, OpenInterest`
- 主力连续数据契约：
  - `backtest_data` 下每个 CSV 对应一个品种，文件名即品种名（如 `rb.csv`）
  - 文件内允许同品种多个合约拼接（主力连续），但每行必须保留真实 `InstrumentID`
  - 生产门禁默认要求：文件名品种前缀必须与 `InstrumentID` 字母前缀一致

## Data Conversion & Validation

Convert CSV to parquet partitions:
```bash
python3 scripts/data/convert_backtest_csv_to_parquet.py \
  --execute \
  --input backtest_data \
  --output-dir runtime/backtest/parquet \
  --report-json docs/results/backtest_parquet_conversion_report.json \
  --filename-prefix-policy error \
  --rollover-min-gap-ms 0
```

Validate parquet dataset:
```bash
python3 scripts/data/validate_backtest_parquet_dataset.py \
  --dataset-root runtime/backtest/parquet \
  --report-json docs/results/backtest_parquet_validation_report.json \
  --require-non-empty \
  --conversion-report docs/results/backtest_parquet_conversion_report.json
```

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
  --emit-state-snapshots \
  --report-json runtime/backtest/report.json \
  --report-md runtime/backtest/report.md
```

Replay parameter notes:
- `--engine-mode` supports `csv` / `parquet` / `core_sim`.
- `--dataset-root` is required when `--engine-mode=parquet`.
- `--engine-mode=core_sim` requires either `--dataset-root` or `--csv` as input source.
- `--rollover-mode` controls contract switch handling: `strict|carry`.
- `--rollover-price-mode` controls pricing source in `core_sim strict`: `bbo|mid|last`.
- `--rollover-slippage-bps` adds configurable bps slippage on rollover execution legs.
- Deterministic report now includes `rollover_events` (summary) and `rollover_actions` (audit actions: `cancel|close|open|carry`), and WAL includes `kind=rollover` records for parity checks.

Core sim replay example:
```bash
scripts/backtest/replay_csv.py \
  --engine-mode core_sim \
  --csv backtest_data/rb.csv \
  --rollover-mode strict \
  --rollover-price-mode bbo \
  --rollover-slippage-bps 5 \
  --deterministic-fills \
  --report-json runtime/backtest/report_core_sim.json
```

Parquet replay example:
```bash
scripts/backtest/replay_csv.py \
  --engine-mode parquet \
  --dataset-root runtime/backtest/parquet/<run_id> \
  --scenario-template deterministic_regression \
  --report-json runtime/backtest/report_parquet.json
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
