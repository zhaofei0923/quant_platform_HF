# Backtest Replay Harness (Pure C++)

## Purpose

Run deterministic replay using `backtest_cli` and validate parity/performance using C++ CLIs.

## Data Preparation (CSV -> Parquet v2)

```bash
mkdir -p docs/results
./build/csv_to_parquet_cli \
  --input_csv backtest_data/rb.csv \
  --output_root backtest_data/parquet_v2 \
  --source rb \
  --resume true \
  --require_arrow_writer false \
  --batch_rows 500000 \
  --memory_budget_mb 1024 \
  --row_group_mb 128 \
  --compression snappy
```

## Basic Replay

```bash
mkdir -p docs/results
./build/backtest_cli \
  --engine_mode csv \
  --csv_path backtest_data/rb.csv \
  --detector_config configs/sim/ctp.yaml \
  --max_ticks 5000 \
  --output_json docs/results/backtest_cli_smoke.json \
  --output_md docs/results/backtest_cli_smoke.md
```

## Parquet Replay

```bash
./build/backtest_cli \
  --engine_mode parquet \
  --dataset_root backtest_data/parquet_v2 \
  --dataset_manifest backtest_data/parquet_v2/_manifest/partitions.jsonl \
  --strict_parquet true \
  --streaming true \
  --start_date 2025-01-01 \
  --end_date 2025-01-31 \
  --output_json docs/results/backtest_cli_parquet.json
```

### Bar Aggregation Notes (CSV + Parquet)

- CSV 与 Parquet 回测都复用实盘 `BarAggregator` 路径进行 1m 合成与多周期 fanout。
- Tick 交易时段过滤在两条回测路径均启用。
- Trading sessions config 是回测硬依赖（CSV/Parquet 一致）：
  - `TRADING_SESSIONS_CONFIG_PATH` has highest priority.
  - fallback is `configs/trading_sessions.yaml`.
  - missing/unreadable config causes fail-fast (no silent fallback).
- Composite 多周期订阅由 `composite.sub_strategies[].timeframe_minutes` 控制；trace parquet 会写出 `timeframe_minutes` 列。

## Benchmark Gate

```bash
./build/backtest_benchmark_cli \
  --runs 5 \
  --baseline_p95_ms 100 \
  --result_json docs/results/backtest_benchmark_result.json
```

## CSV vs Parquet Compare

```bash
./build/csv_parquet_compare_cli \
  --csv_path backtest_data/rb.csv \
  --parquet_root backtest_data/parquet_v2 \
  --max_ticks 500000 \
  --output_json docs/results/csv_parquet_speed_compare_c.json
```

## Consistency Validation

```bash
./build/csv_parquet_compare_cli \
  --csv_path backtest_data/rb.csv \
  --parquet_root backtest_data/parquet_v2 \
  --runs 1 \
  --warmup_runs 0 \
  --max_ticks 200000 \
  --output_json docs/results/csv_parquet_consistency.json
```

Check `equal=true` and inspect `summary.parquet.scan_rows/scan_row_groups/io_bytes/early_stop_hit`.

## Optional Detector Config

`--detector_config` accepts a YAML file with detector keys. Supported layouts:

- top-level keys (for dedicated files), e.g. `adx_period: 14`
- nested keys under `market_state_detector`
- nested keys under `ctp.market_state_detector` (for shared CTP config fragments)

## CI Speedup Gate

```bash
bash scripts/build/run_csv_parquet_speedup_gate.sh \
  --build-dir build \
  --csv-path runtime/benchmarks/backtest/rb_perf_large.csv \
  --source rb \
  --output-root runtime/benchmarks/backtest/parquet_v2_ci \
  --results-dir docs/results \
  --runs 3 \
  --warmup-runs 1 \
  --max-ticks 50000 \
  --min-speedup 5.0 \
  --symbol-count 20 \
  --focus-symbol rb2405 \
  --require-arrow-writer true
```

The gate reads `summary.parquet_vs_csv_speedup` and enforces `>=5.0` (plus `equal=true`).

## Required Outputs

- Replay JSON: `docs/results/backtest_cli_*.json`
- Benchmark JSON: `docs/results/backtest_benchmark_result.json`
- Compare JSON: `docs/results/csv_parquet_speed_compare_c.json`
