# Backtest Replay Harness (Pure C++)

## Purpose

Run deterministic replay using `backtest_cli` and validate parity/performance using C++ CLIs.

## Basic Replay

```bash
mkdir -p docs/results
./build/backtest_cli \
  --engine_mode csv \
  --csv_path backtest_data/rb.csv \
  --max_ticks 5000 \
  --output_json docs/results/backtest_cli_smoke.json \
  --output_md docs/results/backtest_cli_smoke.md
```

## Parquet Replay

```bash
./build/backtest_cli \
  --engine_mode parquet \
  --dataset_root backtest_data/parquet \
  --start_date 2025-01-01 \
  --end_date 2025-01-31 \
  --output_json docs/results/backtest_cli_parquet.json
```

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
  --dataset_root backtest_data/parquet \
  --output_json docs/results/csv_parquet_speed_compare_c.json
```

## Required Outputs

- Replay JSON: `docs/results/backtest_cli_*.json`
- Benchmark JSON: `docs/results/backtest_benchmark_result.json`
- Compare JSON: `docs/results/csv_parquet_speed_compare_c.json`
