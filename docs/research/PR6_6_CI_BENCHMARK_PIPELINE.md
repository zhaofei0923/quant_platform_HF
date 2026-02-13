# PR-6.6 CI Benchmark Pipeline

## 目标

为回测性能门禁补齐可复现的 CI 数据准备与基准校验闭环：

1. 从仓库内固定 CSV 生成小型 benchmark 数据样本；
2. 使用 deterministic backtest 运行性能基准；
3. 按 baseline 阈值在 CI 中判定通过/失败；
4. 产出结构化 JSON 报告用于归档。

## 新增资产

- 数据准备脚本：`scripts/perf/prepare_backtest_benchmark_data.py`
- 基准执行脚本：`scripts/perf/run_backtest_benchmark.py`
- 基准阈值配置：`configs/perf/backtest_benchmark_baseline.json`
- CI 接入：`.github/workflows/ci.yml` (`Backtest Benchmark Data Prep + Gate` step)

## 产物

- `docs/results/backtest_benchmark_data_report.json`
- `docs/results/backtest_benchmark_result.json`

## 本地运行

```bash
python3 scripts/perf/prepare_backtest_benchmark_data.py \
  --input-csv backtest_data/rb.csv \
  --output-csv runtime/benchmarks/backtest/rb_ci_sample.csv \
  --max-ticks 1200 \
  --report-json docs/results/backtest_benchmark_data_report.json

python3 scripts/perf/run_backtest_benchmark.py \
  --csv-path runtime/benchmarks/backtest/rb_ci_sample.csv \
  --baseline configs/perf/backtest_benchmark_baseline.json \
  --result-json docs/results/backtest_benchmark_result.json
```
