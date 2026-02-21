#!/usr/bin/env bash
set -euo pipefail

build_dir="build"
results_dir="docs/results"
baseline_json="tests/regression/backtest_consistency/baseline/legacy_python/backtest_baseline.json"
provenance_json="tests/regression/backtest_consistency/baseline/legacy_python/provenance.json"
csv_path="runtime/benchmarks/backtest/rb_ci_sample.csv"
dataset_root="runtime/benchmarks/backtest/parquet_v2"
abs_tol="1e-8"
rel_tol="1e-6"
max_ticks="4"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --build-dir PATH          Build directory containing backtest_consistency_cli
  --results-dir PATH        Output directory for consistency report
  --baseline-json PATH      Baseline JSON path
  --provenance-json PATH    Baseline provenance JSON path
  --csv-path PATH           Source CSV used to bootstrap parquet dataset
  --dataset-root PATH       Parquet dataset root path
  --max-ticks N             Max ticks for backtest run (default: 4)
  --abs-tol V               Absolute tolerance (default: 1e-8)
  --rel-tol V               Relative tolerance (default: 1e-6)
  -h, --help                Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      build_dir="$2"
      shift 2
      ;;
    --results-dir)
      results_dir="$2"
      shift 2
      ;;
    --baseline-json)
      baseline_json="$2"
      shift 2
      ;;
    --provenance-json)
      provenance_json="$2"
      shift 2
      ;;
    --csv-path)
      csv_path="$2"
      shift 2
      ;;
    --dataset-root)
      dataset_root="$2"
      shift 2
      ;;
    --max-ticks)
      max_ticks="$2"
      shift 2
      ;;
    --abs-tol)
      abs_tol="$2"
      shift 2
      ;;
    --rel-tol)
      rel_tol="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -x "${build_dir}/backtest_consistency_cli" ]]; then
  echo "error: missing executable: ${build_dir}/backtest_consistency_cli" >&2
  exit 2
fi
if [[ ! -x "${build_dir}/csv_to_parquet_cli" ]]; then
  echo "error: missing executable: ${build_dir}/csv_to_parquet_cli" >&2
  exit 2
fi

bash scripts/build/check_backtest_baseline.sh \
  --baseline-json "${baseline_json}" \
  --provenance-json "${provenance_json}"

mkdir -p "${results_dir}"
mkdir -p "$(dirname "${csv_path}")"
mkdir -p "${dataset_root}"
report_json="${results_dir}/backtest_consistency_report.json"

if [[ ! -f "${csv_path}" ]]; then
  cat >"${csv_path}" <<'CSV'
symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest
rb2405,SHFE,1704067200000000000,100.0,1,99.9,5,100.1,5,10,1000,100
rb2405,SHFE,1704067201000000000,101.0,1,100.9,5,101.1,5,11,1111,100
rb2405,SHFE,1704067202000000000,99.0,1,98.9,5,99.1,5,12,1188,100
rb2405,SHFE,1704067203000000000,102.0,1,101.9,5,102.1,5,13,1326,100
CSV
fi

"${build_dir}/csv_to_parquet_cli" \
  --input_csv "${csv_path}" \
  --output_root "${dataset_root}" \
  --source rb \
  --resume true >/dev/null

"${build_dir}/backtest_consistency_cli" \
  --engine_mode parquet \
  --dataset_root "${dataset_root}" \
  --max_ticks "${max_ticks}" \
  --deterministic_fills true \
  --run_id backtest-consistency-gate \
  --baseline_json "${baseline_json}" \
  --output_json "${report_json}" \
  --abs_tol "${abs_tol}" \
  --rel_tol "${rel_tol}"

echo "backtest consistency compare passed: ${report_json}"
