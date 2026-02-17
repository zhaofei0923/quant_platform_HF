#!/usr/bin/env bash
set -euo pipefail

build_dir="build"
results_dir="docs/results"
baseline_json="tests/regression/backtest_consistency/baseline/legacy_python/backtest_baseline.json"
provenance_json="tests/regression/backtest_consistency/baseline/legacy_python/provenance.json"
csv_path="runtime/benchmarks/backtest/rb_ci_sample.csv"
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
  --csv-path PATH           CSV dataset path
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

bash scripts/build/check_backtest_baseline.sh \
  --baseline-json "${baseline_json}" \
  --provenance-json "${provenance_json}"

mkdir -p "${results_dir}"
report_json="${results_dir}/backtest_consistency_report.json"

"${build_dir}/backtest_consistency_cli" \
  --engine_mode csv \
  --csv_path "${csv_path}" \
  --max_ticks "${max_ticks}" \
  --deterministic_fills true \
  --run_id backtest-consistency-gate \
  --baseline_json "${baseline_json}" \
  --output_json "${report_json}" \
  --abs_tol "${abs_tol}" \
  --rel_tol "${rel_tol}"

echo "backtest consistency compare passed: ${report_json}"
