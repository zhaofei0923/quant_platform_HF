#!/usr/bin/env bash
set -euo pipefail

build_dir="build"
csv_path="runtime/benchmarks/backtest/rb_ci_sample.csv"
dataset_root="runtime/benchmarks/backtest/parquet_v2"
results_dir="docs/results"
config_path="configs/sim/ctp.yaml"
max_ticks="4"
baseline_json="tests/regression/backtest_consistency/baseline/legacy_python/backtest_baseline.json"
provenance_json="tests/regression/backtest_consistency/baseline/legacy_python/provenance.json"
abs_tol="1e-8"
rel_tol="1e-6"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --build-dir PATH    Build directory containing CLIs (default: build)
  --csv-path PATH     Source CSV used to bootstrap parquet dataset (default: runtime/benchmarks/backtest/rb_ci_sample.csv)
  --dataset-root PATH Parquet dataset root for consistency checks (default: runtime/benchmarks/backtest/parquet_v2)
  --results-dir PATH  Directory for generated reports (default: docs/results)
  --config PATH       Config path for simnow_compare_cli (default: configs/sim/ctp.yaml)
  --max-ticks N       Max ticks used in checks (default: 4)
  --baseline-json P   Legacy baseline JSON path
  --provenance-json P Legacy provenance JSON path
  --abs-tol V         Absolute tolerance for floating fields (default: 1e-8)
  --rel-tol V         Relative tolerance for floating fields (default: 1e-6)
  -h, --help          Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      build_dir="$2"
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
    --results-dir)
      results_dir="$2"
      shift 2
      ;;
    --config)
      config_path="$2"
      shift 2
      ;;
    --max-ticks)
      max_ticks="$2"
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
if [[ ! -x "${build_dir}/simnow_compare_cli" ]]; then
  echo "error: missing executable: ${build_dir}/simnow_compare_cli" >&2
  exit 2
fi
if [[ ! -x "${build_dir}/csv_to_parquet_cli" ]]; then
  echo "error: missing executable: ${build_dir}/csv_to_parquet_cli" >&2
  exit 2
fi

mkdir -p "${results_dir}"
mkdir -p "$(dirname "${csv_path}")"
mkdir -p "${dataset_root}"

if [[ ! -f "${csv_path}" ]]; then
  cat >"${csv_path}" <<'CSV'
symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest
rb2405,SHFE,1704067200000000000,100.0,1,99.9,5,100.1,5,10,1000,100
rb2405,SHFE,1704067201000000000,101.0,1,100.9,5,101.1,5,11,1111,100
rb2405,SHFE,1704067202000000000,99.0,1,98.9,5,99.1,5,12,1188,100
rb2405,SHFE,1704067203000000000,102.0,1,101.9,5,102.1,5,13,1326,100
CSV
fi

if ! "${build_dir}/csv_to_parquet_cli" \
  --input_csv "${csv_path}" \
  --output_root "${dataset_root}" \
  --source rb \
  --resume true >/dev/null; then
  echo "error: failed to bootstrap parquet dataset from csv: ${csv_path}" >&2
  exit 2
fi

shadow_report_json="${results_dir}/shadow_consistency_report.json"
shadow_report_html="${results_dir}/shadow_consistency_report.html"
shadow_report_sqlite="${results_dir}/shadow_consistency.sqlite"

shadow_status="pass"
shadow_reason="within_threshold"

if ! "${build_dir}/simnow_compare_cli" \
  --config "${config_path}" \
  --dataset_root "${dataset_root}" \
  --run_id "shadow-consistency-gate" \
  --max_ticks "${max_ticks}" \
  --dry_run true \
  --strict true \
  --intents_abs_max 0 \
  --output_json "${shadow_report_json}" \
  --output_html "${shadow_report_html}" \
  --sqlite_path "${shadow_report_sqlite}" >/dev/null; then
  shadow_status="fail"
  shadow_reason="simnow_compare_cli returned non-zero"
fi

backtest_report_json="${results_dir}/backtest_consistency_report.json"
summary_json="${results_dir}/consistency_gate_summary.json"

backtest_status="pass"
backtest_reason="within_tolerance"

if ! bash scripts/build/check_backtest_baseline.sh \
  --baseline-json "${baseline_json}" \
  --provenance-json "${provenance_json}" >/dev/null; then
  backtest_status="fail"
  backtest_reason="baseline_unavailable_or_invalid"
fi

if [[ "${backtest_status}" == "pass" ]]; then
  if ! "${build_dir}/backtest_consistency_cli" \
    --engine_mode parquet \
    --dataset_root "${dataset_root}" \
    --max_ticks "${max_ticks}" \
    --run_id "backtest-consistency-gate" \
    --deterministic_fills true \
    --baseline_json "${baseline_json}" \
    --output_json "${backtest_report_json}" \
    --abs_tol "${abs_tol}" \
    --rel_tol "${rel_tol}" >/dev/null; then
    backtest_status="fail"
    backtest_reason="baseline_diff_exceeds_tolerance"
  fi
fi

if [[ ! -f "${backtest_report_json}" ]]; then
  cat >"${backtest_report_json}" <<EOF
{
  "status": "fail",
  "reason": "${backtest_reason}",
  "baseline_json": "${baseline_json}"
}
EOF
fi

overall_status="pass"
if [[ "${shadow_status}" != "pass" || "${backtest_status}" != "pass" ]]; then
  overall_status="fail"
fi

cat >"${summary_json}" <<EOF
{
  "overall_status": "${overall_status}",
  "shadow_consistency": "${shadow_status}",
  "shadow_reason": "${shadow_reason}",
  "backtest_consistency": "${backtest_status}",
  "backtest_reason": "${backtest_reason}",
  "shadow_report": "${shadow_report_json}",
  "backtest_report": "${backtest_report_json}"
}
EOF

if [[ "${overall_status}" != "pass" ]]; then
  echo "consistency gates failed: see ${summary_json}" >&2
  exit 1
fi

echo "consistency gates passed: ${summary_json}"
