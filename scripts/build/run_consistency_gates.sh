#!/usr/bin/env bash
set -euo pipefail

build_dir="build"
csv_path="runtime/benchmarks/backtest/rb_ci_sample.csv"
results_dir="docs/results"
config_path="configs/sim/ctp.yaml"
max_ticks="4"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --build-dir PATH    Build directory containing CLIs (default: build)
  --csv-path PATH     Input CSV for consistency checks (default: runtime/benchmarks/backtest/rb_ci_sample.csv)
  --results-dir PATH  Directory for generated reports (default: docs/results)
  --config PATH       Config path for simnow_compare_cli (default: configs/sim/ctp.yaml)
  --max-ticks N       Max ticks used in checks (default: 4)
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

if [[ ! -x "${build_dir}/backtest_cli" ]]; then
  echo "error: missing executable: ${build_dir}/backtest_cli" >&2
  exit 2
fi
if [[ ! -x "${build_dir}/simnow_compare_cli" ]]; then
  echo "error: missing executable: ${build_dir}/simnow_compare_cli" >&2
  exit 2
fi

mkdir -p "${results_dir}"
mkdir -p "$(dirname "${csv_path}")"

if [[ ! -f "${csv_path}" ]]; then
  cat >"${csv_path}" <<'CSV'
symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest
rb2405,SHFE,1704067200000000000,100.0,1,99.9,5,100.1,5,10,1000,100
rb2405,SHFE,1704067201000000000,101.0,1,100.9,5,101.1,5,11,1111,100
rb2405,SHFE,1704067202000000000,99.0,1,98.9,5,99.1,5,12,1188,100
rb2405,SHFE,1704067203000000000,102.0,1,101.9,5,102.1,5,13,1326,100
CSV
fi

shadow_report_json="${results_dir}/shadow_consistency_report.json"
shadow_report_html="${results_dir}/shadow_consistency_report.html"
shadow_report_sqlite="${results_dir}/shadow_consistency.sqlite"

shadow_status="pass"
shadow_reason="within_threshold"

if ! "${build_dir}/simnow_compare_cli" \
  --config "${config_path}" \
  --csv_path "${csv_path}" \
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

backtest_json_a="${results_dir}/backtest_consistency_a.json"
backtest_json_b="${results_dir}/backtest_consistency_b.json"
backtest_md_a="${results_dir}/backtest_consistency_a.md"
backtest_md_b="${results_dir}/backtest_consistency_b.md"
backtest_report_json="${results_dir}/backtest_consistency_report.json"
summary_json="${results_dir}/consistency_gate_summary.json"

backtest_status="pass"
backtest_reason="outputs_identical"

"${build_dir}/backtest_cli" \
  --engine_mode csv \
  --csv_path "${csv_path}" \
  --max_ticks "${max_ticks}" \
  --run_id "backtest-consistency-gate" \
  --output_json "${backtest_json_a}" \
  --output_md "${backtest_md_a}" >/dev/null

"${build_dir}/backtest_cli" \
  --engine_mode csv \
  --csv_path "${csv_path}" \
  --max_ticks "${max_ticks}" \
  --run_id "backtest-consistency-gate" \
  --output_json "${backtest_json_b}" \
  --output_md "${backtest_md_b}" >/dev/null

if ! cmp -s "${backtest_json_a}" "${backtest_json_b}"; then
  backtest_status="fail"
  backtest_reason="dual_run_output_differs"
fi

sha_a="$(sha256sum "${backtest_json_a}" | awk '{print $1}')"
sha_b="$(sha256sum "${backtest_json_b}" | awk '{print $1}')"

cat >"${backtest_report_json}" <<EOF
{
  "status": "${backtest_status}",
  "reason": "${backtest_reason}",
  "csv_path": "${csv_path}",
  "run_id": "backtest-consistency-gate",
  "files_equal": $([[ "${backtest_status}" == "pass" ]] && echo "true" || echo "false"),
  "sha256_a": "${sha_a}",
  "sha256_b": "${sha_b}",
  "json_a": "${backtest_json_a}",
  "json_b": "${backtest_json_b}"
}
EOF

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
