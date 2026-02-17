#!/usr/bin/env bash
set -euo pipefail

build_dir="build"
csv_path="runtime/benchmarks/backtest/rb_perf_large.csv"
source_symbol="rb"
output_root="runtime/benchmarks/backtest/parquet_v2"
results_dir="docs/results"
compare_json=""
summary_json=""
input_json=""
min_speedup="5.0"
runs="3"
warmup_runs="1"
max_ticks="120000"
require_equal="true"
require_arrow_writer="false"
symbol_count="20"
focus_symbol=""

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --build-dir PATH              Build directory containing CLIs (default: build)
  --csv-path PATH               Input CSV path (default: runtime/benchmarks/backtest/rb_perf_large.csv)
  --source SYMBOL               Source symbol prefix (default: rb)
  --output-root PATH            Parquet output root (default: runtime/benchmarks/backtest/parquet_v2)
  --results-dir PATH            Results directory (default: docs/results)
  --compare-json PATH           Compare JSON output path (default: <results-dir>/csv_parquet_speed_compare_gate.json)
  --summary-json PATH           Gate summary JSON path (default: <results-dir>/csv_parquet_speedup_gate_summary.json)
  --input-json PATH             Existing compare JSON; if provided, skip conversion/benchmark execution
  --runs N                      Compare benchmark runs (default: 3)
  --warmup-runs N               Compare warmup runs (default: 1)
  --max-ticks N                 Compare max ticks (default: 120000)
  --min-speedup FLOAT           Required parquet_vs_csv speedup (default: 5.0)
  --require-equal BOOL          Require compare JSON equal=true (default: true)
  --require-arrow-writer BOOL   Pass through to csv_to_parquet_cli --require_arrow_writer (default: false)
  --symbol-count N              Expand dataset to N instruments for partition-pruning benchmark (default: 20)
  --focus-symbol SYMBOL         Symbol used in compare filter (default: <source>2405)
  -h, --help                    Show help
USAGE
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
    --source)
      source_symbol="$2"
      shift 2
      ;;
    --output-root)
      output_root="$2"
      shift 2
      ;;
    --results-dir)
      results_dir="$2"
      shift 2
      ;;
    --compare-json)
      compare_json="$2"
      shift 2
      ;;
    --summary-json)
      summary_json="$2"
      shift 2
      ;;
    --input-json)
      input_json="$2"
      shift 2
      ;;
    --runs)
      runs="$2"
      shift 2
      ;;
    --warmup-runs)
      warmup_runs="$2"
      shift 2
      ;;
    --max-ticks)
      max_ticks="$2"
      shift 2
      ;;
    --min-speedup)
      min_speedup="$2"
      shift 2
      ;;
    --require-equal)
      require_equal="$2"
      shift 2
      ;;
    --require-arrow-writer)
      require_arrow_writer="$2"
      shift 2
      ;;
    --symbol-count)
      symbol_count="$2"
      shift 2
      ;;
    --focus-symbol)
      focus_symbol="$2"
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

mkdir -p "${results_dir}"
if [[ -z "${compare_json}" ]]; then
  compare_json="${results_dir}/csv_parquet_speed_compare_gate.json"
fi
if [[ -z "${summary_json}" ]]; then
  summary_json="${results_dir}/csv_parquet_speedup_gate_summary.json"
fi

if [[ -z "${focus_symbol}" ]]; then
  focus_symbol="${source_symbol}2405"
fi

compare_input="${input_json}"
if [[ -z "${compare_input}" ]]; then
  compare_input="${compare_json}"

  if [[ ! -x "${build_dir}/csv_to_parquet_cli" ]]; then
    echo "error: missing executable: ${build_dir}/csv_to_parquet_cli" >&2
    exit 2
  fi
  if [[ ! -x "${build_dir}/csv_parquet_compare_cli" ]]; then
    echo "error: missing executable: ${build_dir}/csv_parquet_compare_cli" >&2
    exit 2
  fi

  if [[ ! -f "${csv_path}" ]]; then
    mkdir -p "$(dirname "${csv_path}")"
    bash scripts/build/generate_backtest_benchmark_csv.sh \
      --output "${csv_path}" \
      --ticks "${max_ticks}"
  fi

  work_csv_path="${csv_path}"
  if [[ "${symbol_count}" != "1" ]]; then
    work_csv_path="${results_dir}/csv_parquet_speedup_gate_input.csv"
    python3 - "${csv_path}" "${work_csv_path}" "${source_symbol}" "${symbol_count}" <<'PY'
import csv
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
prefix = sys.argv[3].lower()
count = int(sys.argv[4])

with src.open("r", encoding="utf-8", newline="") as f:
    reader = csv.reader(f)
    rows = list(reader)

if not rows:
    raise SystemExit("empty csv input")

header = rows[0]
data_rows = rows[1:]

symbol_idx = None
for key in ("InstrumentID", "instrument_id", "symbol", "Symbol"):
    if key in header:
        symbol_idx = header.index(key)
        break
if symbol_idx is None:
    raise SystemExit("unable to locate instrument/symbol column")

dst.parent.mkdir(parents=True, exist_ok=True)
with dst.open("w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    symbols = [f"{prefix}{2405 + idx}" for idx in range(count)]
    for row in data_rows:
        for symbol in symbols:
            out_row = list(row)
            out_row[symbol_idx] = symbol
            writer.writerow(out_row)
PY
  fi

  "${build_dir}/csv_to_parquet_cli" \
    --input_csv "${work_csv_path}" \
    --output_root "${output_root}" \
    --source "${source_symbol}" \
    --resume true \
    --require_arrow_writer "${require_arrow_writer}" >/dev/null

  "${build_dir}/csv_parquet_compare_cli" \
    --csv_path "${work_csv_path}" \
    --parquet_root "${output_root}" \
    --symbols "${focus_symbol}" \
    --runs "${runs}" \
    --warmup_runs "${warmup_runs}" \
    --max_ticks "${max_ticks}" \
    --output_json "${compare_json}" >/dev/null
fi

if [[ ! -f "${compare_input}" ]]; then
  echo "error: compare json not found: ${compare_input}" >&2
  exit 2
fi

readarray -t parsed < <(python3 - "${compare_input}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, 'r', encoding='utf-8') as f:
    payload = json.load(f)

summary = payload.get('summary', {})
speedup = summary.get('parquet_vs_csv_speedup')
equal = payload.get('equal')

if speedup is None:
    raise SystemExit('missing summary.parquet_vs_csv_speedup')

print(float(speedup))
print('true' if bool(equal) else 'false')
PY
)

actual_speedup="${parsed[0]}"
actual_equal="${parsed[1]}"

speedup_ok="false"
if python3 - "${actual_speedup}" "${min_speedup}" <<'PY'
import sys
actual = float(sys.argv[1])
threshold = float(sys.argv[2])
raise SystemExit(0 if actual >= threshold else 1)
PY
then
  speedup_ok="true"
fi

equal_ok="true"
if [[ "${require_equal}" == "true" && "${actual_equal}" != "true" ]]; then
  equal_ok="false"
fi

status="pass"
reason="ok"
if [[ "${speedup_ok}" != "true" ]]; then
  status="fail"
  reason="speedup_below_threshold"
elif [[ "${equal_ok}" != "true" ]]; then
  status="fail"
  reason="consistency_not_equal"
fi

cat >"${summary_json}" <<EOF_SUMMARY
{
  "status": "${status}",
  "reason": "${reason}",
  "min_speedup": ${min_speedup},
  "actual_speedup": ${actual_speedup},
  "require_equal": ${require_equal},
  "actual_equal": ${actual_equal},
  "focus_symbol": "${focus_symbol}",
  "symbol_count": ${symbol_count},
  "compare_json": "${compare_input}"
}
EOF_SUMMARY

if [[ "${status}" != "pass" ]]; then
  echo "csv/parquet speedup gate failed: ${summary_json}" >&2
  exit 1
fi

echo "csv/parquet speedup gate passed: ${summary_json}"
