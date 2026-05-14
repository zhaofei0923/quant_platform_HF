#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build-gcc}"
WAL_EXPORT_BIN="${SIMNOW_WAL_EXPORT_BIN:-${BUILD_DIR}/simnow_wal_export_cli}"
CSV_TO_PARQUET_BIN="${CSV_TO_PARQUET_BIN:-${BUILD_DIR}/csv_to_parquet_cli}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_ROOT}/runtime/trading/wal/simnow/events.wal}"
EXPORT_ROOT="${SIMNOW_EXPORT_ROOT:-${QUANT_ROOT}/runtime/trading/exports/simnow}"
RECONCILE_ROOT="${SIMNOW_RECONCILE_ROOT:-${QUANT_ROOT}/runtime/trading/reconcile/simnow}"
REPORT_ROOT="${SIMNOW_REPORT_ROOT:-${QUANT_ROOT}/runtime/trading/reports/simnow}"
MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${QUANT_ROOT}/runtime/market_data/simnow}"
TRADING_DAY="${TRADING_DAY:-}"
PROJECT_DB="${SIMNOW_EOD_PROJECT_DB:-0}"
QUERY_DB="${SIMNOW_EOD_QUERY_DB:-${PROJECT_DB}}"
STRICT_RECONCILE="${SIMNOW_STRICT_RECONCILE:-0}"
CONVERT_MARKET_PARQUET="${SIMNOW_EOD_CONVERT_MARKET_PARQUET:-0}"
DRY_RUN=0

usage() {
  cat <<USAGE
Usage: $0 --trading-day <YYYYMMDD|YYYY-MM-DD> [options]

Options:
  --trading-day <value>          Trading day to export
  --wal-file <path>              WAL file (default: ${WAL_FILE})
  --export-root <path>           CSV/JSONL export root (default: ${EXPORT_ROOT})
  --reconcile-root <path>        Reconcile report root (default: ${RECONCILE_ROOT})
  --report-root <path>           EOD report root (default: ${REPORT_ROOT})
  --project-db                   Replay WAL into trading DB using configured storage
  --query-db                     Query trading DB counts for reconcile
  --strict-reconcile             Fail on DB projection/reconcile mismatch
  --convert-market-parquet       Convert market tick CSVs with csv_to_parquet_cli when available
  --dry-run                      Print commands only
  -h, --help                     Show this help
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_value() {
  local option_name="$1"
  local option_value="${2:-}"
  [[ -n "${option_value}" ]] || die "${option_name} requires a value"
}

normalize_day() {
  printf '%s' "$1" | tr -d -- '-[:space:]'
}

is_bool_flag() {
  [[ "${1:-}" == "0" || "${1:-}" == "1" ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --trading-day) require_value "$1" "${2:-}"; TRADING_DAY="$(normalize_day "$2")"; shift 2 ;;
    --wal-file) require_value "$1" "${2:-}"; WAL_FILE="$2"; shift 2 ;;
    --export-root) require_value "$1" "${2:-}"; EXPORT_ROOT="$2"; shift 2 ;;
    --reconcile-root) require_value "$1" "${2:-}"; RECONCILE_ROOT="$2"; shift 2 ;;
    --report-root) require_value "$1" "${2:-}"; REPORT_ROOT="$2"; shift 2 ;;
    --project-db) PROJECT_DB=1; QUERY_DB=1; shift ;;
    --query-db) QUERY_DB=1; shift ;;
    --strict-reconcile) STRICT_RECONCILE=1; shift ;;
    --convert-market-parquet) CONVERT_MARKET_PARQUET=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

TRADING_DAY="$(normalize_day "${TRADING_DAY}")"
[[ -n "${TRADING_DAY}" ]] || die "--trading-day is required"
[[ "${TRADING_DAY}" =~ ^[0-9]{8}$ ]] || die "--trading-day must be YYYYMMDD or YYYY-MM-DD"
is_bool_flag "${PROJECT_DB}" || die "SIMNOW_EOD_PROJECT_DB must be 0 or 1"
is_bool_flag "${QUERY_DB}" || die "SIMNOW_EOD_QUERY_DB must be 0 or 1"
is_bool_flag "${STRICT_RECONCILE}" || die "SIMNOW_STRICT_RECONCILE must be 0 or 1"
is_bool_flag "${CONVERT_MARKET_PARQUET}" || die "SIMNOW_EOD_CONVERT_MARKET_PARQUET must be 0 or 1"

EXPORT_DIR="${EXPORT_ROOT}/${TRADING_DAY}"
RECONCILE_DIR="${RECONCILE_ROOT}/${TRADING_DAY}"
REPORT_DIR="${REPORT_ROOT}/${TRADING_DAY}"
mkdir -p "${EXPORT_DIR}" "${RECONCILE_DIR}" "${REPORT_DIR}"

wal_cmd=(
  "${WAL_EXPORT_BIN}"
  --trading-day "${TRADING_DAY}"
  --wal-file "${WAL_FILE}"
  --export-root "${EXPORT_ROOT}"
  --reconcile-root "${RECONCILE_ROOT}"
  --summary-json "${EXPORT_DIR}/summary.json"
  --summary-md "${EXPORT_DIR}/summary.md"
  --reconcile-json "${RECONCILE_DIR}/reconcile.json"
  --reconcile-md "${RECONCILE_DIR}/reconcile.md"
  --project-db "${PROJECT_DB}"
  --query-db "${QUERY_DB}"
  --strict-reconcile "${STRICT_RECONCILE}"
)

if [[ ${DRY_RUN} -eq 1 ]]; then
  printf '[dry-run] wal export:'
  printf ' %q' "${wal_cmd[@]}"
  printf '\n'
else
  [[ -x "${WAL_EXPORT_BIN}" ]] || die "simnow_wal_export_cli is not executable: ${WAL_EXPORT_BIN}"
  "${wal_cmd[@]}"
fi

if [[ "${CONVERT_MARKET_PARQUET}" == "1" ]]; then
  if [[ ! -x "${CSV_TO_PARQUET_BIN}" ]]; then
    echo "[warn] csv_to_parquet_cli is not executable: ${CSV_TO_PARQUET_BIN}" >&2
  else
    while IFS= read -r tick_csv; do
      [[ -n "${tick_csv}" ]] || continue
      rel="${tick_csv#${MARKET_DATA_DIR}/}"
      parquet_root="${EXPORT_DIR}/market_parquet/${rel%/ticks.csv}"
      parquet_cmd=(
        "${CSV_TO_PARQUET_BIN}"
        --input_csv "${tick_csv}"
        --output_root "${parquet_root}"
        --start_date "${TRADING_DAY}"
        --end_date "${TRADING_DAY}"
      )
      if [[ ${DRY_RUN} -eq 1 ]]; then
        printf '[dry-run] market parquet:'
        printf ' %q' "${parquet_cmd[@]}"
        printf '\n'
      else
        "${parquet_cmd[@]}" || echo "[warn] market parquet conversion failed: ${tick_csv}" >&2
      fi
    done < <(find "${MARKET_DATA_DIR}" -type f -path '*/market/ticks.csv' -print 2>/dev/null | sort)
  fi
fi

cat > "${REPORT_DIR}/simnow_export_manifest.env" <<EOF
trading_day=${TRADING_DAY}
wal_file=${WAL_FILE}
export_dir=${EXPORT_DIR}
reconcile_dir=${RECONCILE_DIR}
orders_csv=${EXPORT_DIR}/orders.csv
trade_fills_csv=${EXPORT_DIR}/trade_fills.csv
events_jsonl=${EXPORT_DIR}/events.jsonl
summary_json=${EXPORT_DIR}/summary.json
reconcile_json=${RECONCILE_DIR}/reconcile.json
project_db=${PROJECT_DB}
query_db=${QUERY_DB}
strict_reconcile=${STRICT_RECONCILE}
convert_market_parquet=${CONVERT_MARKET_PARQUET}
EOF

echo "[ok] SimNow trading-day export completed: ${EXPORT_DIR}"