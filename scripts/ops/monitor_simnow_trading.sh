#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

RUN_ROOT="${SIMNOW_RUN_ROOT:-${QUANT_ROOT}/runtime/simnow_trading}"
MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${QUANT_ROOT}/runtime/market_data/simnow}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_ROOT}/runtime_events.wal}"
CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/sim/ctp_sim_trade_candidates.yaml}"
TICK_STALE_SECONDS="${SIMNOW_TICK_STALE_SECONDS:-180}"
BAR_STALE_SECONDS="${SIMNOW_BAR_STALE_SECONDS:-240}"
HEALTH_GRACE_SECONDS="${SIMNOW_HEALTH_GRACE_SECONDS:-180}"
TAIL_LINES=30
WATCH_SECONDS=0
PRODUCTS="${SIMNOW_PRODUCTS:-}"

usage() {
  cat <<USAGE
Usage: $0 [options]

Print a one-shot SimNow trading health summary, or repeat it with --watch-seconds.
When tick files are fresh but exchange update time has not advanced into a new
closed bar minute, stale bar file mtimes are reported as waiting instead of unhealthy.

Options:
  --run-root <path>             SimNow run root (default: ${RUN_ROOT})
  --market-data-dir <path>      Market CSV root (default: ${MARKET_DATA_DIR})
  --wal-file <path>             WAL file path (default: ${WAL_FILE})
  --config <path>               Config used to infer product_ids (default: ${CONFIG_PATH})
  --products <csv>              Product ids to check, e.g. c,hc
  --tick-stale-seconds <int>    Tick freshness threshold (default: ${TICK_STALE_SECONDS})
  --bar-stale-seconds <int>     Bar freshness threshold (default: ${BAR_STALE_SECONDS})
  --health-grace-seconds <int>  Startup grace before missing data is unhealthy (default: ${HEALTH_GRACE_SECONDS})
  --tail-lines <int>            Recent alert log lines to show (default: ${TAIL_LINES})
  --watch-seconds <int>         Repeat every N seconds; 0 means one-shot (default: ${WATCH_SECONDS})
  -h, --help                    Show this help
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

is_non_negative_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

pid_is_alive() {
  local process_pid="${1:-}"
  [[ "${process_pid}" =~ ^[0-9]+$ ]] || return 1
  kill -0 "${process_pid}" 2>/dev/null
}

read_state_file() {
  local file_path="$1"
  [[ -f "${file_path}" ]] || return 1
  cat "${file_path}"
}

read_current_pid() {
  local pid_file="${RUN_ROOT}/current_core_engine.pid"
  [[ -f "${pid_file}" ]] || return 1
  tr -dc '0-9' < "${pid_file}"
}

find_supervisor_pids() {
  local process_pid
  local process_args_text
  while read -r process_pid process_args_text; do
    [[ -n "${process_pid:-}" ]] || continue
    [[ "${process_pid}" == "$$" || "${process_pid}" == "${BASHPID}" ]] && continue
    [[ "${process_args_text:-}" == *"supervise_simnow_trading.sh"* ]] || continue
    printf '%s\n' "${process_pid}"
  done < <(ps -eo pid=,args=)
}

process_elapsed_seconds() {
  local process_pid="$1"
  ps -p "${process_pid}" -o etimes= 2>/dev/null | tr -dc '0-9' || true
}

file_age_seconds() {
  local file_path="$1"
  local modified_epoch
  local now_epoch
  [[ -f "${file_path}" ]] || { printf '%s\n' -1; return 0; }
  modified_epoch="$(stat -c '%Y' "${file_path}" 2>/dev/null || printf '0')"
  [[ "${modified_epoch}" =~ ^[0-9]+$ && "${modified_epoch}" != "0" ]] || {
    printf '%s\n' -1
    return 0
  }
  now_epoch="$(date +%s)"
  printf '%s\n' $((now_epoch - modified_epoch))
}

latest_file_by_path() {
  local path_pattern="$1"
  find "${MARKET_DATA_DIR}" -type f -path "${path_pattern}" -printf '%T@ %p\n' 2>/dev/null |
    sort -nr | head -n 1 | sed 's/^[^ ]* //'
}

latest_file_by_name() {
  local file_name="$1"
  find "${MARKET_DATA_DIR}" -type f -name "${file_name}" -printf '%T@ %p\n' 2>/dev/null |
    sort -nr | head -n 1 | sed 's/^[^ ]* //'
}

sanitize_log() {
  sed -E 's/((investor_id|account_id|user_id)=")[^"]*(")/\1<redacted>\3/g; s/((balance|available|curr_margin|frozen_margin|close_profit|position_profit)=")[-0-9.eE+]*(")/\1<redacted>\3/g'
}

config_products() {
  if [[ -n "${PRODUCTS}" ]]; then
    printf '%s\n' "${PRODUCTS}"
    return 0
  fi
  [[ -f "${CONFIG_PATH}" ]] || return 1
  grep -E '^[[:space:]]*product_ids:' "${CONFIG_PATH}" | head -n 1 |
    sed -E "s/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/\"//g; s/'//g; s/[[:space:]]//g"
}

json_value() {
  local key="$1"
  local file_path="$2"
  grep -Eo '"'"${key}"'"[[:space:]]*:[[:space:]]*"[^"]*"' "${file_path}" 2>/dev/null |
    head -n 1 | sed -E 's/^[^:]*:[[:space:]]*"([^"]*)"/\1/'
}

dominant_instrument_id() {
  local product="$1"
  local dominant_path="${QUANT_ROOT}/runtime/ctp_instruments/${product}_dominant_contract.json"
  [[ -f "${dominant_path}" ]] || return 1
  json_value instrument_id "${dominant_path}"
}

last_tick_minute() {
  local file_path="$1"
  local instrument_id="${2:-}"
  [[ -f "${file_path}" ]] || return 1
  awk -F, -v instrument_id="${instrument_id}" '
    NR > 1 && (instrument_id == "" || $1 == instrument_id) {
      update_time = $5
      sub(/:[0-9][0-9]$/, "", update_time)
      value = $3 " " update_time
    }
    END {
      if (value != "") {
        print value
      }
    }
  ' "${file_path}"
}

last_bar_minute() {
  local file_path="$1"
  local instrument_id="${2:-}"
  [[ -f "${file_path}" ]] || return 1
  awk -F, -v instrument_id="${instrument_id}" '
    NR > 1 && (instrument_id == "" || $1 == instrument_id) {
      value = $5
    }
    END {
      if (value != "") {
        print value
      }
    }
  ' "${file_path}"
}

minute_epoch_seconds() {
  local minute_text="$1"
  local compact_day
  local clock_text
  [[ "${minute_text}" =~ ^[0-9]{8}[[:space:]][0-9]{2}:[0-9]{2}$ ]] || return 1
  compact_day="${minute_text%% *}"
  clock_text="${minute_text#* }"
  date -d "${compact_day:0:4}-${compact_day:4:2}-${compact_day:6:2} ${clock_text}" +%s \
    2>/dev/null
}

minute_delta() {
  local newer_minute="$1"
  local older_minute="$2"
  local newer_epoch
  local older_epoch
  newer_epoch="$(minute_epoch_seconds "${newer_minute}" || true)"
  older_epoch="$(minute_epoch_seconds "${older_minute}" || true)"
  [[ "${newer_epoch}" =~ ^[0-9]+$ && "${older_epoch}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' $(((newer_epoch - older_epoch) / 60))
}

print_file_freshness() {
  local label="$1"
  local file_path="$2"
  local threshold_seconds="$3"
  local elapsed_seconds="$4"
  local age_seconds

  if [[ -z "${file_path}" ]]; then
    if (( elapsed_seconds >= HEALTH_GRACE_SECONDS )); then
      echo "${label}: missing status=unhealthy"
      unhealthy_count=$((unhealthy_count + 1))
    else
      echo "${label}: missing status=grace"
    fi
    return 0
  fi

  age_seconds="$(file_age_seconds "${file_path}")"
  if (( age_seconds < 0 )); then
    echo "${label}: missing status=unhealthy"
    unhealthy_count=$((unhealthy_count + 1))
  elif (( age_seconds > threshold_seconds )); then
    echo "${label}: age=${age_seconds}s threshold=${threshold_seconds}s status=stale path=${file_path}"
    unhealthy_count=$((unhealthy_count + 1))
  else
    echo "${label}: age=${age_seconds}s threshold=${threshold_seconds}s status=fresh path=${file_path}"
  fi
}

print_bar_freshness() {
  local label="$1"
  local bar_file="$2"
  local tick_file="$3"
  local threshold_seconds="$4"
  local elapsed_seconds="$5"
  local instrument_id="${6:-}"
  local age_seconds
  local tick_age_seconds
  local tick_minute
  local bar_minute
  local delta_minutes

  if [[ -z "${bar_file}" ]]; then
    if (( elapsed_seconds >= HEALTH_GRACE_SECONDS )); then
      echo "${label}: missing status=unhealthy"
      unhealthy_count=$((unhealthy_count + 1))
    else
      echo "${label}: missing status=grace"
    fi
    return 0
  fi

  age_seconds="$(file_age_seconds "${bar_file}")"
  if (( age_seconds < 0 )); then
    echo "${label}: missing status=unhealthy"
    unhealthy_count=$((unhealthy_count + 1))
    return 0
  fi
  if (( age_seconds <= threshold_seconds )); then
    echo "${label}: age=${age_seconds}s threshold=${threshold_seconds}s status=fresh path=${bar_file}"
    return 0
  fi

  tick_age_seconds="$(file_age_seconds "${tick_file}")"
  tick_minute="$(last_tick_minute "${tick_file}" "${instrument_id}" || true)"
  bar_minute="$(last_bar_minute "${bar_file}" "${instrument_id}" || true)"
  delta_minutes="$(minute_delta "${tick_minute}" "${bar_minute}" || true)"

  if [[ "${tick_age_seconds}" =~ ^[0-9]+$ && ${tick_age_seconds} -le ${TICK_STALE_SECONDS} &&
        "${delta_minutes}" =~ ^-?[0-9]+$ && ${delta_minutes} -le 1 ]]; then
    echo "${label}: age=${age_seconds}s threshold=${threshold_seconds}s status=waiting_next_trade_minute tick_minute=${tick_minute:-unknown} bar_minute=${bar_minute:-unknown} path=${bar_file}"
    return 0
  fi

  echo "${label}: age=${age_seconds}s threshold=${threshold_seconds}s status=stale tick_minute=${tick_minute:-unknown} bar_minute=${bar_minute:-unknown} path=${bar_file}"
  unhealthy_count=$((unhealthy_count + 1))
}

print_once() {
  local checked_at
  local core_pid
  local run_dir
  local core_log
  local supervisor_pids
  local elapsed_seconds=0
  local product_csv
  local product
  local tick_file
  local bar_file
  local dominant_path
  local instrument_id
  local exchange_id
  local selection_metric
  local active_instrument_id
  local order_events=0
  local trade_events=0

  unhealthy_count=0
  checked_at="$(date -Is)"
  core_pid="$(read_current_pid || true)"
  run_dir="$(read_state_file "${RUN_ROOT}/current_run_dir" || true)"
  core_log="$(read_state_file "${RUN_ROOT}/current_core_engine_log" || true)"
  supervisor_pids="$(find_supervisor_pids | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"

  echo "[status] checked_at=${checked_at}"
  echo "[paths] run_root=${RUN_ROOT}"
  [[ -n "${run_dir}" ]] && echo "[paths] run_dir=${run_dir}"
  [[ -n "${core_log}" ]] && echo "[paths] core_log=${core_log}"
  echo "[paths] market_data_dir=${MARKET_DATA_DIR}"

  if [[ -n "${core_pid}" ]] && pid_is_alive "${core_pid}"; then
    elapsed_seconds="$(process_elapsed_seconds "${core_pid}")"
    [[ -n "${elapsed_seconds}" ]] || elapsed_seconds=0
    echo "[process] core_engine=alive pid=${core_pid} elapsed=${elapsed_seconds}s"
  elif [[ -n "${core_pid}" ]]; then
    echo "[process] core_engine=dead pid=${core_pid}"
    unhealthy_count=$((unhealthy_count + 1))
  else
    echo "[process] core_engine=missing_pid"
    unhealthy_count=$((unhealthy_count + 1))
  fi

  if [[ -n "${supervisor_pids}" ]]; then
    echo "[process] supervisor=alive pids=${supervisor_pids}"
  else
    echo "[process] supervisor=not_running"
  fi

  echo "[disk]"
  df -Pm "${RUN_ROOT}" "${MARKET_DATA_DIR}" 2>/dev/null | awk 'NR == 1 || NR > 1 {print}' || true

  product_csv="$(config_products || true)"
  echo "[contracts] products=${product_csv:-unknown}"
  if [[ -n "${product_csv}" ]]; then
    IFS=',' read -r -a product_array <<< "${product_csv}"
    for product in "${product_array[@]}"; do
      [[ -n "${product}" ]] || continue
      dominant_path="${QUANT_ROOT}/runtime/ctp_instruments/${product}_dominant_contract.json"
      if [[ -f "${dominant_path}" ]]; then
        instrument_id="$(json_value instrument_id "${dominant_path}")"
        exchange_id="$(json_value exchange_id "${dominant_path}")"
        selection_metric="$(json_value selection_metric "${dominant_path}")"
        echo "${product}: instrument=${instrument_id:-unknown} exchange=${exchange_id:-unknown} metric=${selection_metric:-unknown}"
      else
        echo "${product}: dominant_contract=missing path=${dominant_path}"
      fi
    done
  fi

  echo "[market_data]"
  if [[ -n "${product_csv}" ]]; then
    IFS=',' read -r -a product_array <<< "${product_csv}"
    for product in "${product_array[@]}"; do
      [[ -n "${product}" ]] || continue
      tick_file="$(latest_file_by_path "*/varieties/${product}/market/ticks.csv" || true)"
      bar_file="$(latest_file_by_path "*/varieties/${product}/market/bars_1m.csv" || true)"
      active_instrument_id="$(dominant_instrument_id "${product}" || true)"
      print_file_freshness "${product}.ticks" "${tick_file}" "${TICK_STALE_SECONDS}" "${elapsed_seconds}"
      print_bar_freshness "${product}.bars_1m" "${bar_file}" "${tick_file}" \
        "${BAR_STALE_SECONDS}" "${elapsed_seconds}" "${active_instrument_id}"
    done
  else
    tick_file="$(latest_file_by_name ticks.csv || true)"
    bar_file="$(latest_file_by_name bars_1m.csv || true)"
    print_file_freshness "ticks" "${tick_file}" "${TICK_STALE_SECONDS}" "${elapsed_seconds}"
    print_bar_freshness "bars_1m" "${bar_file}" "${tick_file}" "${BAR_STALE_SECONDS}" \
      "${elapsed_seconds}" ""
  fi

  echo "[wal]"
  if [[ -f "${WAL_FILE}" ]]; then
    order_events="$(grep -c 'client_order_id' "${WAL_FILE}" || true)"
    trade_events="$(grep -Ec 'filled_volume[^0-9]*[1-9]|trade_id' "${WAL_FILE}" || true)"
    echo "wal_file=${WAL_FILE} order_events=${order_events} trade_or_fill_events=${trade_events}"
  else
    echo "wal_file=${WAL_FILE} status=missing"
  fi

  echo "[recent_alerts]"
  if [[ -n "${core_log}" && -f "${core_log}" ]]; then
    grep -Ein 'level=(warn|error)|reject|timeout|disconnect|critical|client_order_id|order_(insert|event|intent)|OnRtnTrade|filled_volume|trade_id|PARTIALLY_FILLED|FILLED' "${core_log}" |
      sanitize_log | tail -n "${TAIL_LINES}" || true
  else
    echo "core log is unavailable"
  fi

  if (( unhealthy_count > 0 )); then
    echo "[result] unhealthy count=${unhealthy_count}"
    return 2
  fi
  echo "[result] healthy"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; shift 2 ;;
    --market-data-dir) require_value "$1" "${2:-}"; MARKET_DATA_DIR="$2"; shift 2 ;;
    --wal-file) require_value "$1" "${2:-}"; WAL_FILE="$2"; shift 2 ;;
    --config|--ctp-config-path) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; shift 2 ;;
    --products) require_value "$1" "${2:-}"; PRODUCTS="$2"; shift 2 ;;
    --tick-stale-seconds) require_value "$1" "${2:-}"; TICK_STALE_SECONDS="$2"; shift 2 ;;
    --bar-stale-seconds) require_value "$1" "${2:-}"; BAR_STALE_SECONDS="$2"; shift 2 ;;
    --health-grace-seconds) require_value "$1" "${2:-}"; HEALTH_GRACE_SECONDS="$2"; shift 2 ;;
    --tail-lines) require_value "$1" "${2:-}"; TAIL_LINES="$2"; shift 2 ;;
    --watch-seconds) require_value "$1" "${2:-}"; WATCH_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

is_non_negative_int "${TICK_STALE_SECONDS}" || die "--tick-stale-seconds must be a non-negative integer"
is_non_negative_int "${BAR_STALE_SECONDS}" || die "--bar-stale-seconds must be a non-negative integer"
is_non_negative_int "${HEALTH_GRACE_SECONDS}" || die "--health-grace-seconds must be a non-negative integer"
is_non_negative_int "${TAIL_LINES}" || die "--tail-lines must be a non-negative integer"
is_non_negative_int "${WATCH_SECONDS}" || die "--watch-seconds must be a non-negative integer"

cd "${QUANT_ROOT}"

if (( WATCH_SECONDS == 0 )); then
  print_once
  exit $?
fi

while true; do
  print_once || true
  sleep "${WATCH_SECONDS}"
done
