#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT
# shellcheck source=runtime_path_defaults.sh
source "${SCRIPT_DIR}/runtime_path_defaults.sh"

ENV_FILE="${ENV_FILE:-${QUANT_ROOT}/runtime/simnow.env}"
CONFIG_PATH="${QUANT_ROOT}/configs/sim/ctp_sim_trade_candidates.yaml"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build-gcc}"
START_SCRIPT="${SIMNOW_START_SCRIPT:-${SCRIPT_DIR}/start_simnow_trading.sh}"
DAILY_SETTLEMENT_SCRIPT="${SIMNOW_DAILY_SETTLEMENT_SCRIPT:-${SCRIPT_DIR}/run_daily_settlement.sh}"
OPS_HEALTH_BIN="${OPS_HEALTH_BIN:-${BUILD_DIR}/ops_health_report_cli}"
OPS_ALERT_BIN="${OPS_ALERT_BIN:-${BUILD_DIR}/ops_alert_report_cli}"
POSITION_SNAPSHOT_BIN="${SIMNOW_POSITION_SNAPSHOT_BIN:-${BUILD_DIR}/simnow_flatten_positions}"
EXPORT_SCRIPT="${SIMNOW_EXPORT_SCRIPT:-${SCRIPT_DIR}/export_simnow_trading_day.sh}"
SIGNAL_MONITOR_SCRIPT="${SIMNOW_SIGNAL_MONITOR_SCRIPT:-${SCRIPT_DIR}/monitor_simnow_signal_execution.sh}"
RUN_ROOT="${SIMNOW_RUN_ROOT:-}"
MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${QUANT_HFT_MARKET_DATA_DIR:-}}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_HFT_WAL_FILE:-}}"
EXPORT_ROOT="${SIMNOW_EXPORT_ROOT:-}"
RECONCILE_ROOT="${SIMNOW_RECONCILE_ROOT:-}"
REPORT_ROOT="${SIMNOW_REPORT_ROOT:-}"
TRADING_WINDOWS="${SIMNOW_TRADING_WINDOWS:-night=21:00-23:05,day_am=09:00-11:35,day_pm=13:30-15:20}"
PREWARM_WINDOWS="${SIMNOW_PREWARM_WINDOWS:-night=20:45-21:00,day_am=08:45-09:00,day_pm=13:25-13:30}"
SESSION_CALENDAR_FILE="${SIMNOW_SESSION_CALENDAR_FILE:-}"
PRODUCT_SCOPE="${SIMNOW_PRODUCT_SCOPE:-}"
EOD_TIME="${SIMNOW_EOD_TIME:-15:25}"
EOD_EXECUTE="${SIMNOW_EOD_EXECUTE:-1}"
EOD_RETRY_INTERVAL_SECONDS="${SIMNOW_EOD_RETRY_INTERVAL_SECONDS:-300}"
EPOCH_FIRST_TRADING_DAY="${SIMNOW_EPOCH_FIRST_TRADING_DAY:-}"
EOD_PROJECT_DB="${SIMNOW_EOD_PROJECT_DB:-0}"
EOD_QUERY_DB="${SIMNOW_EOD_QUERY_DB:-1}"
STRICT_RECONCILE="${SIMNOW_STRICT_RECONCILE:-1}"
CONVERT_MARKET_PARQUET="${SIMNOW_EOD_CONVERT_MARKET_PARQUET:-0}"
CHECK_INTERVAL_SECONDS="${SIMNOW_CHECK_INTERVAL_SECONDS:-30}"
RESTART_DELAY_SECONDS="${SIMNOW_RESTART_DELAY_SECONDS:-15}"
MAX_RESTARTS_PER_WINDOW="${SIMNOW_MAX_RESTARTS_PER_WINDOW:-5}"
STOP_TIMEOUT_SECONDS="${SIMNOW_STOP_TIMEOUT_SECONDS:-30}"
MIN_FREE_MB="${SIMNOW_MIN_FREE_MB:-2048}"
LOG_MAX_BYTES="${SIMNOW_LOG_MAX_BYTES:-104857600}"
LOG_RETENTION_DAYS="${SIMNOW_LOG_RETENTION_DAYS:-14}"
HEALTH_GRACE_SECONDS="${SIMNOW_HEALTH_GRACE_SECONDS:-180}"
READINESS_STALE_SECONDS="${SIMNOW_READINESS_STALE_SECONDS:-30}"
TICK_STALE_SECONDS="${SIMNOW_TICK_STALE_SECONDS:-180}"
BAR_STALE_SECONDS="${SIMNOW_BAR_STALE_SECONDS:-240}"
FILL_STALE_SECONDS="${SIMNOW_FILL_STALE_SECONDS:-900}"
REQUIRE_FILL_HEARTBEAT="${SIMNOW_REQUIRE_FILL_HEARTBEAT:-0}"
ALERT_THROTTLE_SECONDS="${SIMNOW_ALERT_THROTTLE_SECONDS:-600}"
PROBE_SECONDS="${SIMNOW_PROBE_SECONDS:-5}"
PROBE_TIMEOUT_SECONDS="${SIMNOW_PROBE_TIMEOUT_SECONDS:-120}"
HEALTH_INTERVAL_MS="${SIMNOW_HEALTH_INTERVAL_MS:-1000}"
INSTRUMENT_TIMEOUT_SECONDS="${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-45}"
ALLOW_UNCONFIRMED_SETTLEMENT="${SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT:-0}"
SIGNAL_MONITOR_ENABLED="${SIMNOW_SIGNAL_EXECUTION_MONITOR:-0}"
SIGNAL_MONITOR_EXTERNAL="${SIMNOW_SIGNAL_MONITOR_EXTERNAL:-0}"
SIGNAL_MONITOR_POLL_SECONDS="${SIMNOW_SIGNAL_MONITOR_POLL_SECONDS:-5}"
SIGNAL_MONITOR_ROOT="${SIMNOW_SIGNAL_MONITOR_ROOT:-}"
SIGNAL_MONITOR_HEARTBEAT_FILE="${SIMNOW_SIGNAL_MONITOR_HEARTBEAT_FILE:-${SIGNAL_MONITOR_ROOT}/heartbeat.json}"
SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS="${SIMNOW_SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS:-30}"
RUN_ID_PREFIX="${SIMNOW_RUN_ID_PREFIX:-simnow-auto}"
RUN_ONCE=0
DRY_RUN=0
NO_EOD=0
NO_STOP_OUTSIDE=0
WINDOWS_SET_BY_CLI=0
PREWARM_WINDOWS_SET_BY_CLI=0
SESSION_CALENDAR_FILE_SET_BY_CLI=0
PRODUCT_SCOPE_SET_BY_CLI=0
RUN_ROOT_SET_BY_CLI=0
MARKET_DATA_DIR_SET_BY_CLI=0
WAL_FILE_SET_BY_CLI=0
REPORT_ROOT_SET_BY_CLI=0
EXPORT_ROOT_SET_BY_CLI=0
RECONCILE_ROOT_SET_BY_CLI=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Run the unattended SimNow trading supervisor. It starts core_engine inside trading
windows, stops it outside trading windows, restarts crashes, monitors tick/bar/fill
freshness, rotates logs, and chains end-of-day settlement/report steps.

Options:
  --env-file <path>              Environment file to source (default: ${ENV_FILE})
  --config <path>                CTP yaml config (default: ${CONFIG_PATH})
  --build-dir <path>             Build directory (default: ${BUILD_DIR})
  --run-root <path>              Run output root (default: ${RUN_ROOT})
  --market-data-dir <path>       Market CSV root (default: ${MARKET_DATA_DIR})
  --wal-file <path>              WAL path (default: ${WAL_FILE})
  --report-root <path>           EOD report root (default: ${REPORT_ROOT})
  --export-root <path>           EOD export root (default: ${EXPORT_ROOT})
  --reconcile-root <path>        EOD reconcile root (default: ${RECONCILE_ROOT})
  --windows <spec>               Engine-active windows (default: ${TRADING_WINDOWS})
  --prewarm-windows <spec>       Core CloseOnly prewarm windows (default: ${PREWARM_WINDOWS})
  --session-calendar-file <path> Required CSV session calendar for every new session
  --product-scope <spec>         Configured product/exchange list, e.g. hc:SHFE,c:DCE
  --trading-days-file <path>     Deprecated alias for --session-calendar-file
  --eod-time <HH:MM>             End-of-day chain trigger time (default: ${EOD_TIME})
  --check-interval-seconds <int> Supervisor loop interval (default: ${CHECK_INTERVAL_SECONDS})
  --max-restarts <int>           Max crash restarts per session window (default: ${MAX_RESTARTS_PER_WINDOW})
  --min-free-mb <int>            Required free disk space (default: ${MIN_FREE_MB})
  --tick-stale-seconds <int>     Alert when latest ticks.csv is stale (default: ${TICK_STALE_SECONDS})
  --bar-stale-seconds <int>      Alert when latest bars_1m.csv is stale (default: ${BAR_STALE_SECONDS})
  --fill-stale-seconds <int>     Alert when order activity has no fill for N seconds (default: ${FILL_STALE_SECONDS})
  --no-eod                       Do not run settlement/report chain
  --no-stop-outside              Keep core_engine alive outside configured windows
  --once                         Evaluate once, then exit
  --dry-run                      Print current decision and commands, then exit
  -h, --help                     Show this help

Window spec examples:
  --prewarm-windows night=20:45-21:00,day_am=08:45-09:00,day_pm=13:25-13:30
  --windows night=21:00-23:05,day_am=09:00-11:35,day_pm=13:30-15:20

Session calendar format (exact header; dates may be YYYYMMDD or YYYY-MM-DD):
  natural_date,session,trading_day,exchange,product
  # product_scope=hc:SHFE,c:DCE
  # session_scope.night=hc:SHFE,c:DCE
  2026-09-07,day_am,2026-09-07,SHFE,hc
  2026-09-07,day_am,2026-09-07,DCE,c

The optional session_scope.<session> metadata narrows a session to products which actually
trade then (for example, omit a no-night GFEX product from session_scope.night).  When absent,
the legacy behavior remains: every product_scope member is required for that session.

Alert hooks are inherited from start_simnow_trading.sh:
  SIMNOW_ALERT_WEBHOOK_URL, SIMNOW_ALERT_EMAIL_TO, SIMNOW_ALERT_COMMAND

Optional full-pipeline health monitor (legacy environment names remain compatible):
  SIMNOW_SIGNAL_EXECUTION_MONITOR=1 starts monitor_simnow_signal_execution.sh
  alongside the supervisor and atomically publishes pipeline_health.json plus incidents
  under runtime/trading/monitor/simnow.
  SIMNOW_SIGNAL_MONITOR_EXTERNAL=1 expects the independent systemd monitor unit
  and makes the supervisor verify its heartbeat instead of spawning a child.
USAGE
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

send_alert() {
  local severity="${1:-info}"
  local message="${2:-}"
  local payload
  local escaped_message

  [[ -n "${message}" ]] || return 0
  echo "[alert:${severity}] ${message}" >&2

  if [[ -n "${SIMNOW_ALERT_WEBHOOK_URL:-}" ]] && command -v curl >/dev/null 2>&1; then
    escaped_message="$(json_escape "[${severity}] ${message}")"
    payload="{\"msgtype\":\"text\",\"text\":{\"content\":\"${escaped_message}\"}}"
    curl -fsS -m 10 -H 'Content-Type: application/json' \
      -d "${payload}" "${SIMNOW_ALERT_WEBHOOK_URL}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_EMAIL_TO:-}" ]] && command -v mail >/dev/null 2>&1; then
    printf '%s\n' "${message}" | mail -s "[quant-hft][${severity}] SimNow supervisor" \
      "${SIMNOW_ALERT_EMAIL_TO}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_COMMAND:-}" ]]; then
    ALERT_SEVERITY="${severity}" ALERT_MESSAGE="${message}" \
      bash -lc "${SIMNOW_ALERT_COMMAND}" >/dev/null 2>&1 || true
  fi
}

send_alert_once() {
  local alert_key="$1"
  local severity="$2"
  local message="$3"
  local now_epoch
  local last_epoch=0
  local state_file

  mkdir -p "${ALERT_STATE_DIR}"
  state_file="${ALERT_STATE_DIR}/$(printf '%s' "${alert_key}" | tr -c 'A-Za-z0-9_.-' '_').last"
  now_epoch="$(date +%s)"
  if [[ -f "${state_file}" ]]; then
    last_epoch="$(tr -dc '0-9' < "${state_file}" || true)"
    [[ -n "${last_epoch}" ]] || last_epoch=0
  fi
  if (( now_epoch - last_epoch >= ALERT_THROTTLE_SECONDS )); then
    printf '%s\n' "${now_epoch}" > "${state_file}"
    send_alert "${severity}" "${message}"
  fi
}

die() {
  echo "error: $*" >&2
  send_alert "error" "$*"
  exit 1
}

require_value() {
  local option_name="$1"
  local option_value="${2:-}"
  [[ -n "${option_value}" ]] || die "${option_name} requires a value"
}

is_positive_int() {
  [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

is_non_negative_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

is_bool_flag() {
  [[ "${1:-}" == "0" || "${1:-}" == "1" ]]
}

is_true_text() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  [[ "${value}" == "1" || "${value}" == "true" || "${value}" == "yes" || "${value}" == "on" ]]
}

yaml_bool_value() {
  local key="$1"
  local file_path="$2"
  awk -F: -v key="${key}" '
    $1 ~ "^[[:space:]]*" key "[[:space:]]*$" {
      value = $2
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/[[:space:]\"'"'"']/, "", value)
      print tolower(value)
      exit
    }
  ' "${file_path}" 2>/dev/null
}

time_to_minutes() {
  local time_text="$1"
  local hour_text="${time_text%:*}"
  local minute_text="${time_text#*:}"
  local hour
  local minute
  [[ "${hour_text}" =~ ^[0-9][0-9]?$ && "${minute_text}" =~ ^[0-9][0-9]$ ]] || return 1
  hour=$((10#${hour_text}))
  minute=$((10#${minute_text}))
  (( hour <= 23 && minute <= 59 )) || return 1
  printf '%d\n' $((hour * 60 + minute))
}

pid_is_alive() {
  local process_pid="${1:-}"
  [[ "${process_pid}" =~ ^[0-9]+$ ]] || return 1
  kill -0 "${process_pid}" 2>/dev/null
}

current_pid() {
  [[ -f "${CURRENT_PID_FILE}" ]] || return 1
  tr -dc '0-9' < "${CURRENT_PID_FILE}"
}

remove_stale_current_pid() {
  local process_pid

  process_pid="$(current_pid || true)"
  [[ -n "${process_pid}" ]] || return 0
  if pid_is_alive "${process_pid}"; then
    return 0
  fi

  rm -f "${CURRENT_PID_FILE}"
  echo "[info] removed stale core_engine pid file pid=${process_pid}" | \
    tee -a "${SUPERVISOR_LOG}"
}

current_engine_log() {
  if [[ -f "${CURRENT_LOG_FILE}" ]]; then
    cat "${CURRENT_LOG_FILE}"
    return 0
  fi
  if [[ -f "${CURRENT_RUN_FILE}" ]]; then
    printf '%s/core_engine.log\n' "$(cat "${CURRENT_RUN_FILE}")"
    return 0
  fi
  return 1
}

check_free_disk() {
  local path="$1"
  local min_free_mb="$2"
  local free_mb

  # Inspect the nearest existing parent; never create unbound recovery artifacts.
  local existing_path="${path}"
  while [[ ! -d "${existing_path}" ]]; do
    existing_path="$(dirname "${existing_path}")"
  done
  free_mb="$(df -Pm "${existing_path}" | awk 'NR == 2 {print $4}')"
  [[ "${free_mb}" =~ ^[0-9]+$ ]] || die "unable to determine free disk space for ${path}"
  if (( free_mb < min_free_mb )); then
    send_alert_once "disk.${path}" "critical" \
      "free disk space under ${path} is ${free_mb}MB, below required ${min_free_mb}MB"
    return 1
  fi
}

copytruncate_if_needed() {
  local file_path="$1"
  local max_bytes="$2"
  local file_size
  local rotated_path

  [[ -f "${file_path}" ]] || return 0
  file_size="$(stat -c '%s' "${file_path}")"
  [[ "${file_size}" =~ ^[0-9]+$ ]] || return 0
  if (( file_size <= max_bytes )); then
    return 0
  fi

  rotated_path="${file_path}.$(date +%Y%m%dT%H%M%S)"
  cp "${file_path}" "${rotated_path}"
  : > "${file_path}"
  if command -v gzip >/dev/null 2>&1; then
    gzip -f "${rotated_path}" || true
  fi
  echo "[info] copytruncate rotated log: ${file_path}"
}

rotate_logs() {
  [[ -d "${RUN_ROOT}" ]] || return 0
  while IFS= read -r -d '' log_file; do
    copytruncate_if_needed "${log_file}" "${LOG_MAX_BYTES}"
  done < <(find "${RUN_ROOT}" -type f -name '*.log' -print0 2>/dev/null)
  find "${RUN_ROOT}" -type f \( -name '*.log.*' -o -name '*.log.*.gz' \) \
    -mtime "+${LOG_RETENTION_DAYS}" -delete 2>/dev/null || true
}

now_date() {
  if [[ -n "${SIMNOW_FAKE_NOW:-}" ]]; then
    date -d "${SIMNOW_FAKE_NOW}" "$@"
  else
    date "$@"
  fi
}

trim_calendar_field() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "${value}"
}

normalize_calendar_date() {
  local raw_date="$1"
  local compact_date
  local dashed_date
  local normalized_date

  if [[ "${raw_date}" =~ ^[0-9]{8}$ ]]; then
    compact_date="${raw_date}"
  elif [[ "${raw_date}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    compact_date="${raw_date//-/}"
  else
    return 1
  fi
  dashed_date="${compact_date:0:4}-${compact_date:4:2}-${compact_date:6:2}"
  normalized_date="$(date -d "${dashed_date}" +%Y%m%d 2>/dev/null)" || return 1
  [[ "${normalized_date}" == "${compact_date}" ]] || return 1
  printf '%s\n' "${compact_date}"
}

normalize_product_scope() {
  local raw_scope="$1"
  local member=""
  local product=""
  local exchange=""
  local normalized=""
  local -A seen=()

  IFS=',' read -r -a members <<< "${raw_scope}"
  for member in "${members[@]}"; do
    member="${member//[[:space:]]/}"
    [[ "${member}" =~ ^([A-Za-z][A-Za-z0-9]*):([A-Z][A-Z0-9]*)$ ]] || return 1
    product="${BASH_REMATCH[1]}"
    exchange="${BASH_REMATCH[2]}"
    [[ -z "${seen[${product}:${exchange}]+x}" ]] || return 1
    seen["${product}:${exchange}"]=1
    normalized="${normalized:+${normalized},}${product}:${exchange}"
  done
  [[ -n "${normalized}" ]] || return 1
  printf '%s\n' "${normalized}"
}

scope_contains() {
  local raw_scope="$1"
  local product="$2"
  local exchange="$3"
  local member=""
  local configured_product=""
  local configured_exchange=""

  IFS=',' read -r -a members <<< "${raw_scope}"
  for member in "${members[@]}"; do
    IFS=':' read -r configured_product configured_exchange <<< "${member}"
    if [[ "${product}" == "${configured_product}" && \
          "${exchange}" == "${configured_exchange}" ]]; then
      return 0
    fi
  done
  return 1
}

product_scope_contains() {
  scope_contains "${PRODUCT_SCOPE}" "$1" "$2"
}

session_calendar_trading_day() {
  local target_natural_date="$1"
  local target_session="$2"
  local header=""
  local row=""
  local natural_date_raw=""
  local session=""
  local trading_day_raw=""
  local exchange=""
  local product=""
  local natural_date=""
  local trading_day=""
  local row_key=""
  local target_trading_day=""
  local line_number=1
  local target_rows=0
  local configured_member=""
  local configured_product=""
  local configured_exchange=""
  local metadata_scope=""
  local metadata_scope_rows=0
  local metadata_session_label=""
  local metadata_session_value=""
  local normalized_session_scope=""
  local effective_row_scope=""
  local required_target_scope=""
  local scoped_member=""
  local scoped_product=""
  local scoped_exchange=""
  local data_rows_started=0
  local -A seen_rows=()
  local -A target_scope_rows=()
  local -A metadata_session_scopes=()
  local -A metadata_session_scope_seen=()

  if [[ -z "${SESSION_CALENDAR_FILE}" ]]; then
    printf '%s\n' "calendar_file_missing"
    return 2
  fi
  if [[ ! -f "${SESSION_CALENDAR_FILE}" || ! -r "${SESSION_CALENDAR_FILE}" ]]; then
    printf '%s\n' "calendar_file_unreadable:${SESSION_CALENDAR_FILE}"
    return 2
  fi
  if ! IFS= read -r header < "${SESSION_CALENDAR_FILE}"; then
    printf '%s\n' "calendar_file_empty:${SESSION_CALENDAR_FILE}"
    return 2
  fi
  header="${header%$'\r'}"
  header="${header#$'\xef\xbb\xbf'}"
  if [[ "${header}" != "natural_date,session,trading_day,exchange,product" ]]; then
    printf '%s\n' "calendar_header_invalid"
    return 2
  fi

  while IFS= read -r row || [[ -n "${row}" ]]; do
    line_number=$((line_number + 1))
    row="${row%$'\r'}"
    if [[ "${row}" == "# product_scope="* ]]; then
      metadata_scope_rows=$((metadata_scope_rows + 1))
      if (( metadata_scope_rows > 1 )); then
        printf '%s\n' "calendar_metadata_duplicate:line=${line_number}"
        return 2
      fi
      metadata_scope="${row#\# product_scope=}"
      continue
    fi
    if [[ "${row}" == "# session_scope."* ]]; then
      if (( data_rows_started != 0 )); then
        printf '%s\n' "calendar_session_metadata_after_data:line=${line_number}"
        return 2
      fi
      metadata_session_label="${row#\# session_scope.}"
      metadata_session_value="${metadata_session_label#*=}"
      metadata_session_label="${metadata_session_label%%=*}"
      if [[ "${metadata_session_label}" != "day_am" && \
            "${metadata_session_label}" != "day_pm" && \
            "${metadata_session_label}" != "night" ]]; then
        printf '%s\n' "calendar_session_metadata_invalid:line=${line_number}"
        return 2
      fi
      if [[ -n "${metadata_session_scope_seen[${metadata_session_label}]+x}" ]]; then
        printf '%s\n' "calendar_session_metadata_duplicate:line=${line_number}"
        return 2
      fi
      normalized_session_scope=""
      if [[ -n "${metadata_session_value}" ]]; then
        normalized_session_scope="$(normalize_product_scope "${metadata_session_value}" 2>/dev/null || true)"
        if [[ -z "${normalized_session_scope}" ]]; then
          printf '%s\n' "calendar_session_metadata_scope_invalid:line=${line_number}"
          return 2
        fi
        IFS=',' read -r -a scoped_members <<< "${normalized_session_scope}"
        for scoped_member in "${scoped_members[@]}"; do
          IFS=':' read -r scoped_product scoped_exchange <<< "${scoped_member}"
          if ! product_scope_contains "${scoped_product}" "${scoped_exchange}"; then
            printf '%s\n' "calendar_session_metadata_scope_mismatch:line=${line_number}"
            return 2
          fi
        done
      fi
      metadata_session_scope_seen["${metadata_session_label}"]=1
      metadata_session_scopes["${metadata_session_label}"]="${normalized_session_scope}"
      continue
    fi
    if [[ -z "${row}" || "${row}" == \#* ]]; then
      printf '%s\n' "calendar_row_invalid:line=${line_number}"
      return 2
    fi
    data_rows_started=1
    if [[ ! "${row}" =~ ^[^,]*,[^,]*,[^,]*,[^,]*,[^,]*$ ]]; then
      printf '%s\n' "calendar_column_count_invalid:line=${line_number}"
      return 2
    fi

    IFS=',' read -r natural_date_raw session trading_day_raw exchange product <<< "${row}"
    natural_date_raw="$(trim_calendar_field "${natural_date_raw}")"
    session="$(trim_calendar_field "${session}")"
    trading_day_raw="$(trim_calendar_field "${trading_day_raw}")"
    exchange="$(trim_calendar_field "${exchange}")"
    product="$(trim_calendar_field "${product}")"
    natural_date="$(normalize_calendar_date "${natural_date_raw}" || true)"
    trading_day="$(normalize_calendar_date "${trading_day_raw}" || true)"
    if [[ -z "${natural_date}" || -z "${trading_day}" ]]; then
      printf '%s\n' "calendar_date_invalid:line=${line_number}"
      return 2
    fi
    if [[ "${session}" != "day_am" && "${session}" != "day_pm" && \
          "${session}" != "night" ]]; then
      printf '%s\n' "calendar_session_invalid:line=${line_number}"
      return 2
    fi
    if [[ ! "${exchange}" =~ ^[A-Z][A-Z0-9]*$ || \
          ! "${product}" =~ ^[A-Za-z][A-Za-z0-9]*$ ]]; then
      printf '%s\n' "calendar_member_invalid:line=${line_number}"
      return 2
    fi
    effective_row_scope="${PRODUCT_SCOPE}"
    if [[ -n "${metadata_session_scope_seen[${session}]+x}" ]]; then
      effective_row_scope="${metadata_session_scopes[${session}]}"
    fi
    if ! scope_contains "${effective_row_scope}" "${product}" "${exchange}"; then
      printf '%s\n' "calendar_product_scope_mismatch:line=${line_number}"
      return 2
    fi
    if [[ "${session}" == "night" ]]; then
      if (( 10#${trading_day} <= 10#${natural_date} )); then
        printf '%s\n' "calendar_night_trading_day_invalid:line=${line_number}"
        return 2
      fi
    elif [[ "${trading_day}" != "${natural_date}" ]]; then
      printf '%s\n' "calendar_day_trading_day_invalid:line=${line_number}"
      return 2
    fi

    row_key="${natural_date}|${session}|${exchange}|${product}"
    if [[ -n "${seen_rows[${row_key}]+x}" ]]; then
      printf '%s\n' "calendar_row_duplicate:line=${line_number}"
      return 2
    fi
    seen_rows["${row_key}"]="${trading_day}"

    if [[ "${natural_date}" == "${target_natural_date}" && \
          "${session}" == "${target_session}" ]]; then
      target_rows=$((target_rows + 1))
      target_scope_rows["${product}:${exchange}"]=$((
        ${target_scope_rows["${product}:${exchange}"]:-0} + 1))
      if [[ -z "${target_trading_day}" ]]; then
        target_trading_day="${trading_day}"
      elif [[ "${target_trading_day}" != "${trading_day}" ]]; then
        printf '%s\n' "calendar_group_trading_day_mismatch"
        return 2
      fi
    fi
  done < <(tail -n +2 "${SESSION_CALENDAR_FILE}")

  if (( metadata_scope_rows != 1 )) || \
     [[ "$(normalize_product_scope "${metadata_scope}" 2>/dev/null || true)" != "${PRODUCT_SCOPE}" ]]; then
    printf '%s\n' "calendar_product_scope_metadata_mismatch"
    return 2
  fi
  required_target_scope="${PRODUCT_SCOPE}"
  if [[ -n "${metadata_session_scope_seen[${target_session}]+x}" ]]; then
    required_target_scope="${metadata_session_scopes[${target_session}]}"
  fi
  if [[ -z "${required_target_scope}" ]]; then
    printf '%s\n' "calendar_session_not_applicable"
    return 2
  fi
  if (( target_rows == 0 )); then
    printf '%s\n' "calendar_session_out_of_coverage"
    return 2
  fi
  IFS=',' read -r -a configured_members <<< "${required_target_scope}"
  for configured_member in "${configured_members[@]}"; do
    IFS=':' read -r configured_product configured_exchange <<< "${configured_member}"
    if [[ "${target_scope_rows[${configured_product}:${configured_exchange}]:-0}" != "1" ]]; then
      printf '%s\n' "calendar_group_incomplete:required=${configured_product}:${configured_exchange}"
      return 2
    fi
  done
  printf '%s\n' "${target_trading_day}"
}

current_window_info() {
  local phase="$1"
  local windows="$2"
  local now_minutes
  local today_dash
  local today_compact
  local previous_compact
  local window_index=0
  local window_specs
  local spec
  local label
  local range
  local start_text
  local end_text
  local start_minutes
  local end_minutes
  local natural_date

  now_minutes="$(time_to_minutes "$(now_date +%H:%M)")"
  today_dash="$(now_date +%F)"
  today_compact="$(now_date +%Y%m%d)"
  previous_compact="$(date -d "${today_dash} -1 day" +%Y%m%d)"

  IFS=',' read -r -a window_specs <<< "${windows}"
  for spec in "${window_specs[@]}"; do
    spec="${spec//[[:space:]]/}"
    [[ -n "${spec}" ]] || continue
    if [[ "${spec}" == *=* ]]; then
      label="${spec%%=*}"
      range="${spec#*=}"
    else
      label="window_${window_index}"
      range="${spec}"
    fi
    start_text="${range%-*}"
    end_text="${range#*-}"
    start_minutes="$(time_to_minutes "${start_text}")" || die "invalid trading window start time: ${range}"
    end_minutes="$(time_to_minutes "${end_text}")" || die "invalid trading window end time: ${range}"

    natural_date=""
    if (( start_minutes <= end_minutes )); then
      if (( now_minutes >= start_minutes && now_minutes < end_minutes )); then
        natural_date="${today_compact}"
      fi
    else
      if (( now_minutes >= start_minutes )); then
        natural_date="${today_compact}"
      elif (( now_minutes < end_minutes )); then
        natural_date="${previous_compact}"
      fi
    fi

    if [[ -n "${natural_date}" ]]; then
      printf '%s|%s|%s|%s\n' "${phase}" "${label}" "${natural_date}" "${range}"
      return 0
    fi
    window_index=$((window_index + 1))
  done
  return 1
}

current_schedule_info() {
  current_window_info "active" "${TRADING_WINDOWS}" && return 0
  current_window_info "prewarm" "${PREWARM_WINDOWS}" && return 0
  return 1
}

today_eod_due() {
  local attempt_epoch
  local attempt_file
  local now_epoch
  local now_minutes
  local eod_minutes
  local today_compact
  local mapped_trading_day
  local marker_file
  local terminal_file

  [[ ${NO_EOD} -eq 0 ]] || return 1
  today_compact="$(now_date +%Y%m%d)"
  if [[ -n "${EPOCH_FIRST_TRADING_DAY}" && "${today_compact}" < "${EPOCH_FIRST_TRADING_DAY}" ]]; then
    return 1
  fi
  mapped_trading_day="$(session_calendar_trading_day "${today_compact}" day_pm || true)"
  [[ "${mapped_trading_day}" == "${today_compact}" ]] || return 1
  now_minutes="$(time_to_minutes "$(now_date +%H:%M)")"
  eod_minutes="$(time_to_minutes "${EOD_TIME}")" || die "invalid --eod-time: ${EOD_TIME}"
  (( now_minutes >= eod_minutes )) || return 1
  marker_file="${EOD_STATE_DIR}/${today_compact}.done"
  terminal_file="${EOD_STATE_DIR}/${today_compact}.terminal"
  [[ ! -f "${marker_file}" && ! -f "${terminal_file}" ]] || return 1
  attempt_file="${EOD_STATE_DIR}/${today_compact}.last_attempt_epoch"
  if [[ -f "${attempt_file}" ]]; then
    attempt_epoch="$(tr -dc '0-9' < "${attempt_file}")"
    now_epoch="$(now_date +%s)"
    if [[ "${attempt_epoch}" =~ ^[0-9]+$ ]] &&
       (( now_epoch >= attempt_epoch &&
          now_epoch - attempt_epoch < EOD_RETRY_INTERVAL_SECONDS )); then
      return 1
    fi
  fi
  return 0
}

newest_file_age_seconds() {
  local root_dir="$1"
  local file_name="$2"
  local newest_epoch=0
  local file_epoch
  local now_epoch

  [[ -d "${root_dir}" ]] || { printf '%s\n' -1; return 0; }
  while IFS= read -r -d '' candidate_file; do
    file_epoch="$(stat -c '%Y' "${candidate_file}" 2>/dev/null || printf '0')"
    if [[ "${file_epoch}" =~ ^[0-9]+$ ]] && (( file_epoch > newest_epoch )); then
      newest_epoch="${file_epoch}"
    fi
  done < <(find "${root_dir}" -type f -name "${file_name}" -print0 2>/dev/null)

  if (( newest_epoch == 0 )); then
    printf '%s\n' -1
    return 0
  fi
  now_epoch="$(date +%s)"
  printf '%s\n' $((now_epoch - newest_epoch))
}

last_log_ts_ns() {
  local log_file="$1"
  local pattern="$2"
  [[ -f "${log_file}" ]] || return 1
  grep -E "${pattern}" "${log_file}" | tail -n 1 | sed -n 's/.*ts_ns=\([0-9][0-9]*\).*/\1/p'
}

age_seconds_from_ns() {
  local ts_ns="${1:-0}"
  local now_ns
  [[ "${ts_ns}" =~ ^[0-9]+$ && "${ts_ns}" != "0" ]] || { printf '%s\n' -1; return 0; }
  now_ns="$(date +%s%N)"
  printf '%s\n' $(((now_ns - ts_ns) / 1000000000))
}

check_market_data_freshness() {
  local session_start_epoch="$1"
  local now_epoch
  local tick_age
  local bar_age

  now_epoch="$(date +%s)"
  if (( now_epoch - session_start_epoch < HEALTH_GRACE_SECONDS )); then
    return 0
  fi

  tick_age="$(newest_file_age_seconds "${MARKET_DATA_DIR}" ticks.csv)"
  if (( tick_age < 0 )); then
    send_alert_once "no_tick_file" "critical" "no ticks.csv found under ${MARKET_DATA_DIR} during active trading session"
  elif (( tick_age > TICK_STALE_SECONDS )); then
    send_alert_once "stale_tick" "critical" "latest ticks.csv is stale for ${tick_age}s; threshold=${TICK_STALE_SECONDS}s"
  fi

  bar_age="$(newest_file_age_seconds "${MARKET_DATA_DIR}" bars_1m.csv)"
  if (( bar_age < 0 )); then
    send_alert_once "no_bar_file" "warning" "no bars_1m.csv found under ${MARKET_DATA_DIR} during active trading session"
  elif (( bar_age > BAR_STALE_SECONDS )); then
    send_alert_once "stale_bar" "warning" "latest bars_1m.csv is stale for ${bar_age}s; threshold=${BAR_STALE_SECONDS}s"
  fi
}

check_fill_freshness() {
  local session_start_epoch="$1"
  local log_file
  local order_ts_ns
  local fill_ts_ns
  local fill_ts_value
  local order_age
  local fill_age
  local now_epoch

  log_file="$(current_engine_log || true)"
  [[ -n "${log_file}" && -f "${log_file}" ]] || return 0
  now_epoch="$(date +%s)"
  if (( now_epoch - session_start_epoch < HEALTH_GRACE_SECONDS )); then
    return 0
  fi

  order_ts_ns="$(last_log_ts_ns "${log_file}" 'client_order_id|order_insert|order_intent|ReqOrderInsert|PlaceOrder' || true)"
  fill_ts_ns="$(last_log_ts_ns "${log_file}" 'OnRtnTrade|trading_append_trade_event|PARTIALLY_FILLED|FILLED|filled_volume="?[1-9]' || true)"

  if [[ "${REQUIRE_FILL_HEARTBEAT}" == "1" ]]; then
    fill_age="$(age_seconds_from_ns "${fill_ts_ns:-0}")"
    if (( fill_age < 0 || fill_age > FILL_STALE_SECONDS )); then
      send_alert_once "stale_fill_heartbeat" "warning" \
        "no fill callback detected for ${fill_age}s; threshold=${FILL_STALE_SECONDS}s"
    fi
  fi

  [[ -n "${order_ts_ns}" ]] || return 0
  fill_ts_value="${fill_ts_ns:-0}"
  order_age="$(age_seconds_from_ns "${order_ts_ns}")"
  fill_age="$(age_seconds_from_ns "${fill_ts_value}")"
  if (( fill_age < 0 || order_ts_ns > fill_ts_value )) && (( order_age > FILL_STALE_SECONDS )); then
    send_alert_once "order_without_fill" "warning" \
      "order activity has no newer fill callback for ${order_age}s; threshold=${FILL_STALE_SECONDS}s"
  fi
}

check_core_readiness_health() {
  local session_start_epoch="$1"
  local readiness_file="${QUANT_HFT_READINESS_FILE:-}"
  local now_epoch modified_epoch age_seconds content mode

  now_epoch="$(date +%s)"
  (( now_epoch - session_start_epoch >= HEALTH_GRACE_SECONDS )) || return 0
  if [[ -z "${readiness_file}" || ! -s "${readiness_file}" ]]; then
    send_alert_once "core_readiness_missing" "critical" \
      "core readiness heartbeat is missing; automatic opens remain fail-closed"
    return 1
  fi
  modified_epoch="$(stat -c %Y -- "${readiness_file}" 2>/dev/null || printf '0')"
  [[ "${modified_epoch}" =~ ^[0-9]+$ ]] || modified_epoch=0
  age_seconds=$((now_epoch - modified_epoch))
  if (( age_seconds > READINESS_STALE_SECONDS )); then
    send_alert_once "core_readiness_stale" "critical" \
      "core readiness heartbeat is stale; automatic opens remain fail-closed"
    return 1
  fi

  content="$(tr -d '\n\r' < "${readiness_file}")"
  mode="$(printf '%s\n' "${content}" | \
    LC_ALL=C sed -nE 's/.*"mode"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p' | head -n 1)"
  if grep -Eq '"mode"[[:space:]]*:[[:space:]]*"Ready"' <<< "${content}" &&
     grep -Eq '"recovery_complete"[[:space:]]*:[[:space:]]*true' <<< "${content}" &&
     grep -Eq '"trader_ready"[[:space:]]*:[[:space:]]*true' <<< "${content}" &&
     grep -Eq '"gateway_healthy"[[:space:]]*:[[:space:]]*true' <<< "${content}" &&
     grep -Eq '"settlement_confirmed"[[:space:]]*:[[:space:]]*true' <<< "${content}" &&
     grep -Eq '"pending_exit_count"[[:space:]]*:[[:space:]]*0' <<< "${content}" &&
     grep -Eq '"unresolved_mapping_count"[[:space:]]*:[[:space:]]*0' <<< "${content}"; then
    return 0
  fi
  [[ -n "${mode}" ]] || mode="unknown"
  send_alert_once "core_readiness_not_ready" "critical" \
    "core readiness is ${mode}; automatic opens remain fail-closed"
  return 1
}

stop_engine() {
  local reason="${1:-schedule_stop}"
  local process_pid
  local waited_seconds=0

  process_pid="$(current_pid || true)"
  if ! pid_is_alive "${process_pid}"; then
    rm -f "${CURRENT_PID_FILE}"
    return 0
  fi

  echo "[step] stopping core_engine pid=${process_pid} reason=${reason}"
  kill -TERM "${process_pid}" 2>/dev/null || true
  while pid_is_alive "${process_pid}" && (( waited_seconds < STOP_TIMEOUT_SECONDS )); do
    sleep 1
    waited_seconds=$((waited_seconds + 1))
  done
  if pid_is_alive "${process_pid}"; then
    send_alert_once "stop_timeout" "critical" "core_engine pid=${process_pid} did not stop after ${STOP_TIMEOUT_SECONDS}s; sending KILL"
    kill -KILL "${process_pid}" 2>/dev/null || true
  fi
  rm -f "${CURRENT_PID_FILE}"
}

calendar_digest() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${SESSION_CALENDAR_FILE}" | awk '{print "sha256:" $1}'
  else
    cksum "${SESSION_CALENDAR_FILE}" | awk '{print "cksum:" $1 ":" $2}'
  fi
}

prewarm_marker_path() {
  local session_label="$1"
  local natural_date="$2"
  local trading_day="$3"
  printf '%s/%s.%s.%s.ok\n' "${PREWARM_STATE_DIR}" "${natural_date}" "${session_label}" \
    "${trading_day}"
}

prewarm_marker_payload() {
  local session_label="$1"
  local natural_date="$2"
  local trading_day="$3"
  local digest

  digest="$(calendar_digest)" || return 1
  printf 'schema_version=1\n'
  printf 'natural_date=%s\n' "${natural_date}"
  printf 'session=%s\n' "${session_label}"
  printf 'trading_day=%s\n' "${trading_day}"
  printf 'product_scope=%s\n' "${PRODUCT_SCOPE}"
  printf 'calendar_digest=%s\n' "${digest}"
  printf 'local_checks=passed\n'
}

prewarm_marker_is_valid() {
  local session_label="$1"
  local natural_date="$2"
  local trading_day="$3"
  local marker_file

  marker_file="$(prewarm_marker_path "${session_label}" "${natural_date}" "${trading_day}")"
  [[ -f "${marker_file}" ]] || return 1
  cmp -s "${marker_file}" <(prewarm_marker_payload "${session_label}" "${natural_date}" \
    "${trading_day}")
}

prewarm_session() {
  local session_label="$1"
  local natural_date="$2"
  local trading_day="$3"
  local marker_file
  local temporary_marker

  marker_file="$(prewarm_marker_path "${session_label}" "${natural_date}" "${trading_day}")"
  if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "[dry-run] prewarm: session=${session_label} natural_date=${natural_date} trading_day=${trading_day} product_scope=${PRODUCT_SCOPE}"
    echo "[dry-run] prewarm-check: calendar=${SESSION_CALENDAR_FILE} config=${CONFIG_PATH} build_dir=${BUILD_DIR}"
    return 0
  fi
  if prewarm_marker_is_valid "${session_label}" "${natural_date}" "${trading_day}"; then
    return 0
  fi

  [[ -x "${START_SCRIPT}" ]] || {
    send_alert_once "prewarm.start_script_missing" "critical" \
      "prewarm start script is not executable: ${START_SCRIPT}"
    return 1
  }
  [[ -f "${CONFIG_PATH}" ]] || {
    send_alert_once "prewarm.config_missing" "critical" \
      "prewarm config file is missing: ${CONFIG_PATH}"
    return 1
  }
  [[ -x "${BUILD_DIR}/core_engine" ]] || {
    send_alert_once "prewarm.core_engine_missing" "critical" \
      "prewarm core_engine is not executable: ${BUILD_DIR}/core_engine"
    return 1
  }
  [[ -x "${BUILD_DIR}/simnow_probe" ]] || {
    send_alert_once "prewarm.simnow_probe_missing" "critical" \
      "prewarm simnow_probe is not executable: ${BUILD_DIR}/simnow_probe"
    return 1
  }
  check_free_disk "${RUN_ROOT}" "${MIN_FREE_MB}" || return 1
  check_free_disk "${MARKET_DATA_DIR}" "${MIN_FREE_MB}" || return 1

  mkdir -p "${PREWARM_STATE_DIR}"
  temporary_marker="${marker_file}.tmp.${BASHPID}"
  if ! prewarm_marker_payload "${session_label}" "${natural_date}" "${trading_day}" > \
      "${temporary_marker}"; then
    rm -f "${temporary_marker}"
    return 1
  fi
  mv -f "${temporary_marker}" "${marker_file}"
  echo "[info] prewarm passed session=${session_label} natural_date=${natural_date} trading_day=${trading_day}" | \
    tee -a "${SUPERVISOR_LOG}"
}

start_engine_for_session() {
  local session_label="$1"
  local trading_day="$2"
  local restart_number="$3"
  local run_id
  local start_cmd

  run_id="${RUN_ID_PREFIX}-${trading_day}-${session_label}-$(date +%H%M%S)-r${restart_number}"
  start_cmd=(
    "${START_SCRIPT}"
    --env-file "${ENV_FILE}"
    --config "${CONFIG_PATH}"
    --build-dir "${BUILD_DIR}"
    --run-root "${RUN_ROOT}"
    --wal-file "${WAL_FILE}"
    --run-id "${run_id}"
    --probe-seconds "${PROBE_SECONDS}"
    --probe-timeout-seconds "${PROBE_TIMEOUT_SECONDS}"
    --health-interval-ms "${HEALTH_INTERVAL_MS}"
    --instrument-timeout-seconds "${INSTRUMENT_TIMEOUT_SECONDS}"
    --min-free-mb "${MIN_FREE_MB}"
    --log-max-bytes "${LOG_MAX_BYTES}"
    --log-retention-days "${LOG_RETENTION_DAYS}"
    --background
  )

  if [[ ${DRY_RUN} -eq 1 ]]; then
    printf '[dry-run] start:'
    printf ' %q' "${start_cmd[@]}"
    printf '\n'
    return 0
  fi

  echo "[step] starting session=${session_label} trading_day=${trading_day} restart=${restart_number}"
  if (exec 8>&-; "${start_cmd[@]}"); then
    printf '%s|%s|%s\n' "${session_label}" "${trading_day}" "$(date +%s)" > "${SESSION_STATE_FILE}"
    return 0
  fi

  send_alert_once "start_failed" "critical" "failed to start core_engine for session=${session_label} trading_day=${trading_day}"
  return 1
}

start_signal_execution_monitor() {
  local monitor_pid=""
  local monitor_pid_file="${RUN_ROOT}/signal_execution_monitor.pid"
  local monitor_log="${RUN_ROOT}/signal_execution_monitor.log"

  [[ "${SIGNAL_MONITOR_ENABLED}" == "1" || "${SIGNAL_MONITOR_EXTERNAL}" == "1" ]] || return 0
  [[ "${SIGNAL_MONITOR_EXTERNAL}" != "1" ]] || return 0
  if [[ ! -x "${SIGNAL_MONITOR_SCRIPT}" ]]; then
    send_alert_once "signal_monitor_missing" "warning" \
      "signal execution monitor script is not executable: ${SIGNAL_MONITOR_SCRIPT}"
    return 1
  fi
  if [[ -f "${monitor_pid_file}" ]]; then
    monitor_pid="$(tr -dc '0-9' < "${monitor_pid_file}" || true)"
    if pid_is_alive "${monitor_pid}"; then
      return 0
    fi
  fi

  echo "[step] starting signal execution monitor" | tee -a "${SUPERVISOR_LOG}"
  (
    exec 8>&-
    QUANT_ROOT="${QUANT_ROOT}" SIMNOW_RUN_ROOT="${RUN_ROOT}" \
      SIMNOW_MARKET_DATA_DIR="${MARKET_DATA_DIR}" SIMNOW_WAL_FILE="${WAL_FILE}" \
      "${SIGNAL_MONITOR_SCRIPT}" \
        --run-root "${RUN_ROOT}" \
        --market-data-dir "${MARKET_DATA_DIR}" \
        --wal-file "${WAL_FILE}" \
        --monitor-root "${SIGNAL_MONITOR_ROOT}" \
        --heartbeat-file "${SIGNAL_MONITOR_HEARTBEAT_FILE}" \
        --poll-seconds "${SIGNAL_MONITOR_POLL_SECONDS}"
  ) > "${monitor_log}" 2>&1 &
  printf '%s\n' "$!" > "${monitor_pid_file}"
}

check_signal_monitor_heartbeat() {
  local session_start_epoch="$1"
  local now_epoch
  local heartbeat_age

  [[ "${SIGNAL_MONITOR_ENABLED}" == "1" || "${SIGNAL_MONITOR_EXTERNAL}" == "1" ]] || return 0
  now_epoch="$(date +%s)"
  if (( now_epoch - session_start_epoch < HEALTH_GRACE_SECONDS )); then
    return 0
  fi
  if [[ ! -s "${SIGNAL_MONITOR_HEARTBEAT_FILE}" ]]; then
    send_alert_once "signal_monitor_heartbeat_missing" "critical" \
      "signal execution monitor heartbeat is missing: ${SIGNAL_MONITOR_HEARTBEAT_FILE}"
    return 1
  fi
  heartbeat_age="$(newest_file_age_seconds "$(dirname "${SIGNAL_MONITOR_HEARTBEAT_FILE}")" \
    "$(basename "${SIGNAL_MONITOR_HEARTBEAT_FILE}")")"
  if (( heartbeat_age < 0 || heartbeat_age > SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS )); then
    send_alert_once "signal_monitor_heartbeat_stale" "critical" \
      "signal execution monitor heartbeat is stale for ${heartbeat_age}s; threshold=${SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS}s"
    return 1
  fi
}

count_market_rows_by_trading_day() {
  local file_name="$1"
  local trading_day="$2"
  local total_rows=0
  local file_rows

  [[ -d "${MARKET_DATA_DIR}" ]] || { printf '%s\n' 0; return 0; }
  while IFS= read -r -d '' csv_file; do
    file_rows="$(awk -F, -v trading_day="${trading_day}" 'FNR > 1 && $3 == trading_day {count++} END {print count + 0}' "${csv_file}")"
    total_rows=$((total_rows + file_rows))
  done < <(find "${MARKET_DATA_DIR}" -type f -name "${file_name}" -print0 2>/dev/null)
  printf '%s\n' "${total_rows}"
}

write_daily_report() {
  local trading_day="$1"
  local output_dir="$2"
  local tick_rows
  local bar_rows
  local wal_order_events=0
  local wal_trade_events=0
  local report_md="${output_dir}/simnow_daily_report.md"
  local report_json="${output_dir}/simnow_daily_report.json"

  mkdir -p "${output_dir}"
  tick_rows="$(count_market_rows_by_trading_day ticks.csv "${trading_day}")"
  bar_rows="$(count_market_rows_by_trading_day bars_1m.csv "${trading_day}")"
  if [[ -f "${WAL_FILE}" ]]; then
    wal_order_events="$(grep -Ec '"event_type":"order_update"|"kind":"order"' "${WAL_FILE}" || true)"
    wal_trade_events="$(grep -Ec '"event_type":"trade_fill"|"kind":"trade"' "${WAL_FILE}" || true)"
  fi

  cat > "${report_json}" <<EOF
{"trading_day":"${trading_day}","tick_rows":${tick_rows},"bar_rows":${bar_rows},"wal_order_events":${wal_order_events},"wal_trade_events":${wal_trade_events},"market_data_dir":"${MARKET_DATA_DIR}","wal_file":"${WAL_FILE}","export_dir":"${EXPORT_ROOT}/${trading_day}","reconcile_dir":"${RECONCILE_ROOT}/${trading_day}"}
EOF

  cat > "${report_md}" <<EOF
# SimNow Daily Report ${trading_day}

- Status: generated
- Tick rows: ${tick_rows}
- 1m bar rows: ${bar_rows}
- WAL order events: ${wal_order_events}
- WAL trade/fill events: ${wal_trade_events}
- Market data dir: ${MARKET_DATA_DIR}
- WAL file: ${WAL_FILE}
- Export dir: ${EXPORT_ROOT}/${trading_day}
- Reconcile dir: ${RECONCILE_ROOT}/${trading_day}

Linked artifacts in this directory:
- daily_settlement_evidence.json
- settlement_diff.json
- simnow_export.log
- simnow_export_manifest.env
- ops_health_report.json / ops_health_report.md
- ops_alert_report.json / ops_alert_report.md
EOF
}

run_optional_analysis_command() {
  local trading_day="$1"
  local output_dir="$2"

  [[ -n "${SIMNOW_ANALYSIS_COMMAND:-}" ]] || return 0
  echo "[step] running SIMNOW_ANALYSIS_COMMAND"
  if ! TRADING_DAY="${trading_day}" SIMNOW_EOD_DIR="${output_dir}" RUN_ROOT="${RUN_ROOT}" \
      bash -lc "${SIMNOW_ANALYSIS_COMMAND}" > "${output_dir}/analysis_command.log" 2>&1; then
    send_alert_once "analysis_failed" "warning" "SIMNOW_ANALYSIS_COMMAND failed for trading_day=${trading_day}"
    return 1
  fi
}

run_readonly_broker_position_snapshot() {
  local trading_day="$1"
  local output_dir="$2"
  local flow_dir="${RUN_ROOT}/eod/${trading_day}/position_snapshot_flow"
  local status_file="${output_dir}/eod_fallback_status.env"

  if [[ ! -x "${POSITION_SNAPSHOT_BIN}" ]]; then
    printf 'schema_version=1\nmode=position_snapshot_only\nstatus=unavailable\nreason=binary_missing\n' \
      > "${status_file}"
    send_alert_once "position_snapshot_missing" "warning" \
      "read-only broker position snapshot binary is missing: ${POSITION_SNAPSHOT_BIN}"
    return 1
  fi

  mkdir -p "${flow_dir}"
  if "${POSITION_SNAPSHOT_BIN}" --config "${CONFIG_PATH}" --flow-path "${flow_dir}" \
      --query-timeout-seconds "${INSTRUMENT_TIMEOUT_SECONDS}" \
      > "${output_dir}/broker_position_snapshot.log" 2>&1; then
    printf 'schema_version=1\nmode=position_snapshot_only\nstatus=completed\nsettlement_completed=false\n' \
      > "${status_file}"
    send_alert_once "settlement_position_snapshot_only" "warning" \
      "full settlement unavailable; read-only broker position snapshot completed for trading_day=${trading_day}"
    return 0
  fi

  printf 'schema_version=1\nmode=position_snapshot_only\nstatus=failed\nsettlement_completed=false\n' \
    > "${status_file}"
  send_alert_once "position_snapshot_failed" "critical" \
    "full settlement and read-only broker position snapshot both failed for trading_day=${trading_day}"
  return 1
}

run_end_of_day_chain() {
  local trading_day="$1"
  local output_dir="${REPORT_ROOT}/${trading_day}"
  local marker_file="${EOD_STATE_DIR}/${trading_day}.done"
  local terminal_file="${EOD_STATE_DIR}/${trading_day}.terminal"
  local attempt_file="${EOD_STATE_DIR}/${trading_day}.last_attempt_epoch"
  local settlement_cmd
  local export_cmd

  [[ -f "${marker_file}" || -f "${terminal_file}" ]] && return 0
  mkdir -p "${output_dir}"
  printf '%s\n' "$(now_date +%s)" > "${attempt_file}"

  echo "[step] running end-of-day chain for trading_day=${trading_day}"
  stop_engine "end_of_day"

  if [[ -x "${DAILY_SETTLEMENT_SCRIPT}" ]]; then
    settlement_cmd=(
      "${DAILY_SETTLEMENT_SCRIPT}"
      --trading-day "${trading_day}"
      --ctp-config-path "${CONFIG_PATH}"
      --evidence-json "${output_dir}/daily_settlement_evidence.json"
      --diff-json "${output_dir}/settlement_diff.json"
    )
    if [[ "${EOD_EXECUTE}" == "1" ]]; then
      settlement_cmd+=(--execute)
    fi
    if ! "${settlement_cmd[@]}" > "${output_dir}/daily_settlement.log" 2>&1; then
      send_alert_once "settlement_failed" "critical" "daily settlement failed for trading_day=${trading_day}"
      if run_readonly_broker_position_snapshot "${trading_day}" "${output_dir}"; then
        printf 'terminal_at=%s\nsettlement_completed=false\nposition_snapshot_completed=true\n' \
          "$(date -Is)" > "${terminal_file}"
      fi
      return 1
    fi
  else
    send_alert_once "settlement_missing" "warning" "daily settlement script is not executable: ${DAILY_SETTLEMENT_SCRIPT}"
    if run_readonly_broker_position_snapshot "${trading_day}" "${output_dir}"; then
      printf 'terminal_at=%s\nsettlement_completed=false\nposition_snapshot_completed=true\n' \
        "$(date -Is)" > "${terminal_file}"
    fi
    return 1
  fi

  if [[ -x "${EXPORT_SCRIPT}" ]]; then
    export_cmd=(
      "${EXPORT_SCRIPT}"
      --trading-day "${trading_day}"
      --wal-file "${WAL_FILE}"
      --export-root "${EXPORT_ROOT}"
      --reconcile-root "${RECONCILE_ROOT}"
      --report-root "${REPORT_ROOT}"
    )
    if [[ "${EOD_PROJECT_DB}" == "1" ]]; then
      export_cmd+=(--project-db)
    elif [[ "${EOD_QUERY_DB}" == "1" ]]; then
      export_cmd+=(--query-db)
    fi
    if [[ "${STRICT_RECONCILE}" == "1" ]]; then
      export_cmd+=(--strict-reconcile)
    fi
    if [[ "${CONVERT_MARKET_PARQUET}" == "1" ]]; then
      export_cmd+=(--convert-market-parquet)
    fi
    if ! "${export_cmd[@]}" > "${output_dir}/simnow_export.log" 2>&1; then
      if [[ "${STRICT_RECONCILE}" == "1" ]]; then
        send_alert_once "simnow_export_failed" "critical" "SimNow WAL export/reconcile failed for trading_day=${trading_day}"
        return 1
      fi
      send_alert_once "simnow_export_failed" "warning" "SimNow WAL export/reconcile failed for trading_day=${trading_day}"
    fi
  else
    send_alert_once "simnow_export_missing" "warning" "SimNow export script is not executable: ${EXPORT_SCRIPT}"
  fi

  if [[ -x "${OPS_HEALTH_BIN}" ]]; then
    "${OPS_HEALTH_BIN}" \
      --environment simnow \
      --service core_engine \
      --core-process-alive false \
      --strategy-engine-chain-status settled \
      --storage-redis-health unknown \
      --storage-timescale-health unknown \
      --scope "simnow unattended trading" \
      --output_json "${output_dir}/ops_health_report.json" \
      --output_md "${output_dir}/ops_health_report.md" \
      > "${output_dir}/ops_health_report.log" 2>&1 || \
      send_alert_once "health_report_failed" "warning" "ops health report generation failed for ${trading_day}"
  fi

  if [[ -x "${OPS_ALERT_BIN}" && -f "${output_dir}/ops_health_report.json" ]]; then
    "${OPS_ALERT_BIN}" \
      --health-json-file "${output_dir}/ops_health_report.json" \
      --output_json "${output_dir}/ops_alert_report.json" \
      --output_md "${output_dir}/ops_alert_report.md" \
      > "${output_dir}/ops_alert_report.log" 2>&1 || \
      send_alert_once "alert_report_failed" "warning" "ops alert report generation failed for ${trading_day}"
  fi

  write_daily_report "${trading_day}" "${output_dir}"
  run_optional_analysis_command "${trading_day}" "${output_dir}" || true

  printf 'done_at=%s\n' "$(date -Is)" > "${marker_file}"
  send_alert "info" "end-of-day chain completed for SimNow trading_day=${trading_day}; output_dir=${output_dir}"
}

print_dry_run_decision() {
  local schedule_info
  local phase
  local session_label
  local natural_date
  local session_range
  local trading_day
  echo "[dry-run] root=${QUANT_ROOT}"
  echo "[dry-run] env_file=${ENV_FILE}"
  echo "[dry-run] config=${CONFIG_PATH}"
  echo "[dry-run] windows=${TRADING_WINDOWS}"
  echo "[dry-run] prewarm_windows=${PREWARM_WINDOWS}"
  echo "[dry-run] session_calendar_file=${SESSION_CALENDAR_FILE:-<missing>}"
  echo "[dry-run] product_scope=${PRODUCT_SCOPE:-<missing>}"
  echo "[dry-run] eod_time=${EOD_TIME} eod_execute=${EOD_EXECUTE}"
  echo "[dry-run] run_root=${RUN_ROOT}"
  echo "[dry-run] market_data_dir=${MARKET_DATA_DIR}"
  echo "[dry-run] wal_file=${WAL_FILE}"
  echo "[dry-run] report_root=${REPORT_ROOT}"
  echo "[dry-run] export_root=${EXPORT_ROOT}"
  echo "[dry-run] reconcile_root=${RECONCILE_ROOT}"
  echo "[dry-run] eod_project_db=${EOD_PROJECT_DB} eod_query_db=${EOD_QUERY_DB} strict_reconcile=${STRICT_RECONCILE} convert_market_parquet=${CONVERT_MARKET_PARQUET}"
  if schedule_info="$(current_schedule_info)"; then
    IFS='|' read -r phase session_label natural_date session_range <<< "${schedule_info}"
    if trading_day="$(session_calendar_trading_day "${natural_date}" "${session_label}")"; then
      if [[ "${phase}" == "prewarm" ]]; then
        echo "[dry-run] decision=prewarm_start_or_keep_alive session=${session_label} natural_date=${natural_date} trading_day=${trading_day} range=${session_range} permission=CloseOnly"
        prewarm_session "${session_label}" "${natural_date}" "${trading_day}"
        start_engine_for_session "${session_label}" "${trading_day}" 0
      else
        echo "[dry-run] decision=start_or_keep_alive session=${session_label} natural_date=${natural_date} trading_day=${trading_day} range=${session_range}"
        prewarm_session "${session_label}" "${natural_date}" "${trading_day}"
        start_engine_for_session "${session_label}" "${trading_day}" 0
      fi
    else
      echo "[dry-run] decision=fail_closed phase=${phase} session=${session_label} natural_date=${natural_date} reason=${trading_day}"
      return 2
    fi
  else
    echo "[dry-run] decision=outside_trading_window"
    if today_eod_due; then
      echo "[dry-run] eod_due=true trading_day=$(now_date +%Y%m%d)"
    else
      echo "[dry-run] eod_due=false"
    fi
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) require_value "$1" "${2:-}"; ENV_FILE="$2"; shift 2 ;;
    --config|--ctp-config-path) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; shift 2 ;;
    --build-dir)
      require_value "$1" "${2:-}"
      BUILD_DIR="$2"
      OPS_HEALTH_BIN="${BUILD_DIR}/ops_health_report_cli"
      OPS_ALERT_BIN="${BUILD_DIR}/ops_alert_report_cli"
      POSITION_SNAPSHOT_BIN="${BUILD_DIR}/simnow_flatten_positions"
      shift 2
      ;;
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; RUN_ROOT_SET_BY_CLI=1; shift 2 ;;
    --market-data-dir) require_value "$1" "${2:-}"; MARKET_DATA_DIR="$2"; MARKET_DATA_DIR_SET_BY_CLI=1; shift 2 ;;
    --wal-file) require_value "$1" "${2:-}"; WAL_FILE="$2"; WAL_FILE_SET_BY_CLI=1; shift 2 ;;
    --report-root) require_value "$1" "${2:-}"; REPORT_ROOT="$2"; REPORT_ROOT_SET_BY_CLI=1; shift 2 ;;
    --export-root) require_value "$1" "${2:-}"; EXPORT_ROOT="$2"; EXPORT_ROOT_SET_BY_CLI=1; shift 2 ;;
    --reconcile-root) require_value "$1" "${2:-}"; RECONCILE_ROOT="$2"; RECONCILE_ROOT_SET_BY_CLI=1; shift 2 ;;
    --windows) require_value "$1" "${2:-}"; TRADING_WINDOWS="$2"; WINDOWS_SET_BY_CLI=1; shift 2 ;;
    --prewarm-windows) require_value "$1" "${2:-}"; PREWARM_WINDOWS="$2"; PREWARM_WINDOWS_SET_BY_CLI=1; shift 2 ;;
    --session-calendar-file|--trading-days-file)
      require_value "$1" "${2:-}"
      SESSION_CALENDAR_FILE="$2"
      SESSION_CALENDAR_FILE_SET_BY_CLI=1
      shift 2
      ;;
    --product-scope)
      require_value "$1" "${2:-}"
      PRODUCT_SCOPE="$2"
      PRODUCT_SCOPE_SET_BY_CLI=1
      shift 2
      ;;
    --eod-time) require_value "$1" "${2:-}"; EOD_TIME="$2"; shift 2 ;;
    --check-interval-seconds) require_value "$1" "${2:-}"; CHECK_INTERVAL_SECONDS="$2"; shift 2 ;;
    --max-restarts) require_value "$1" "${2:-}"; MAX_RESTARTS_PER_WINDOW="$2"; shift 2 ;;
    --min-free-mb) require_value "$1" "${2:-}"; MIN_FREE_MB="$2"; shift 2 ;;
    --tick-stale-seconds) require_value "$1" "${2:-}"; TICK_STALE_SECONDS="$2"; shift 2 ;;
    --bar-stale-seconds) require_value "$1" "${2:-}"; BAR_STALE_SECONDS="$2"; shift 2 ;;
    --fill-stale-seconds) require_value "$1" "${2:-}"; FILL_STALE_SECONDS="$2"; shift 2 ;;
    --no-eod) NO_EOD=1; shift ;;
    --no-stop-outside) NO_STOP_OUTSIDE=1; shift ;;
    --once) RUN_ONCE=1; shift ;;
    --dry-run) DRY_RUN=1; RUN_ONCE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

is_positive_int "${CHECK_INTERVAL_SECONDS}" || die "--check-interval-seconds must be positive"
is_non_negative_int "${MAX_RESTARTS_PER_WINDOW}" || die "--max-restarts must be non-negative"
is_positive_int "${RESTART_DELAY_SECONDS}" || die "SIMNOW_RESTART_DELAY_SECONDS must be positive"
is_positive_int "${STOP_TIMEOUT_SECONDS}" || die "SIMNOW_STOP_TIMEOUT_SECONDS must be positive"
is_positive_int "${MIN_FREE_MB}" || die "--min-free-mb must be positive"
is_positive_int "${LOG_MAX_BYTES}" || die "SIMNOW_LOG_MAX_BYTES must be positive"
is_non_negative_int "${LOG_RETENTION_DAYS}" || die "SIMNOW_LOG_RETENTION_DAYS must be non-negative"
is_non_negative_int "${HEALTH_GRACE_SECONDS}" || die "SIMNOW_HEALTH_GRACE_SECONDS must be non-negative"
is_positive_int "${READINESS_STALE_SECONDS}" || die "SIMNOW_READINESS_STALE_SECONDS must be positive"
is_positive_int "${TICK_STALE_SECONDS}" || die "--tick-stale-seconds must be positive"
is_positive_int "${BAR_STALE_SECONDS}" || die "--bar-stale-seconds must be positive"
is_positive_int "${FILL_STALE_SECONDS}" || die "--fill-stale-seconds must be positive"
is_bool_flag "${REQUIRE_FILL_HEARTBEAT}" || die "SIMNOW_REQUIRE_FILL_HEARTBEAT must be 0 or 1"
is_bool_flag "${EOD_EXECUTE}" || die "SIMNOW_EOD_EXECUTE must be 0 or 1"
is_positive_int "${EOD_RETRY_INTERVAL_SECONDS}" ||
  die "SIMNOW_EOD_RETRY_INTERVAL_SECONDS must be positive"
[[ -z "${EPOCH_FIRST_TRADING_DAY}" || "${EPOCH_FIRST_TRADING_DAY}" =~ ^[0-9]{8}$ ]] ||
  die "SIMNOW_EPOCH_FIRST_TRADING_DAY must use YYYYMMDD"
is_bool_flag "${EOD_PROJECT_DB}" || die "SIMNOW_EOD_PROJECT_DB must be 0 or 1"
is_bool_flag "${EOD_QUERY_DB}" || die "SIMNOW_EOD_QUERY_DB must be 0 or 1"
is_bool_flag "${STRICT_RECONCILE}" || die "SIMNOW_STRICT_RECONCILE must be 0 or 1"
is_bool_flag "${CONVERT_MARKET_PARQUET}" || die "SIMNOW_EOD_CONVERT_MARKET_PARQUET must be 0 or 1"
is_positive_int "${INSTRUMENT_TIMEOUT_SECONDS}" || die "SIMNOW_INSTRUMENT_TIMEOUT_SECONDS must be positive"

cd "${QUANT_ROOT}"
[[ -f "${ENV_FILE}" ]] || die "env file not found: ${ENV_FILE}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

if [[ ${WINDOWS_SET_BY_CLI} -eq 0 ]]; then
  TRADING_WINDOWS="${SIMNOW_TRADING_WINDOWS:-${TRADING_WINDOWS}}"
fi
if [[ ${PREWARM_WINDOWS_SET_BY_CLI} -eq 0 ]]; then
  PREWARM_WINDOWS="${SIMNOW_PREWARM_WINDOWS:-${PREWARM_WINDOWS}}"
fi
if [[ ${SESSION_CALENDAR_FILE_SET_BY_CLI} -eq 0 ]]; then
  SESSION_CALENDAR_FILE="${SIMNOW_SESSION_CALENDAR_FILE:-${SESSION_CALENDAR_FILE}}"
fi
if [[ ${PRODUCT_SCOPE_SET_BY_CLI} -eq 0 ]]; then
  PRODUCT_SCOPE="${SIMNOW_PRODUCT_SCOPE:-${PRODUCT_SCOPE}}"
fi
if [[ -z "${PRODUCT_SCOPE}" && -r "${SESSION_CALENDAR_FILE}" ]]; then
  PRODUCT_SCOPE="$(sed -n 's/^# product_scope=//p' "${SESSION_CALENDAR_FILE}" | head -n 1)"
fi
PRODUCT_SCOPE="$(normalize_product_scope "${PRODUCT_SCOPE}" 2>/dev/null || true)"
export SIMNOW_PRODUCT_SCOPE="${PRODUCT_SCOPE}"
if [[ ${RUN_ROOT_SET_BY_CLI} -eq 0 ]]; then
  RUN_ROOT="${SIMNOW_RUN_ROOT:-${RUN_ROOT}}"
fi
if [[ ${MARKET_DATA_DIR_SET_BY_CLI} -eq 0 ]]; then
  MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${MARKET_DATA_DIR}}"
fi
if [[ ${WAL_FILE_SET_BY_CLI} -eq 0 ]]; then
  WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_HFT_WAL_FILE:-${WAL_FILE}}}"
fi
if [[ ${REPORT_ROOT_SET_BY_CLI} -eq 0 ]]; then
  REPORT_ROOT="${SIMNOW_REPORT_ROOT:-${REPORT_ROOT}}"
fi
if [[ ${EXPORT_ROOT_SET_BY_CLI} -eq 0 ]]; then
  EXPORT_ROOT="${SIMNOW_EXPORT_ROOT:-${EXPORT_ROOT}}"
fi
if [[ ${RECONCILE_ROOT_SET_BY_CLI} -eq 0 ]]; then
  RECONCILE_ROOT="${SIMNOW_RECONCILE_ROOT:-${RECONCILE_ROOT}}"
fi
# Resolve paths under exactly the environment that the engine will inherit.
export CTP_SIM_MARKET_FRONT="${CTP_SIM_MARKET_FRONT:-tcp://182.254.243.31:30011}"
export CTP_SIM_TRADER_FRONT="${CTP_SIM_TRADER_FRONT:-tcp://182.254.243.31:30001}"
export CTP_SIM_IS_PRODUCTION_MODE="${CTP_SIM_IS_PRODUCTION_MODE:-true}"
export CTP_SIM_ENABLE_REAL_API="${CTP_SIM_ENABLE_REAL_API:-true}"
if [[ ${WAL_FILE_SET_BY_CLI} -eq 0 && -n "${QUANT_HFT_WAL_FILE:-}" &&
      -n "${SIMNOW_WAL_FILE:-}" && "${QUANT_HFT_WAL_FILE}" != "${SIMNOW_WAL_FILE}" ]]; then
  die "QUANT_HFT_WAL_FILE conflicts with SIMNOW_WAL_FILE; select one explicit --wal-file"
fi
if [[ -z "${RUN_ROOT}" || -z "${WAL_FILE}" || -z "${MARKET_DATA_DIR}" ||
      -z "${REPORT_ROOT}" || -z "${EXPORT_ROOT}" || -z "${RECONCILE_ROOT}" ]]; then
  load_runtime_path_defaults
  RUN_ROOT="${RUN_ROOT:-${RESOLVED_RUN_ROOT}}"
  WAL_FILE="${WAL_FILE:-${RESOLVED_WAL_FILE}}"
  MARKET_DATA_DIR="${MARKET_DATA_DIR:-${RESOLVED_MARKET_DATA_DIR}}"
  REPORT_ROOT="${REPORT_ROOT:-${RESOLVED_REPORT_ROOT}}"
  EXPORT_ROOT="${EXPORT_ROOT:-${RESOLVED_EXPORT_ROOT}}"
  RECONCILE_ROOT="${RECONCILE_ROOT:-${RESOLVED_RECONCILE_ROOT}}"
  export QUANT_HFT_READINESS_FILE="${QUANT_HFT_READINESS_FILE:-${RESOLVED_READINESS_FILE}}"
fi
export QUANT_HFT_WAL_FILE="${WAL_FILE}"
export SIMNOW_WAL_FILE="${WAL_FILE}"
export QUANT_HFT_MARKET_DATA_DIR="${MARKET_DATA_DIR}"
SIGNAL_MONITOR_ROOT="${SIGNAL_MONITOR_ROOT:-${RUN_ROOT}/monitor}"
EOD_TIME="${SIMNOW_EOD_TIME:-${EOD_TIME}}"
EOD_EXECUTE="${SIMNOW_EOD_EXECUTE:-${EOD_EXECUTE}}"
EOD_PROJECT_DB="${SIMNOW_EOD_PROJECT_DB:-${EOD_PROJECT_DB}}"
EOD_QUERY_DB="${SIMNOW_EOD_QUERY_DB:-${EOD_QUERY_DB}}"
STRICT_RECONCILE="${SIMNOW_STRICT_RECONCILE:-${STRICT_RECONCILE}}"
CONVERT_MARKET_PARQUET="${SIMNOW_EOD_CONVERT_MARKET_PARQUET:-${CONVERT_MARKET_PARQUET}}"
PROBE_SECONDS="${SIMNOW_PROBE_SECONDS:-${PROBE_SECONDS}}"
PROBE_TIMEOUT_SECONDS="${SIMNOW_PROBE_TIMEOUT_SECONDS:-${PROBE_TIMEOUT_SECONDS}}"
INSTRUMENT_TIMEOUT_SECONDS="${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-${INSTRUMENT_TIMEOUT_SECONDS}}"
ALLOW_UNCONFIRMED_SETTLEMENT="${SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT:-${ALLOW_UNCONFIRMED_SETTLEMENT}}"
SIGNAL_MONITOR_ENABLED="${SIMNOW_SIGNAL_EXECUTION_MONITOR:-${SIGNAL_MONITOR_ENABLED}}"
SIGNAL_MONITOR_EXTERNAL="${SIMNOW_SIGNAL_MONITOR_EXTERNAL:-${SIGNAL_MONITOR_EXTERNAL}}"
SIGNAL_MONITOR_POLL_SECONDS="${SIMNOW_SIGNAL_MONITOR_POLL_SECONDS:-${SIGNAL_MONITOR_POLL_SECONDS}}"
SIGNAL_MONITOR_ROOT="${SIMNOW_SIGNAL_MONITOR_ROOT:-${SIGNAL_MONITOR_ROOT}}"
SIGNAL_MONITOR_HEARTBEAT_FILE="${SIMNOW_SIGNAL_MONITOR_HEARTBEAT_FILE:-${SIGNAL_MONITOR_ROOT}/heartbeat.json}"
SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS="${SIMNOW_SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS:-${SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS}}"
is_bool_flag "${EOD_EXECUTE}" || die "SIMNOW_EOD_EXECUTE must be 0 or 1"
is_bool_flag "${EOD_PROJECT_DB}" || die "SIMNOW_EOD_PROJECT_DB must be 0 or 1"
is_bool_flag "${EOD_QUERY_DB}" || die "SIMNOW_EOD_QUERY_DB must be 0 or 1"
is_bool_flag "${STRICT_RECONCILE}" || die "SIMNOW_STRICT_RECONCILE must be 0 or 1"
is_bool_flag "${CONVERT_MARKET_PARQUET}" || die "SIMNOW_EOD_CONVERT_MARKET_PARQUET must be 0 or 1"
is_bool_flag "${ALLOW_UNCONFIRMED_SETTLEMENT}" || die "SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT must be 0 or 1"
is_bool_flag "${SIGNAL_MONITOR_ENABLED}" || die "SIMNOW_SIGNAL_EXECUTION_MONITOR must be 0 or 1"
is_bool_flag "${SIGNAL_MONITOR_EXTERNAL}" || die "SIMNOW_SIGNAL_MONITOR_EXTERNAL must be 0 or 1"
is_positive_int "${PROBE_SECONDS}" || die "SIMNOW_PROBE_SECONDS must be positive"
is_positive_int "${PROBE_TIMEOUT_SECONDS}" || die "SIMNOW_PROBE_TIMEOUT_SECONDS must be positive"
is_positive_int "${INSTRUMENT_TIMEOUT_SECONDS}" || die "SIMNOW_INSTRUMENT_TIMEOUT_SECONDS must be positive"
is_positive_int "${SIGNAL_MONITOR_POLL_SECONDS}" || die "SIMNOW_SIGNAL_MONITOR_POLL_SECONDS must be positive"
is_positive_int "${SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS}" || die "SIMNOW_SIGNAL_MONITOR_HEARTBEAT_STALE_SECONDS must be positive"
[[ -n "${RUN_ROOT}" ]] || die "SIMNOW_RUN_ROOT must not be empty"
[[ -n "${MARKET_DATA_DIR}" ]] || die "SIMNOW_MARKET_DATA_DIR must not be empty"
[[ -n "${WAL_FILE}" ]] || die "SIMNOW_WAL_FILE must not be empty"
[[ -n "${REPORT_ROOT}" ]] || die "SIMNOW_REPORT_ROOT must not be empty"
[[ -n "${EXPORT_ROOT}" ]] || die "SIMNOW_EXPORT_ROOT must not be empty"
[[ -n "${RECONCILE_ROOT}" ]] || die "SIMNOW_RECONCILE_ROOT must not be empty"

CONFIG_SETTLEMENT_CONFIRM_REQUIRED="$(yaml_bool_value settlement_confirm_required "${CONFIG_PATH}")"
if is_true_text "${CTP_SIM_ENABLE_REAL_API:-true}" && \
   [[ "${CONFIG_SETTLEMENT_CONFIRM_REQUIRED}" == "false" && "${ALLOW_UNCONFIRMED_SETTLEMENT}" != "1" ]]; then
  die "settlement_confirm_required=false is unsafe for real SimNow trading; set it true or export SIMNOW_ALLOW_UNCONFIRMED_SETTLEMENT=1 for diagnostics only"
fi

mkdir -p "${RUN_ROOT}" "${REPORT_ROOT}" "${EXPORT_ROOT}" "${RECONCILE_ROOT}"
LOCK_DIR="${SIMNOW_LOCK_DIR:-${RUN_ROOT}/locks}"
LOCK_FILE="${LOCK_DIR}/supervisor.lock"
CURRENT_PID_FILE="${SIMNOW_CURRENT_PID_FILE:-${RUN_ROOT}/current_core_engine.pid}"
CURRENT_RUN_FILE="${SIMNOW_CURRENT_RUN_FILE:-${RUN_ROOT}/current_run_dir}"
CURRENT_LOG_FILE="${SIMNOW_CURRENT_LOG_FILE:-${RUN_ROOT}/current_core_engine_log}"
ALERT_STATE_DIR="${RUN_ROOT}/alert_state"
EOD_STATE_DIR="${RUN_ROOT}/eod"
PREWARM_STATE_DIR="${RUN_ROOT}/prewarm"
SESSION_STATE_FILE="${RUN_ROOT}/current_session.env"
SUPERVISOR_LOG="${RUN_ROOT}/supervisor.log"
mkdir -p "${LOCK_DIR}" "${ALERT_STATE_DIR}" "${EOD_STATE_DIR}"

if command -v flock >/dev/null 2>&1; then
  exec 8>"${LOCK_FILE}"
  flock -n 8 || die "another SimNow supervisor is already running: ${LOCK_FILE}"
else
  LOCK_FALLBACK_DIR="${LOCK_FILE}.d"
  mkdir "${LOCK_FALLBACK_DIR}" 2>/dev/null || die "another SimNow supervisor is already running: ${LOCK_FALLBACK_DIR}"
  trap 'rm -rf "${LOCK_FALLBACK_DIR}"' EXIT
fi

if [[ ${DRY_RUN} -eq 1 ]]; then
  print_dry_run_decision
  exit 0
fi

remove_stale_current_pid
echo "[info] SimNow supervisor started at $(date -Is)" | tee -a "${SUPERVISOR_LOG}"
echo "[info] windows=${TRADING_WINDOWS}" | tee -a "${SUPERVISOR_LOG}"
echo "[info] prewarm_windows=${PREWARM_WINDOWS}" | tee -a "${SUPERVISOR_LOG}"
echo "[info] session_calendar_file=${SESSION_CALENDAR_FILE:-<missing>} product_scope=${PRODUCT_SCOPE}" | \
  tee -a "${SUPERVISOR_LOG}"
start_signal_execution_monitor || true

active_session_key=""
session_start_epoch="$(date +%s)"
restart_count=0

while true; do
  start_signal_execution_monitor || true
  rotate_logs
  check_free_disk "${RUN_ROOT}" "${MIN_FREE_MB}" || true
  check_free_disk "${MARKET_DATA_DIR}" "${MIN_FREE_MB}" || true

  if schedule_info="$(current_schedule_info)"; then
    IFS='|' read -r phase session_label natural_date session_range <<< "${schedule_info}"
    if trading_day="$(session_calendar_trading_day "${natural_date}" "${session_label}")"; then
      # Prewarm and active are separate retry budgets.  A temporarily unavailable broker at
      # 20:45 must not exhaust all start attempts before the 21:00 market opens.
      session_key="${trading_day}.${session_label}.${natural_date}.${phase}"
      if [[ "${session_key}" != "${active_session_key}" ]]; then
        active_session_key="${session_key}"
        restart_count=0
        session_start_epoch="$(date +%s)"
        echo "[info] entering phase=${phase} session=${session_label} natural_date=${natural_date} trading_day=${trading_day} range=${session_range}" | \
          tee -a "${SUPERVISOR_LOG}"
      fi

      if ! prewarm_session "${session_label}" "${natural_date}" "${trading_day}"; then
        stop_engine "prewarm_authorization_failed"
        send_alert_once "prewarm_authorization_failed.${natural_date}.${session_label}" \
          "critical" \
          "prewarm authorization unavailable for session=${session_label} natural_date=${natural_date} trading_day=${trading_day}"
      else
        process_pid="$(current_pid || true)"
        if pid_is_alive "${process_pid}"; then
          check_core_readiness_health "${session_start_epoch}" || true
          if [[ "${phase}" == "active" ]]; then
            check_market_data_freshness "${session_start_epoch}"
            check_fill_freshness "${session_start_epoch}"
            check_signal_monitor_heartbeat "${session_start_epoch}" || true
          fi
        else
          if [[ -n "${process_pid}" ]]; then
            send_alert_once "core_crashed" "critical" \
              "core_engine pid=${process_pid} is no longer alive; session=${session_key}"
          fi
          if (( restart_count >= MAX_RESTARTS_PER_WINDOW )); then
            send_alert_once "restart_budget_exhausted" "critical" \
              "restart budget exhausted for ${session_key}; max=${MAX_RESTARTS_PER_WINDOW}"
          else
            restart_count=$((restart_count + 1))
            start_engine_for_session "${session_label}" "${trading_day}" \
              "${restart_count}" || true
            sleep "${RESTART_DELAY_SECONDS}"
          fi
        fi
      fi
    else
      active_session_key=""
      restart_count=0
      stop_engine "session_calendar_fail_closed"
      send_alert_once "session_calendar.${natural_date}.${session_label}" "critical" \
        "session calendar rejected new session=${session_label} natural_date=${natural_date}: ${trading_day}"
    fi
  else
    active_session_key=""
    restart_count=0
    if [[ ${NO_STOP_OUTSIDE} -eq 0 ]]; then
      stop_engine "outside_trading_window"
    fi
    if today_eod_due; then
      run_end_of_day_chain "$(now_date +%Y%m%d)" || true
    fi
  fi

  if [[ ${RUN_ONCE} -eq 1 ]]; then
    exit 0
  fi
  sleep "${CHECK_INTERVAL_SECONDS}"
done
