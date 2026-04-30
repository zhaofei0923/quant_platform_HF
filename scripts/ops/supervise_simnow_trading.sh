#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

ENV_FILE="${ENV_FILE:-${QUANT_ROOT}/.env}"
CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/sim/ctp.yaml}"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build}"
START_SCRIPT="${SIMNOW_START_SCRIPT:-${SCRIPT_DIR}/start_simnow_trading.sh}"
DAILY_SETTLEMENT_SCRIPT="${SIMNOW_DAILY_SETTLEMENT_SCRIPT:-${SCRIPT_DIR}/run_daily_settlement.sh}"
OPS_HEALTH_BIN="${OPS_HEALTH_BIN:-${BUILD_DIR}/ops_health_report_cli}"
OPS_ALERT_BIN="${OPS_ALERT_BIN:-${BUILD_DIR}/ops_alert_report_cli}"
RUN_ROOT="${SIMNOW_RUN_ROOT:-${QUANT_ROOT}/runtime/simnow_trading}"
MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${QUANT_ROOT}/runtime/market_data/simnow}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_ROOT}/runtime_events.wal}"
TRADING_WINDOWS="${SIMNOW_TRADING_WINDOWS:-night=20:55-02:35,day_am=08:55-11:35,day_pm=13:25-15:20}"
TRADING_DAYS_FILE="${SIMNOW_TRADING_DAYS_FILE:-}"
EOD_TIME="${SIMNOW_EOD_TIME:-15:25}"
EOD_EXECUTE="${SIMNOW_EOD_EXECUTE:-1}"
CHECK_INTERVAL_SECONDS="${SIMNOW_CHECK_INTERVAL_SECONDS:-30}"
RESTART_DELAY_SECONDS="${SIMNOW_RESTART_DELAY_SECONDS:-15}"
MAX_RESTARTS_PER_WINDOW="${SIMNOW_MAX_RESTARTS_PER_WINDOW:-5}"
STOP_TIMEOUT_SECONDS="${SIMNOW_STOP_TIMEOUT_SECONDS:-30}"
MIN_FREE_MB="${SIMNOW_MIN_FREE_MB:-2048}"
LOG_MAX_BYTES="${SIMNOW_LOG_MAX_BYTES:-104857600}"
LOG_RETENTION_DAYS="${SIMNOW_LOG_RETENTION_DAYS:-14}"
HEALTH_GRACE_SECONDS="${SIMNOW_HEALTH_GRACE_SECONDS:-180}"
TICK_STALE_SECONDS="${SIMNOW_TICK_STALE_SECONDS:-180}"
BAR_STALE_SECONDS="${SIMNOW_BAR_STALE_SECONDS:-240}"
FILL_STALE_SECONDS="${SIMNOW_FILL_STALE_SECONDS:-900}"
REQUIRE_FILL_HEARTBEAT="${SIMNOW_REQUIRE_FILL_HEARTBEAT:-0}"
ALERT_THROTTLE_SECONDS="${SIMNOW_ALERT_THROTTLE_SECONDS:-600}"
PROBE_SECONDS="${SIMNOW_PROBE_SECONDS:-5}"
PROBE_TIMEOUT_SECONDS="${SIMNOW_PROBE_TIMEOUT_SECONDS:-120}"
HEALTH_INTERVAL_MS="${SIMNOW_HEALTH_INTERVAL_MS:-1000}"
INSTRUMENT_TIMEOUT_SECONDS="${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-45}"
RUN_ID_PREFIX="${SIMNOW_RUN_ID_PREFIX:-simnow-auto}"
RUN_ONCE=0
DRY_RUN=0
NO_EOD=0
NO_STOP_OUTSIDE=0

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
  --windows <spec>               Trading windows (default: ${TRADING_WINDOWS})
  --trading-days-file <path>     Optional calendar, one YYYYMMDD or YYYY-MM-DD per line
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
  night=20:55-02:35,day_am=08:55-11:35,day_pm=13:25-15:20
  09:00-11:30,13:30-15:00

Alert hooks are inherited from start_simnow_trading.sh:
  SIMNOW_ALERT_WEBHOOK_URL, SIMNOW_ALERT_EMAIL_TO, SIMNOW_ALERT_COMMAND
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

time_to_minutes() {
  local time_text="$1"
  local hour_text="${time_text%:*}"
  local minute_text="${time_text#*:}"
  [[ "${hour_text}" =~ ^[0-9][0-9]?$ && "${minute_text}" =~ ^[0-9][0-9]$ ]] || return 1
  printf '%d\n' $((10#${hour_text} * 60 + 10#${minute_text}))
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

  mkdir -p "${path}"
  free_mb="$(df -Pm "${path}" | awk 'NR == 2 {print $4}')"
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

is_trading_day_allowed() {
  local compact_day="$1"
  local dashed_day
  local weekday

  dashed_day="$(date -d "${compact_day}" +%F)"
  if [[ -n "${TRADING_DAYS_FILE}" ]]; then
    [[ -f "${TRADING_DAYS_FILE}" ]] || return 1
    grep -Eq "^(${compact_day}|${dashed_day})([[:space:]]*(#.*)?)?$" "${TRADING_DAYS_FILE}"
    return $?
  fi

  weekday="$(date -d "${compact_day}" +%u)"
  [[ "${weekday}" =~ ^[1-5]$ ]]
}

current_session_info() {
  local now_minutes
  local today_dash
  local today_compact
  local next_compact
  local window_index=0
  local window_specs
  local spec
  local label
  local range
  local start_text
  local end_text
  local start_minutes
  local end_minutes
  local candidate_day

  now_minutes="$(time_to_minutes "$(date +%H:%M)")"
  today_dash="$(date +%F)"
  today_compact="$(date +%Y%m%d)"
  next_compact="$(date -d "${today_dash} +1 day" +%Y%m%d)"

  IFS=',' read -r -a window_specs <<< "${TRADING_WINDOWS}"
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

    candidate_day=""
    if (( start_minutes <= end_minutes )); then
      if (( now_minutes >= start_minutes && now_minutes < end_minutes )); then
        candidate_day="${today_compact}"
      fi
    else
      if (( now_minutes >= start_minutes )); then
        candidate_day="${next_compact}"
      elif (( now_minutes < end_minutes )); then
        candidate_day="${today_compact}"
      fi
    fi

    if [[ -n "${candidate_day}" ]] && is_trading_day_allowed "${candidate_day}"; then
      printf '%s|%s|%s\n' "${label}" "${candidate_day}" "${range}"
      return 0
    fi
    window_index=$((window_index + 1))
  done
  return 1
}

today_eod_due() {
  local now_minutes
  local eod_minutes
  local today_compact
  local marker_file

  [[ ${NO_EOD} -eq 0 ]] || return 1
  today_compact="$(date +%Y%m%d)"
  is_trading_day_allowed "${today_compact}" || return 1
  now_minutes="$(time_to_minutes "$(date +%H:%M)")"
  eod_minutes="$(time_to_minutes "${EOD_TIME}")" || die "invalid --eod-time: ${EOD_TIME}"
  (( now_minutes >= eod_minutes )) || return 1
  marker_file="${EOD_STATE_DIR}/${today_compact}.done"
  [[ ! -f "${marker_file}" ]]
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
    --run-root "${RUN_ROOT}"
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
  if "${start_cmd[@]}"; then
    printf '%s|%s|%s\n' "${session_label}" "${trading_day}" "$(date +%s)" > "${SESSION_STATE_FILE}"
    return 0
  fi

  send_alert_once "start_failed" "critical" "failed to start core_engine for session=${session_label} trading_day=${trading_day}"
  return 1
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
    wal_order_events="$(grep -c 'client_order_id' "${WAL_FILE}" || true)"
    wal_trade_events="$(grep -Ec 'filled_volume[^0-9]*[1-9]|trade_id' "${WAL_FILE}" || true)"
  fi

  cat > "${report_json}" <<EOF
{"trading_day":"${trading_day}","tick_rows":${tick_rows},"bar_rows":${bar_rows},"wal_order_events":${wal_order_events},"wal_trade_events":${wal_trade_events},"market_data_dir":"${MARKET_DATA_DIR}","wal_file":"${WAL_FILE}"}
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

Linked artifacts in this directory:
- daily_settlement_evidence.json
- settlement_diff.json
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

run_end_of_day_chain() {
  local trading_day="$1"
  local output_dir="${EOD_STATE_DIR}/${trading_day}"
  local marker_file="${EOD_STATE_DIR}/${trading_day}.done"
  local settlement_cmd

  [[ -f "${marker_file}" ]] && return 0
  mkdir -p "${output_dir}"

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
      return 1
    fi
  else
    send_alert_once "settlement_missing" "warning" "daily settlement script is not executable: ${DAILY_SETTLEMENT_SCRIPT}"
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
  local session_info
  echo "[dry-run] root=${QUANT_ROOT}"
  echo "[dry-run] env_file=${ENV_FILE}"
  echo "[dry-run] config=${CONFIG_PATH}"
  echo "[dry-run] windows=${TRADING_WINDOWS}"
  echo "[dry-run] eod_time=${EOD_TIME} eod_execute=${EOD_EXECUTE}"
  echo "[dry-run] market_data_dir=${MARKET_DATA_DIR}"
  if session_info="$(current_session_info)"; then
    IFS='|' read -r session_label trading_day session_range <<< "${session_info}"
    echo "[dry-run] decision=start_or_keep_alive session=${session_label} trading_day=${trading_day} range=${session_range}"
    start_engine_for_session "${session_label}" "${trading_day}" 0
  else
    echo "[dry-run] decision=outside_trading_window"
    if today_eod_due; then
      echo "[dry-run] eod_due=true trading_day=$(date +%Y%m%d)"
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
      shift 2
      ;;
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; shift 2 ;;
    --market-data-dir) require_value "$1" "${2:-}"; MARKET_DATA_DIR="$2"; shift 2 ;;
    --windows) require_value "$1" "${2:-}"; TRADING_WINDOWS="$2"; shift 2 ;;
    --trading-days-file) require_value "$1" "${2:-}"; TRADING_DAYS_FILE="$2"; shift 2 ;;
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
is_positive_int "${TICK_STALE_SECONDS}" || die "--tick-stale-seconds must be positive"
is_positive_int "${BAR_STALE_SECONDS}" || die "--bar-stale-seconds must be positive"
is_positive_int "${FILL_STALE_SECONDS}" || die "--fill-stale-seconds must be positive"
is_bool_flag "${REQUIRE_FILL_HEARTBEAT}" || die "SIMNOW_REQUIRE_FILL_HEARTBEAT must be 0 or 1"
is_bool_flag "${EOD_EXECUTE}" || die "SIMNOW_EOD_EXECUTE must be 0 or 1"
is_positive_int "${INSTRUMENT_TIMEOUT_SECONDS}" || die "SIMNOW_INSTRUMENT_TIMEOUT_SECONDS must be positive"

cd "${QUANT_ROOT}"
[[ -f "${ENV_FILE}" ]] || die "env file not found: ${ENV_FILE}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

mkdir -p "${RUN_ROOT}"
LOCK_DIR="${SIMNOW_LOCK_DIR:-${RUN_ROOT}/locks}"
LOCK_FILE="${LOCK_DIR}/supervisor.lock"
CURRENT_PID_FILE="${SIMNOW_CURRENT_PID_FILE:-${RUN_ROOT}/current_core_engine.pid}"
CURRENT_RUN_FILE="${SIMNOW_CURRENT_RUN_FILE:-${RUN_ROOT}/current_run_dir}"
CURRENT_LOG_FILE="${SIMNOW_CURRENT_LOG_FILE:-${RUN_ROOT}/current_core_engine_log}"
ALERT_STATE_DIR="${RUN_ROOT}/alert_state"
EOD_STATE_DIR="${RUN_ROOT}/eod"
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

echo "[info] SimNow supervisor started at $(date -Is)" | tee -a "${SUPERVISOR_LOG}"
echo "[info] windows=${TRADING_WINDOWS}" | tee -a "${SUPERVISOR_LOG}"

active_session_key=""
session_start_epoch="$(date +%s)"
restart_count=0

while true; do
  rotate_logs
  check_free_disk "${RUN_ROOT}" "${MIN_FREE_MB}" || true
  check_free_disk "${MARKET_DATA_DIR}" "${MIN_FREE_MB}" || true

  if session_info="$(current_session_info)"; then
    IFS='|' read -r session_label trading_day session_range <<< "${session_info}"
    session_key="${trading_day}.${session_label}.${session_range}"
    if [[ "${session_key}" != "${active_session_key}" ]]; then
      active_session_key="${session_key}"
      restart_count=0
      session_start_epoch="$(date +%s)"
      echo "[info] entering session=${session_label} trading_day=${trading_day} range=${session_range}" | tee -a "${SUPERVISOR_LOG}"
    fi

    process_pid="$(current_pid || true)"
    if pid_is_alive "${process_pid}"; then
      check_market_data_freshness "${session_start_epoch}"
      check_fill_freshness "${session_start_epoch}"
    else
      if [[ -n "${process_pid}" ]]; then
        send_alert_once "core_crashed" "critical" "core_engine pid=${process_pid} is no longer alive; session=${session_key}"
      fi
      if (( restart_count >= MAX_RESTARTS_PER_WINDOW )); then
        send_alert_once "restart_budget_exhausted" "critical" \
          "restart budget exhausted for ${session_key}; max=${MAX_RESTARTS_PER_WINDOW}"
      else
        restart_count=$((restart_count + 1))
        start_engine_for_session "${session_label}" "${trading_day}" "${restart_count}" || true
        sleep "${RESTART_DELAY_SECONDS}"
      fi
    fi
  else
    active_session_key=""
    restart_count=0
    if [[ ${NO_STOP_OUTSIDE} -eq 0 ]]; then
      stop_engine "outside_trading_window"
    fi
    if today_eod_due; then
      run_end_of_day_chain "$(date +%Y%m%d)" || true
    fi
  fi

  if [[ ${RUN_ONCE} -eq 1 ]]; then
    exit 0
  fi
  sleep "${CHECK_INTERVAL_SECONDS}"
done