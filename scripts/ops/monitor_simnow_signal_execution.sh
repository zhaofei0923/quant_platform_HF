#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

RUN_ROOT="${SIMNOW_RUN_ROOT:-${QUANT_ROOT}/runtime/trading/runs/simnow}"
MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${QUANT_ROOT}/runtime/market_data/simnow}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_ROOT}/runtime/trading/wal/simnow/events.wal}"
MONITOR_ROOT="${SIMNOW_SIGNAL_MONITOR_ROOT:-${QUANT_ROOT}/runtime/trading/monitor/simnow}"
EVENT_LOG="${SIMNOW_SIGNAL_MONITOR_EVENT_LOG:-${MONITOR_ROOT}/signal_execution_watch.jsonl}"
INCIDENT_ROOT="${SIMNOW_SIGNAL_MONITOR_INCIDENT_ROOT:-${MONITOR_ROOT}/incidents}"
CURRENT_PID_FILE="${SIMNOW_CURRENT_PID_FILE:-${RUN_ROOT}/current_core_engine.pid}"
CURRENT_LOG_FILE="${SIMNOW_CURRENT_LOG_FILE:-${RUN_ROOT}/current_core_engine_log}"
CURRENT_RUN_FILE="${SIMNOW_CURRENT_RUN_FILE:-${RUN_ROOT}/current_run_dir}"
POLL_SECONDS="${SIMNOW_SIGNAL_MONITOR_POLL_SECONDS:-5}"
SIGNAL_TO_ORDER_TIMEOUT_SECONDS="${SIMNOW_SIGNAL_TO_ORDER_TIMEOUT_SECONDS:-30}"
ORDER_TO_CTP_TIMEOUT_SECONDS="${SIMNOW_ORDER_TO_CTP_TIMEOUT_SECONDS:-30}"
CTP_TO_CALLBACK_TIMEOUT_SECONDS="${SIMNOW_CTP_TO_CALLBACK_TIMEOUT_SECONDS:-120}"
FILL_TIMEOUT_SECONDS="${SIMNOW_SIGNAL_FILL_TIMEOUT_SECONDS:-180}"
STATUS_INTERVAL_SECONDS="${SIMNOW_SIGNAL_MONITOR_STATUS_INTERVAL_SECONDS:-60}"
START_AT_END="${SIMNOW_SIGNAL_MONITOR_START_AT_END:-1}"
ONCE=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Continuously monitor SimNow KAMA executable signals and follow whether each signal
reaches order submission, CTP submission, order callbacks, and trade fills. The
script is read-only for trading state; it writes monitor JSONL events and incident
markdown reports under runtime/trading/monitor/simnow by default.

Options:
  --run-root <path>                    SimNow run root (default: ${RUN_ROOT})
  --market-data-dir <path>             Market CSV root (default: ${MARKET_DATA_DIR})
  --wal-file <path>                    WAL file path (default: ${WAL_FILE})
  --monitor-root <path>                Monitor output root (default: ${MONITOR_ROOT})
  --poll-seconds <int>                 Poll interval (default: ${POLL_SECONDS})
  --signal-to-order-timeout <int>      Signal -> order_submitted timeout (default: ${SIGNAL_TO_ORDER_TIMEOUT_SECONDS})
  --order-to-ctp-timeout <int>         order_submitted -> ctp_order_submitted timeout (default: ${ORDER_TO_CTP_TIMEOUT_SECONDS})
  --ctp-to-callback-timeout <int>      CTP submit -> WAL/order callback timeout (default: ${CTP_TO_CALLBACK_TIMEOUT_SECONDS})
  --fill-timeout <int>                 Signal -> trade_fill timeout (default: ${FILL_TIMEOUT_SECONDS})
  --status-interval-seconds <int>      Periodic summary interval; 0 disables (default: ${STATUS_INTERVAL_SECONDS})
  --replay-existing                    Process existing file contents instead of starting at current EOF
  --start-at-end                       Ignore existing file contents on first scan (default)
  --once                               Run one scan and exit
  -h, --help                           Show this help

Alert hooks are optional and inherited from SimNow ops scripts:
  SIMNOW_ALERT_WEBHOOK_URL, SIMNOW_ALERT_EMAIL_TO, SIMNOW_ALERT_COMMAND
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

is_positive_int() {
  [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

json_escape() {
  printf '%s' "$1" | LC_ALL=C sed \
    's/\\/\\\\/g; s/"/\\"/g; s/[[:cntrl:]]/ /g; s/[^ -~]/?/g'
}

send_alert() {
  local severity="${1:-info}"
  local message="${2:-}"
  local escaped_message
  local payload

  [[ -n "${message}" ]] || return 0
  echo "[alert:${severity}] ${message}" >&2

  if [[ -n "${SIMNOW_ALERT_WEBHOOK_URL:-}" ]] && command -v curl >/dev/null 2>&1; then
    escaped_message="$(json_escape "[${severity}] ${message}")"
    payload="{\"msgtype\":\"text\",\"text\":{\"content\":\"${escaped_message}\"}}"
    curl -fsS -m 10 -H 'Content-Type: application/json' \
      -d "${payload}" "${SIMNOW_ALERT_WEBHOOK_URL}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_EMAIL_TO:-}" ]] && command -v mail >/dev/null 2>&1; then
    printf '%s\n' "${message}" | mail -s "[quant-hft][${severity}] SimNow signal execution" \
      "${SIMNOW_ALERT_EMAIL_TO}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_COMMAND:-}" ]]; then
    ALERT_SEVERITY="${severity}" ALERT_MESSAGE="${message}" \
      bash -lc "${SIMNOW_ALERT_COMMAND}" >/dev/null 2>&1 || true
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; shift 2 ;;
    --market-data-dir) require_value "$1" "${2:-}"; MARKET_DATA_DIR="$2"; shift 2 ;;
    --wal-file) require_value "$1" "${2:-}"; WAL_FILE="$2"; shift 2 ;;
    --monitor-root)
      require_value "$1" "${2:-}"
      MONITOR_ROOT="$2"
      EVENT_LOG="${MONITOR_ROOT}/signal_execution_watch.jsonl"
      INCIDENT_ROOT="${MONITOR_ROOT}/incidents"
      shift 2
      ;;
    --poll-seconds) require_value "$1" "${2:-}"; POLL_SECONDS="$2"; shift 2 ;;
    --signal-to-order-timeout)
      require_value "$1" "${2:-}"; SIGNAL_TO_ORDER_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --order-to-ctp-timeout)
      require_value "$1" "${2:-}"; ORDER_TO_CTP_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --ctp-to-callback-timeout)
      require_value "$1" "${2:-}"; CTP_TO_CALLBACK_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --fill-timeout) require_value "$1" "${2:-}"; FILL_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --status-interval-seconds)
      require_value "$1" "${2:-}"; STATUS_INTERVAL_SECONDS="$2"; shift 2 ;;
    --replay-existing) START_AT_END=0; shift ;;
    --start-at-end) START_AT_END=1; shift ;;
    --once) ONCE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

is_positive_int "${POLL_SECONDS}" || die "--poll-seconds must be positive"
is_non_negative_int "${SIGNAL_TO_ORDER_TIMEOUT_SECONDS}" || die "--signal-to-order-timeout must be non-negative"
is_non_negative_int "${ORDER_TO_CTP_TIMEOUT_SECONDS}" || die "--order-to-ctp-timeout must be non-negative"
is_non_negative_int "${CTP_TO_CALLBACK_TIMEOUT_SECONDS}" || die "--ctp-to-callback-timeout must be non-negative"
is_non_negative_int "${FILL_TIMEOUT_SECONDS}" || die "--fill-timeout must be non-negative"
is_non_negative_int "${STATUS_INTERVAL_SECONDS}" || die "--status-interval-seconds must be non-negative"
[[ "${START_AT_END}" == "0" || "${START_AT_END}" == "1" ]] || die "start-at-end flag must be 0 or 1"

mkdir -p "${MONITOR_ROOT}" "${INCIDENT_ROOT}" "$(dirname "${EVENT_LOG}")"

declare -A file_offsets
declare -A signal_seen_epoch signal_minute signal_instrument signal_strategy signal_side signal_ts_ns
declare -A signal_csv_file signal_status signal_reason signal_last_event signal_incident_written
declare -A signal_order_epoch signal_ctp_epoch signal_callback_epoch signal_fill_epoch
declare -A signal_client_order_id signal_exchange_order_id signal_order_ref signal_order_status
declare -A client_trace
declare -A pending_ctp_epoch pending_ctp_order_ref pending_ctp_request_id

initial_scan_done=0
last_core_state="unknown"
last_summary_epoch=0

now_iso() {
  date -Iseconds
}

ns_epoch_seconds() {
  local ns_value="${1:-}"
  if [[ "${ns_value}" =~ ^[0-9]{10,}$ ]]; then
    printf '%s\n' "${ns_value:0:${#ns_value}-9}"
    return 0
  fi
  date +%s
}

pid_is_alive() {
  local process_pid="${1:-}"
  [[ "${process_pid}" =~ ^[0-9]+$ ]] || return 1
  kill -0 "${process_pid}" 2>/dev/null
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

write_event() {
  local event_type="$1"
  local trace_id="${2:-}"
  local message="${3:-}"
  local extra="${4:-}"
  local line

  line="{\"ts\":\"$(json_escape "$(now_iso)")\",\"event\":\"$(json_escape "${event_type}")\""
  if [[ -n "${trace_id}" ]]; then
    line+=",\"trace_id\":\"$(json_escape "${trace_id}")\""
  fi
  if [[ -n "${message}" ]]; then
    line+=",\"message\":\"$(json_escape "${message}")\""
  fi
  if [[ -n "${extra}" ]]; then
    line+=",${extra}"
  fi
  line+="}"
  printf '%s\n' "${line}" >> "${EVENT_LOG}"
}

kv_field() {
  local line="$1"
  local key="$2"
  local value
  value="$(printf '%s\n' "${line}" | LC_ALL=C sed -nE "s/.*(^| )${key}=\"([^\"]*)\".*/\2/p" | head -n 1)"
  if [[ -n "${value}" ]]; then
    printf '%s\n' "${value}"
    return 0
  fi
  printf '%s\n' "${line}" | LC_ALL=C sed -nE "s/.*(^| )${key}=([^ ]+).*/\2/p" | head -n 1
}

json_string_field() {
  local line="$1"
  local key="$2"
  printf '%s\n' "${line}" | LC_ALL=C sed -nE "s/.*\"${key}\":\"([^\"]*)\".*/\1/p" | head -n 1
}

json_number_field() {
  local line="$1"
  local key="$2"
  printf '%s\n' "${line}" | LC_ALL=C sed -nE "s/.*\"${key}\":(-?[0-9.]+).*/\1/p" | head -n 1
}

ensure_signal() {
  local trace_id="$1"
  local seen_epoch="${2:-}"
  [[ -n "${trace_id}" ]] || return 1
  if [[ -z "${signal_seen_epoch[${trace_id}]:-}" ]]; then
    signal_seen_epoch["${trace_id}"]="${seen_epoch:-$(date +%s)}"
    signal_status["${trace_id}"]="observed"
    signal_last_event["${trace_id}"]="observed"
  fi
}

remember_client_trace() {
  local client_order_id="$1"
  local trace_id="$2"
  if [[ -n "${client_order_id}" && -n "${trace_id}" ]]; then
    client_trace["${client_order_id}"]="${trace_id}"
    signal_client_order_id["${trace_id}"]="${client_order_id}"
  fi
}

trace_for_client() {
  local client_order_id="$1"
  if [[ -n "${client_order_id}" && -n "${client_trace[${client_order_id}]:-}" ]]; then
    printf '%s\n' "${client_trace[${client_order_id}]}"
    return 0
  fi
  if [[ "${client_order_id}" =~ ^[^[:space:]]+-(open|close)-[^[:space:]]+-[0-9]{10,}$ ]]; then
    printf '%s\n' "${client_order_id}"
  fi
}

is_settlement_unconfirmed_reject() {
  local text="$1"
  [[ "${text}" == *"ErrorID=42"* || "${text}" == *"结算结果未确认"* ]]
}

remember_pending_ctp_submitted() {
  local client_order_id="$1"
  local order_ref="$2"
  local request_id="$3"
  [[ -n "${client_order_id}" ]] || return 0
  pending_ctp_epoch["${client_order_id}"]="$(date +%s)"
  pending_ctp_order_ref["${client_order_id}"]="${order_ref}"
  pending_ctp_request_id["${client_order_id}"]="${request_id}"
}

apply_pending_ctp_submitted() {
  local trace_id="$1"
  local client_order_id="$2"
  local ctp_epoch
  local order_ref
  local request_id

  [[ -n "${trace_id}" && -n "${client_order_id}" ]] || return 0
  ctp_epoch="${pending_ctp_epoch[${client_order_id}]:-}"
  [[ -n "${ctp_epoch}" ]] || return 0
  order_ref="${pending_ctp_order_ref[${client_order_id}]:-}"
  request_id="${pending_ctp_request_id[${client_order_id}]:-}"
  signal_ctp_epoch["${trace_id}"]="${ctp_epoch}"
  signal_order_ref["${trace_id}"]="${order_ref}"
  signal_status["${trace_id}"]="ctp_order_submitted"
  signal_last_event["${trace_id}"]="ctp_order_submitted"
  write_event "ctp_order_submitted" "${trace_id}" "CTP ReqOrderInsert accepted before order_submitted log" \
    "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"order_ref\":\"$(json_escape "${order_ref}")\",\"request_id\":\"$(json_escape "${request_id}")\",\"backfilled\":true"
  unset "pending_ctp_epoch[${client_order_id}]" "pending_ctp_order_ref[${client_order_id}]" \
    "pending_ctp_request_id[${client_order_id}]"
}

safe_file_token() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

write_incident() {
  local trace_id="$1"
  local reason="$2"
  local detail="${3:-}"
  local now_text
  local incident_path
  local token

  [[ -n "${trace_id}" ]] || return 0
  [[ -z "${signal_incident_written[${trace_id}]:-}" ]] || return 0
  signal_incident_written["${trace_id}"]=1
  signal_status["${trace_id}"]="incident"
  signal_reason["${trace_id}"]="${reason}"
  now_text="$(date +%Y%m%dT%H%M%S)"
  token="$(safe_file_token "${trace_id}")"
  incident_path="${INCIDENT_ROOT}/${now_text}-${token}.md"

  cat > "${incident_path}" <<INCIDENT
# SimNow Signal Execution Incident

- trace_id: ${trace_id}
- reason: ${reason}
- detail: ${detail:-n/a}
- detected_at: $(now_iso)

## Signal

- minute: ${signal_minute[${trace_id}]:-unknown}
- instrument_id: ${signal_instrument[${trace_id}]:-unknown}
- strategy_id: ${signal_strategy[${trace_id}]:-unknown}
- side: ${signal_side[${trace_id}]:-unknown}
- signal_ts_ns: ${signal_ts_ns[${trace_id}]:-unknown}
- csv_file: ${signal_csv_file[${trace_id}]:-unknown}

## Execution Chain

- order_submitted_epoch: ${signal_order_epoch[${trace_id}]:-missing}
- ctp_order_submitted_epoch: ${signal_ctp_epoch[${trace_id}]:-missing}
- order_callback_epoch: ${signal_callback_epoch[${trace_id}]:-missing}
- trade_fill_epoch: ${signal_fill_epoch[${trace_id}]:-missing}
- client_order_id: ${signal_client_order_id[${trace_id}]:-missing}
- exchange_order_id: ${signal_exchange_order_id[${trace_id}]:-missing}
- order_ref: ${signal_order_ref[${trace_id}]:-missing}
- order_status: ${signal_order_status[${trace_id}]:-missing}
- last_event: ${signal_last_event[${trace_id}]:-missing}

## Diagnosis

${detail:-No additional detail captured.}

## Files Checked

- core_log: $(current_engine_log 2>/dev/null || printf 'unknown')
- wal_file: ${WAL_FILE}
- monitor_event_log: ${EVENT_LOG}
INCIDENT

  echo "[incident] ${reason} trace_id=${trace_id} report=${incident_path}"
  write_event "incident" "${trace_id}" "${reason}" "\"detail\":\"$(json_escape "${detail}")\",\"report\":\"$(json_escape "${incident_path}")\""
  send_alert "warn" "SimNow signal execution incident: ${reason} trace_id=${trace_id} report=${incident_path}"
}

mark_signal_passed() {
  local csv_file="$1"
  local minute="$2"
  local instrument_id="$3"
  local strategy_id="$4"
  local side="$5"
  local ts_ns="$6"
  local trace_id
  local seen_epoch

  [[ -n "${instrument_id}" && -n "${strategy_id}" && -n "${side}" && -n "${ts_ns}" ]] || return 0
  trace_id="${strategy_id}-open-${instrument_id}-${ts_ns}"
  seen_epoch="$(ns_epoch_seconds "${ts_ns}")"
  ensure_signal "${trace_id}" "${seen_epoch}"
  signal_minute["${trace_id}"]="${minute}"
  signal_instrument["${trace_id}"]="${instrument_id}"
  signal_strategy["${trace_id}"]="${strategy_id}"
  signal_side["${trace_id}"]="${side}"
  signal_ts_ns["${trace_id}"]="${ts_ns}"
  signal_csv_file["${trace_id}"]="${csv_file}"
  signal_status["${trace_id}"]="signal_passed"
  signal_last_event["${trace_id}"]="signal_passed"
  echo "[signal] ${minute} ${instrument_id} ${strategy_id} ${side} trace_id=${trace_id}"
  write_event "signal_passed" "${trace_id}" "composite gate passed" \
    "\"minute\":\"$(json_escape "${minute}")\",\"instrument_id\":\"$(json_escape "${instrument_id}")\",\"strategy_id\":\"$(json_escape "${strategy_id}")\",\"side\":\"$(json_escape "${side}")\""
}

process_csv_file() {
  local csv_file="$1"
  local total_lines
  local start_line
  [[ -f "${csv_file}" ]] || return 0

  total_lines="$(wc -l < "${csv_file}" | tr -dc '0-9')"
  [[ -n "${total_lines}" ]] || total_lines=0
  start_line="${file_offsets[${csv_file}]:-}"
  if [[ -z "${start_line}" ]]; then
    if [[ "${START_AT_END}" == "1" && "${initial_scan_done}" == "0" ]]; then
      file_offsets["${csv_file}"]="${total_lines}"
      return 0
    fi
    start_line=1
  fi
  if (( total_lines <= start_line )); then
    file_offsets["${csv_file}"]="${total_lines}"
    return 0
  fi

  while IFS=$'\t' read -r minute instrument_id strategy_id side blocked_reason ts_ns; do
    [[ -n "${minute}" ]] || continue
    if [[ -n "${side}" && "${blocked_reason}" == "none" ]]; then
      mark_signal_passed "${csv_file}" "${minute}" "${instrument_id}" "${strategy_id}" \
        "${side}" "${ts_ns}"
    fi
  done < <(
    awk -F, -v start="$((start_line + 1))" '
      NR == 1 {
        header_count = NF
        for (i = 1; i <= NF; ++i) {
          header[$i] = i
        }
        next
      }
      NR < start { next }
      header["raw_signal"] == 0 || header["blocked_reason"] == 0 || header["ts_ns"] == 0 { next }
      {
        raw = $(header["raw_signal"])
        blocked = $(header["blocked_reason"])
        ts_ns = $(header["ts_ns"])
        if (NF > header_count && NF >= 2 && $NF ~ /^[0-9]+$/ && length($NF) >= 16) {
          blocked = $(NF - 1)
          ts_ns = $NF
        }
        if (raw == "") { next }
        minute = header["minute"] ? $(header["minute"]) : ""
        instrument = header["instrument"] ? $(header["instrument"]) : ""
        strategy = header["sub_strategy_id"] ? $(header["sub_strategy_id"]) : ""
        if (strategy == "" && header["engine_strategy_id"]) {
          strategy = $(header["engine_strategy_id"])
        }
        print minute "\t" instrument "\t" strategy "\t" raw "\t" blocked "\t" ts_ns
      }
    ' "${csv_file}"
  )
  file_offsets["${csv_file}"]="${total_lines}"
}

mark_order_submitted() {
  local trace_id="$1"
  local client_order_id="$2"
  local instrument_id="$3"
  local strategy_id="$4"
  local side="$5"
  local event_ts_ns="$6"
  local message="$7"

  [[ -n "${trace_id}" ]] || return 0
  ensure_signal "${trace_id}" "$(ns_epoch_seconds "${event_ts_ns}")"
  remember_client_trace "${client_order_id}" "${trace_id}"
  [[ -n "${instrument_id}" ]] && signal_instrument["${trace_id}"]="${instrument_id}"
  [[ -n "${strategy_id}" ]] && signal_strategy["${trace_id}"]="${strategy_id}"
  [[ -n "${side}" ]] && signal_side["${trace_id}"]="${side}"
  [[ -n "${event_ts_ns}" ]] && signal_ts_ns["${trace_id}"]="${event_ts_ns}"
  signal_order_epoch["${trace_id}"]="$(date +%s)"
  if [[ -n "${signal_ctp_epoch[${trace_id}]:-}" ]]; then
    signal_status["${trace_id}"]="ctp_order_submitted"
    signal_last_event["${trace_id}"]="ctp_order_submitted"
  else
    signal_status["${trace_id}"]="order_submitted"
    signal_last_event["${trace_id}"]="order_submitted"
  fi
  echo "[order] submitted trace_id=${trace_id} client_order_id=${client_order_id} ${message}"
  write_event "order_submitted" "${trace_id}" "${message}" \
    "\"client_order_id\":\"$(json_escape "${client_order_id}")\""
  apply_pending_ctp_submitted "${trace_id}" "${client_order_id}"
}

mark_ctp_submitted() {
  local trace_id="$1"
  local client_order_id="$2"
  local order_ref="$3"
  local request_id="$4"
  [[ -n "${trace_id}" ]] || return 0
  ensure_signal "${trace_id}" "$(date +%s)"
  remember_client_trace "${client_order_id}" "${trace_id}"
  signal_ctp_epoch["${trace_id}"]="$(date +%s)"
  signal_order_ref["${trace_id}"]="${order_ref}"
  signal_status["${trace_id}"]="ctp_order_submitted"
  signal_last_event["${trace_id}"]="ctp_order_submitted"
  echo "[ctp] submitted trace_id=${trace_id} client_order_id=${client_order_id} order_ref=${order_ref} request_id=${request_id}"
  write_event "ctp_order_submitted" "${trace_id}" "CTP ReqOrderInsert accepted" \
    "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"order_ref\":\"$(json_escape "${order_ref}")\",\"request_id\":\"$(json_escape "${request_id}")\""
}

mark_rejected() {
  local trace_id="$1"
  local client_order_id="$2"
  local reason="$3"
  local detail="$4"
  local incident_reason="order_rejected"
  [[ -n "${trace_id}" ]] || return 0
  ensure_signal "${trace_id}" "$(date +%s)"
  remember_client_trace "${client_order_id}" "${trace_id}"
  signal_status["${trace_id}"]="rejected"
  if is_settlement_unconfirmed_reject "${reason} ${detail}"; then
    incident_reason="settlement_unconfirmed"
  fi
  signal_reason["${trace_id}"]="${incident_reason}"
  signal_last_event["${trace_id}"]="rejected"
  echo "[reject] trace_id=${trace_id} reason=${reason} detail=${detail}"
  write_event "order_rejected" "${trace_id}" "${reason}" \
    "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"detail\":\"$(json_escape "${detail}")\",\"classification\":\"$(json_escape "${incident_reason}")\""
  write_incident "${trace_id}" "${incident_reason}" "${reason}; ${detail}"
}

process_log_line() {
  local line="$1"
  local event
  local trace_id
  local client_order_id
  local instrument_id
  local strategy_id
  local side
  local event_ts_ns
  local reason
  local detail
  local order_ref
  local request_id
  local mapped_trace

  event="$(kv_field "${line}" "event")"
  case "${event}" in
    order_submitted)
      trace_id="$(kv_field "${line}" "trace_id")"
      client_order_id="$(kv_field "${line}" "client_order_id")"
      instrument_id="$(kv_field "${line}" "instrument_id")"
      strategy_id="$(kv_field "${line}" "strategy_id")"
      side="$(kv_field "${line}" "side")"
      event_ts_ns="$(kv_field "${line}" "event_ts_ns")"
      detail="$(kv_field "${line}" "message")"
      mark_order_submitted "${trace_id}" "${client_order_id}" "${instrument_id}" \
        "${strategy_id}" "${side}" "${event_ts_ns}" "${detail}"
      ;;
    ctp_order_submitted)
      client_order_id="$(kv_field "${line}" "client_order_id")"
      mapped_trace="$(trace_for_client "${client_order_id}")"
      order_ref="$(kv_field "${line}" "order_ref")"
      request_id="$(kv_field "${line}" "request_id")"
      if [[ -n "${mapped_trace}" ]]; then
        mark_ctp_submitted "${mapped_trace}" "${client_order_id}" "${order_ref}" "${request_id}"
      else
        remember_pending_ctp_submitted "${client_order_id}" "${order_ref}" "${request_id}"
        write_event "ctp_order_submitted_unmapped" "" "CTP submit observed before trace mapping" \
          "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"order_ref\":\"$(json_escape "${order_ref}")\",\"request_id\":\"$(json_escape "${request_id}")\""
      fi
      ;;
    ctp_order_submit_rejected)
      client_order_id="$(kv_field "${line}" "client_order_id")"
      mapped_trace="$(trace_for_client "${client_order_id}")"
      reason="$(kv_field "${line}" "reason")"
      detail="return_code=$(kv_field "${line}" "return_code") request_id=$(kv_field "${line}" "request_id")"
      mark_rejected "${mapped_trace}" "${client_order_id}" "ctp_order_submit_rejected:${reason}" "${detail}"
      ;;
    order_rejected)
      trace_id="$(kv_field "${line}" "trace_id")"
      client_order_id="$(kv_field "${line}" "client_order_id")"
      if [[ -z "${trace_id}" ]]; then
        trace_id="$(trace_for_client "${client_order_id}")"
      fi
      reason="$(kv_field "${line}" "reason")"
      detail="$(kv_field "${line}" "detail")"
      mark_rejected "${trace_id}" "${client_order_id}" "${reason}" "${detail}"
      ;;
    strategy_intent_sink_exception)
      trace_id="$(kv_field "${line}" "trace_id")"
      detail="$(kv_field "${line}" "error")"
      mark_rejected "${trace_id}" "" "strategy_intent_sink_exception" "${detail}"
      ;;
  esac
}

process_log_file() {
  local log_file="$1"
  local total_lines
  local start_line
  [[ -f "${log_file}" ]] || return 0

  total_lines="$(wc -l < "${log_file}" | tr -dc '0-9')"
  [[ -n "${total_lines}" ]] || total_lines=0
  start_line="${file_offsets[${log_file}]:-}"
  if [[ -z "${start_line}" ]]; then
    if [[ "${START_AT_END}" == "1" && "${initial_scan_done}" == "0" ]]; then
      file_offsets["${log_file}"]="${total_lines}"
      return 0
    fi
    start_line=0
  fi
  if (( total_lines < start_line )); then
    start_line=0
  fi
  if (( total_lines <= start_line )); then
    file_offsets["${log_file}"]="${total_lines}"
    return 0
  fi
  while IFS= read -r line; do
    process_log_line "${line}"
  done < <(sed -n "$((start_line + 1)),${total_lines}p" "${log_file}")
  file_offsets["${log_file}"]="${total_lines}"
}

mark_wal_callback() {
  local trace_id="$1"
  local client_order_id="$2"
  local event_type="$3"
  local status="$4"
  local filled_volume="$5"
  local reason="$6"
  local status_msg="$7"
  local exchange_order_id="$8"
  local order_ref="$9"

  [[ -n "${trace_id}" ]] || return 0
  ensure_signal "${trace_id}" "$(date +%s)"
  remember_client_trace "${client_order_id}" "${trace_id}"
  signal_callback_epoch["${trace_id}"]="$(date +%s)"
  signal_order_status["${trace_id}"]="${status}"
  [[ -n "${exchange_order_id}" ]] && signal_exchange_order_id["${trace_id}"]="${exchange_order_id}"
  [[ -n "${order_ref}" ]] && signal_order_ref["${trace_id}"]="${order_ref}"
  signal_last_event["${trace_id}"]="wal:${event_type}:status=${status}"
  write_event "wal_${event_type}" "${trace_id}" "status=${status} filled_volume=${filled_volume}" \
    "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"status\":\"$(json_escape "${status}")\",\"filled_volume\":\"$(json_escape "${filled_volume}")\""

  if [[ "${event_type}" == "trade_fill" || "${status}" == "2" || "${status}" == "3" ]]; then
    if [[ "${filled_volume}" =~ ^[0-9]+$ && "${filled_volume}" -gt 0 ]]; then
      signal_fill_epoch["${trace_id}"]="$(date +%s)"
      signal_status["${trace_id}"]="filled"
      signal_last_event["${trace_id}"]="trade_fill"
      echo "[fill] trace_id=${trace_id} client_order_id=${client_order_id} filled_volume=${filled_volume}"
      write_event "trade_fill" "${trace_id}" "filled_volume=${filled_volume}" \
        "\"client_order_id\":\"$(json_escape "${client_order_id}")\""
      return 0
    fi
  fi

  if [[ "${status}" == "5" ]]; then
    mark_rejected "${trace_id}" "${client_order_id}" "wal_order_rejected" \
      "reason=${reason} status_msg=${status_msg}"
  elif [[ "${status}" == "4" ]]; then
    write_incident "${trace_id}" "order_canceled_without_fill" \
      "order callback status=canceled reason=${reason} status_msg=${status_msg}"
  else
    signal_status["${trace_id}"]="order_callback"
  fi
}

process_wal_line() {
  local line="$1"
  local event_type
  local trace_id
  local client_order_id
  local status
  local filled_volume
  local reason
  local status_msg
  local exchange_order_id
  local order_ref

  [[ "${line}" == *'"event_type"'* ]] || return 0
  event_type="$(json_string_field "${line}" "event_type")"
  [[ "${event_type}" == "order_update" || "${event_type}" == "trade_fill" ]] || return 0
  trace_id="$(json_string_field "${line}" "trace_id")"
  client_order_id="$(json_string_field "${line}" "client_order_id")"
  if [[ -z "${trace_id}" ]]; then
    trace_id="$(trace_for_client "${client_order_id}")"
  fi
  status="$(json_number_field "${line}" "status")"
  filled_volume="$(json_number_field "${line}" "filled_volume")"
  reason="$(json_string_field "${line}" "reason")"
  status_msg="$(json_string_field "${line}" "status_msg")"
  exchange_order_id="$(json_string_field "${line}" "exchange_order_id")"
  order_ref="$(json_string_field "${line}" "order_ref")"
  mark_wal_callback "${trace_id}" "${client_order_id}" "${event_type}" "${status}" \
    "${filled_volume:-0}" "${reason}" "${status_msg}" "${exchange_order_id}" "${order_ref}"
}

process_wal_file() {
  local wal_file="$1"
  local total_lines
  local start_line
  [[ -f "${wal_file}" ]] || return 0
  total_lines="$(wc -l < "${wal_file}" | tr -dc '0-9')"
  [[ -n "${total_lines}" ]] || total_lines=0
  start_line="${file_offsets[${wal_file}]:-}"
  if [[ -z "${start_line}" ]]; then
    if [[ "${START_AT_END}" == "1" && "${initial_scan_done}" == "0" ]]; then
      file_offsets["${wal_file}"]="${total_lines}"
      return 0
    fi
    start_line=0
  fi
  if (( total_lines < start_line )); then
    start_line=0
  fi
  if (( total_lines <= start_line )); then
    file_offsets["${wal_file}"]="${total_lines}"
    return 0
  fi
  while IFS= read -r line; do
    process_wal_line "${line}"
  done < <(sed -n "$((start_line + 1)),${total_lines}p" "${wal_file}")
  file_offsets["${wal_file}"]="${total_lines}"
}

scan_kama_csv_files() {
  [[ -d "${MARKET_DATA_DIR}" ]] || return 0
  while IFS= read -r csv_file; do
    process_csv_file "${csv_file}"
  done < <(find "${MARKET_DATA_DIR}" -path '*/strategy/kama_5m.csv' -type f -print 2>/dev/null | sort)
}

scan_core_log() {
  local log_file
  log_file="$(current_engine_log 2>/dev/null || true)"
  [[ -n "${log_file}" ]] || return 0
  process_log_file "${log_file}"
}

check_core_engine() {
  local pid=""
  local state="stopped"
  if [[ -f "${CURRENT_PID_FILE}" ]]; then
    pid="$(tr -dc '0-9' < "${CURRENT_PID_FILE}" || true)"
  fi
  if pid_is_alive "${pid}"; then
    state="running"
  fi
  if [[ "${state}" != "${last_core_state}" ]]; then
    echo "[core_engine] ${state} pid=${pid:-none}"
    write_event "core_engine_${state}" "" "pid=${pid:-none}" \
      "\"pid\":\"$(json_escape "${pid:-}")\""
    last_core_state="${state}"
  fi
}

check_signal_timeouts() {
  local now_epoch="$1"
  local trace_id
  local seen_epoch
  local age
  local order_age
  local ctp_age
  for trace_id in "${!signal_seen_epoch[@]}"; do
    [[ -z "${signal_fill_epoch[${trace_id}]:-}" ]] || continue
    [[ "${signal_status[${trace_id}]:-}" != "incident" ]] || continue
    [[ "${signal_status[${trace_id}]:-}" != "rejected" ]] || continue
    seen_epoch="${signal_seen_epoch[${trace_id}]:-${now_epoch}}"
    age=$((now_epoch - seen_epoch))

    if [[ -z "${signal_order_epoch[${trace_id}]:-}" ]]; then
      if (( age >= SIGNAL_TO_ORDER_TIMEOUT_SECONDS )); then
        write_incident "${trace_id}" "signal_without_order_submitted" \
          "Composite gate passed, but no order_submitted log was observed within ${SIGNAL_TO_ORDER_TIMEOUT_SECONDS}s. Check strategy intent sink, risk checks, execution planner, and core_engine errors."
      fi
      continue
    fi

    if [[ -z "${signal_ctp_epoch[${trace_id}]:-}" ]]; then
      order_age=$((now_epoch - signal_order_epoch[${trace_id}]))
      if (( order_age >= ORDER_TO_CTP_TIMEOUT_SECONDS )); then
        write_incident "${trace_id}" "order_submitted_without_ctp_submit" \
          "order_submitted was observed, but ctp_order_submitted was not observed within ${ORDER_TO_CTP_TIMEOUT_SECONDS}s. Check CTP gateway readiness, ReqOrderInsert return code, and ctp_order_submit_rejected logs."
      fi
      continue
    fi

    if [[ -z "${signal_callback_epoch[${trace_id}]:-}" ]]; then
      ctp_age=$((now_epoch - signal_ctp_epoch[${trace_id}]))
      if (( ctp_age >= CTP_TO_CALLBACK_TIMEOUT_SECONDS )); then
        write_incident "${trace_id}" "ctp_submit_without_order_callback" \
          "ctp_order_submitted was observed, but no WAL order callback or trade fill was observed within ${CTP_TO_CALLBACK_TIMEOUT_SECONDS}s. Check CTP front callbacks, OnRspOrderInsert/OnRtnOrder/OnErrRtnOrderInsert, and WAL append logs."
      fi
      continue
    fi

    if (( age >= FILL_TIMEOUT_SECONDS )); then
      write_incident "${trace_id}" "order_callback_without_trade_fill" \
        "Order callback was observed, but no trade_fill was observed within ${FILL_TIMEOUT_SECONDS}s. This usually means the order is still resting, canceled without fill, or the submitted price did not cross the market. Check order status, limit price,盘口,涨跌停,保证金, and later WAL/export fills."
    fi
  done
}

print_summary_if_due() {
  local now_epoch="$1"
  local total=0
  local filled=0
  local incidents=0
  local active=0
  local trace_id
  if (( STATUS_INTERVAL_SECONDS == 0 )); then
    return 0
  fi
  if (( now_epoch - last_summary_epoch < STATUS_INTERVAL_SECONDS )); then
    return 0
  fi
  last_summary_epoch="${now_epoch}"
  for trace_id in "${!signal_seen_epoch[@]}"; do
    total=$((total + 1))
    case "${signal_status[${trace_id}]:-}" in
      filled) filled=$((filled + 1)) ;;
      incident|rejected) incidents=$((incidents + 1)) ;;
      *) active=$((active + 1)) ;;
    esac
  done
  echo "[summary] signals=${total} active=${active} filled=${filled} incidents=${incidents} event_log=${EVENT_LOG}"
  write_event "summary" "" "signals=${total} active=${active} filled=${filled} incidents=${incidents}" \
    "\"signals\":${total},\"active\":${active},\"filled\":${filled},\"incidents\":${incidents}"
}

scan_once() {
  local now_epoch
  check_core_engine
  scan_kama_csv_files
  scan_core_log
  process_wal_file "${WAL_FILE}"
  now_epoch="$(date +%s)"
  check_signal_timeouts "${now_epoch}"
  print_summary_if_due "${now_epoch}"
  initial_scan_done=1
}

echo "[start] SimNow signal execution monitor"
echo "[config] market_data_dir=${MARKET_DATA_DIR} wal_file=${WAL_FILE} run_root=${RUN_ROOT} monitor_root=${MONITOR_ROOT}"
write_event "monitor_started" "" "SimNow signal execution monitor started" \
  "\"market_data_dir\":\"$(json_escape "${MARKET_DATA_DIR}")\",\"wal_file\":\"$(json_escape "${WAL_FILE}")\",\"run_root\":\"$(json_escape "${RUN_ROOT}")\""

if (( ONCE == 1 )); then
  scan_once
  exit 0
fi

while true; do
  scan_once
  sleep "${POLL_SECONDS}"
done