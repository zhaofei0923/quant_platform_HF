#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

RUN_ROOT="${SIMNOW_RUN_ROOT:-${QUANT_ROOT}/runtime/trading/runs/simnow}"
MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${QUANT_ROOT}/runtime/market_data/simnow}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_ROOT}/runtime/trading/wal/simnow/events.wal}"
CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/sim/ctp_sim_trade_candidates.yaml}"
MONITOR_ROOT="${SIMNOW_SIGNAL_MONITOR_ROOT:-${QUANT_ROOT}/runtime/trading/monitor/simnow}"
EVENT_LOG="${SIMNOW_SIGNAL_MONITOR_EVENT_LOG:-${MONITOR_ROOT}/signal_execution_watch.jsonl}"
INCIDENT_ROOT="${SIMNOW_SIGNAL_MONITOR_INCIDENT_ROOT:-${MONITOR_ROOT}/incidents}"
HEARTBEAT_FILE="${SIMNOW_SIGNAL_MONITOR_HEARTBEAT_FILE:-${MONITOR_ROOT}/heartbeat.json}"
CORE_READINESS_FILE="${QUANT_HFT_READINESS_FILE:-${MONITOR_ROOT}/readiness.json}"
CURRENT_PID_FILE="${SIMNOW_CURRENT_PID_FILE:-${RUN_ROOT}/current_core_engine.pid}"
CURRENT_LOG_FILE="${SIMNOW_CURRENT_LOG_FILE:-${RUN_ROOT}/current_core_engine_log}"
CURRENT_RUN_FILE="${SIMNOW_CURRENT_RUN_FILE:-${RUN_ROOT}/current_run_dir}"
CURRENT_SESSION_FILE="${SIMNOW_CURRENT_SESSION_FILE:-${RUN_ROOT}/current_session.env}"
TRADING_SESSIONS_CONFIG="${SIMNOW_TRADING_SESSIONS_CONFIG:-${QUANT_ROOT}/configs/trading_sessions.yaml}"
PRODUCTS="${SIMNOW_PRODUCTS:-}"
POLL_SECONDS="${SIMNOW_SIGNAL_MONITOR_POLL_SECONDS:-5}"
CORE_READINESS_STALE_SECONDS="${SIMNOW_CORE_READINESS_STALE_SECONDS:-0}"
TICK_STALE_SECONDS="${SIMNOW_TICK_STALE_SECONDS:-6}"
BAR_WARN_SECONDS="${SIMNOW_BAR_WARN_SECONDS:-5}"
BAR_CRITICAL_SECONDS="${SIMNOW_BAR_CRITICAL_SECONDS:-10}"
STRATEGY_DECISION_TIMEOUT_SECONDS="${SIMNOW_STRATEGY_DECISION_TIMEOUT_SECONDS:-10}"
EXECUTION_DISPOSITION_TIMEOUT_SECONDS="${SIMNOW_EXECUTION_DISPOSITION_TIMEOUT_SECONDS:-2}"
ALERT_COOLDOWN_SECONDS="${SIMNOW_ALERT_COOLDOWN_SECONDS:-900}"
SIGNAL_TO_ORDER_TIMEOUT_SECONDS="${SIMNOW_SIGNAL_TO_ORDER_TIMEOUT_SECONDS:-${EXECUTION_DISPOSITION_TIMEOUT_SECONDS}}"
ORDER_TO_CTP_TIMEOUT_SECONDS="${SIMNOW_ORDER_TO_CTP_TIMEOUT_SECONDS:-30}"
CTP_TO_CALLBACK_TIMEOUT_SECONDS="${SIMNOW_CTP_TO_CALLBACK_TIMEOUT_SECONDS:-120}"
FILL_TIMEOUT_SECONDS="${SIMNOW_SIGNAL_FILL_TIMEOUT_SECONDS:-180}"
STATUS_INTERVAL_SECONDS="${SIMNOW_SIGNAL_MONITOR_STATUS_INTERVAL_SECONDS:-60}"
START_AT_END="${SIMNOW_SIGNAL_MONITOR_START_AT_END:-1}"
HEALTH_SNAPSHOT_FILE="${SIMNOW_PIPELINE_HEALTH_FILE:-${MONITOR_ROOT}/pipeline_health.json}"
CHECKPOINT_FILE="${SIMNOW_SIGNAL_MONITOR_CHECKPOINT_FILE:-${MONITOR_ROOT}/pipeline_checkpoint_v3.tsv}"
ONCE=0
STRICT_EXIT=0
HEARTBEAT_SET_BY_CLI=0
CORE_READINESS_SET_BY_CLI=0
HEALTH_SNAPSHOT_SET_BY_CLI=0
CHECKPOINT_SET_BY_CLI=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Continuously monitor the SimNow pipeline from market ticks through bars, strategy
decisions, order submission, CTP callbacks, and fills. The script is read-only for
trading state and writes versioned health/checkpoint evidence under runtime.

Options:
  --run-root <path>                    SimNow run root (default: ${RUN_ROOT})
  --market-data-dir <path>             Market CSV root (default: ${MARKET_DATA_DIR})
  --wal-file <path>                    WAL file path (default: ${WAL_FILE})
  --monitor-root <path>                Monitor output root (default: ${MONITOR_ROOT})
  --heartbeat-file <path>              Atomic liveness/session heartbeat (default: ${HEARTBEAT_FILE})
  --core-readiness-file <path>         Core structured readiness heartbeat (default: ${CORE_READINESS_FILE})
  --health-snapshot-file <path>        Atomic pipeline health JSON (default: ${HEALTH_SNAPSHOT_FILE})
  --checkpoint-file <path>             Versioned monitor cursor/state (default: ${CHECKPOINT_FILE})
  --trading-sessions-config <path>     Shared trading sessions YAML (default: ${TRADING_SESSIONS_CONFIG})
  --products <csv>                     Product ids to monitor; otherwise read CTP config/disk
  --poll-seconds <int>                 Poll interval (default: ${POLL_SECONDS})
  --tick-stale-seconds <int>           Active-session dominant tick limit (default: ${TICK_STALE_SECONDS})
  --bar-warn-seconds <int>             Bar finalization warning threshold (default: ${BAR_WARN_SECONDS})
  --bar-critical-seconds <int>         Bar finalization critical threshold (default: ${BAR_CRITICAL_SECONDS})
  --strategy-decision-timeout <int>    Eligible 5m bar -> strategy decision (default: ${STRATEGY_DECISION_TIMEOUT_SECONDS})
  --execution-disposition-timeout <int> Allowed signal -> execution disposition (default: ${EXECUTION_DISPOSITION_TIMEOUT_SECONDS})
  --alert-cooldown-seconds <int>       Duplicate alert suppression window (default: ${ALERT_COOLDOWN_SECONDS})
  --signal-to-order-timeout <int>      Signal -> order_submitted timeout (default: ${SIGNAL_TO_ORDER_TIMEOUT_SECONDS})
  --order-to-ctp-timeout <int>         order_submitted -> ctp_order_submitted timeout (default: ${ORDER_TO_CTP_TIMEOUT_SECONDS})
  --ctp-to-callback-timeout <int>      CTP submit -> WAL/order callback timeout (default: ${CTP_TO_CALLBACK_TIMEOUT_SECONDS})
  --fill-timeout <int>                 Signal -> trade_fill timeout (default: ${FILL_TIMEOUT_SECONDS})
  --status-interval-seconds <int>      Periodic summary interval; 0 disables (default: ${STATUS_INTERVAL_SECONDS})
  --replay-existing                    Process existing file contents instead of starting at current EOF
  --start-at-end                       Ignore existing file contents on first scan (default)
  --once                               Run one scan and exit
  --strict-exit                        once-mode exit: 0 healthy/inactive, 1 degraded, 2 unhealthy
  -h, --help                           Show this help

Alert hooks are optional and inherited from SimNow ops scripts:
  SIMNOW_ALERT_WEBHOOK_URL, SIMNOW_ALERT_EMAIL_TO, SIMNOW_ALERT_COMMAND
USAGE
}

die() {
  echo "error: $*" >&2
  exit 3
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
      if [[ ${HEARTBEAT_SET_BY_CLI} -eq 0 ]]; then
        HEARTBEAT_FILE="${MONITOR_ROOT}/heartbeat.json"
      fi
      if [[ ${CORE_READINESS_SET_BY_CLI} -eq 0 && -z "${QUANT_HFT_READINESS_FILE:-}" ]]; then
        CORE_READINESS_FILE="${MONITOR_ROOT}/readiness.json"
      fi
      if [[ ${HEALTH_SNAPSHOT_SET_BY_CLI} -eq 0 && -z "${SIMNOW_PIPELINE_HEALTH_FILE:-}" ]]; then
        HEALTH_SNAPSHOT_FILE="${MONITOR_ROOT}/pipeline_health.json"
      fi
      if [[ ${CHECKPOINT_SET_BY_CLI} -eq 0 && -z "${SIMNOW_SIGNAL_MONITOR_CHECKPOINT_FILE:-}" ]]; then
        CHECKPOINT_FILE="${MONITOR_ROOT}/pipeline_checkpoint_v3.tsv"
      fi
      shift 2
      ;;
    --heartbeat-file)
      require_value "$1" "${2:-}"
      HEARTBEAT_FILE="$2"
      HEARTBEAT_SET_BY_CLI=1
      shift 2
      ;;
    --core-readiness-file)
      require_value "$1" "${2:-}"
      CORE_READINESS_FILE="$2"
      CORE_READINESS_SET_BY_CLI=1
      shift 2
      ;;
    --health-snapshot-file)
      require_value "$1" "${2:-}"
      HEALTH_SNAPSHOT_FILE="$2"
      HEALTH_SNAPSHOT_SET_BY_CLI=1
      shift 2
      ;;
    --checkpoint-file)
      require_value "$1" "${2:-}"
      CHECKPOINT_FILE="$2"
      CHECKPOINT_SET_BY_CLI=1
      shift 2
      ;;
    --trading-sessions-config)
      require_value "$1" "${2:-}"; TRADING_SESSIONS_CONFIG="$2"; shift 2 ;;
    --products) require_value "$1" "${2:-}"; PRODUCTS="$2"; shift 2 ;;
    --poll-seconds) require_value "$1" "${2:-}"; POLL_SECONDS="$2"; shift 2 ;;
    --tick-stale-seconds) require_value "$1" "${2:-}"; TICK_STALE_SECONDS="$2"; shift 2 ;;
    --bar-warn-seconds) require_value "$1" "${2:-}"; BAR_WARN_SECONDS="$2"; shift 2 ;;
    --bar-critical-seconds) require_value "$1" "${2:-}"; BAR_CRITICAL_SECONDS="$2"; shift 2 ;;
    --strategy-decision-timeout)
      require_value "$1" "${2:-}"; STRATEGY_DECISION_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --execution-disposition-timeout)
      require_value "$1" "${2:-}"; EXECUTION_DISPOSITION_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --alert-cooldown-seconds)
      require_value "$1" "${2:-}"; ALERT_COOLDOWN_SECONDS="$2"; shift 2 ;;
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
    --strict-exit) STRICT_EXIT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

is_positive_int "${POLL_SECONDS}" || die "--poll-seconds must be positive"
is_non_negative_int "${TICK_STALE_SECONDS}" || die "--tick-stale-seconds must be non-negative"
is_non_negative_int "${BAR_WARN_SECONDS}" || die "--bar-warn-seconds must be non-negative"
is_non_negative_int "${BAR_CRITICAL_SECONDS}" || die "--bar-critical-seconds must be non-negative"
(( BAR_CRITICAL_SECONDS >= BAR_WARN_SECONDS )) || die "bar critical threshold must be >= warning threshold"
is_non_negative_int "${STRATEGY_DECISION_TIMEOUT_SECONDS}" || die "--strategy-decision-timeout must be non-negative"
is_non_negative_int "${EXECUTION_DISPOSITION_TIMEOUT_SECONDS}" || die "--execution-disposition-timeout must be non-negative"
is_non_negative_int "${ALERT_COOLDOWN_SECONDS}" || die "--alert-cooldown-seconds must be non-negative"
is_non_negative_int "${CORE_READINESS_STALE_SECONDS}" || \
  die "SIMNOW_CORE_READINESS_STALE_SECONDS must be non-negative"
if (( CORE_READINESS_STALE_SECONDS == 0 )); then
  CORE_READINESS_STALE_SECONDS=$((POLL_SECONDS * 2))
fi
is_non_negative_int "${SIGNAL_TO_ORDER_TIMEOUT_SECONDS}" || die "--signal-to-order-timeout must be non-negative"
is_non_negative_int "${ORDER_TO_CTP_TIMEOUT_SECONDS}" || die "--order-to-ctp-timeout must be non-negative"
is_non_negative_int "${CTP_TO_CALLBACK_TIMEOUT_SECONDS}" || die "--ctp-to-callback-timeout must be non-negative"
is_non_negative_int "${FILL_TIMEOUT_SECONDS}" || die "--fill-timeout must be non-negative"
is_non_negative_int "${STATUS_INTERVAL_SECONDS}" || die "--status-interval-seconds must be non-negative"
[[ "${START_AT_END}" == "0" || "${START_AT_END}" == "1" ]] || die "start-at-end flag must be 0 or 1"
[[ "${STRICT_EXIT}" == "0" || "${STRICT_EXIT}" == "1" ]] || die "strict-exit flag must be 0 or 1"

CURRENT_PID_FILE="${SIMNOW_CURRENT_PID_FILE:-${RUN_ROOT}/current_core_engine.pid}"
CURRENT_LOG_FILE="${SIMNOW_CURRENT_LOG_FILE:-${RUN_ROOT}/current_core_engine_log}"
CURRENT_RUN_FILE="${SIMNOW_CURRENT_RUN_FILE:-${RUN_ROOT}/current_run_dir}"
CURRENT_SESSION_FILE="${SIMNOW_CURRENT_SESSION_FILE:-${RUN_ROOT}/current_session.env}"

mkdir -p "${MONITOR_ROOT}" "${INCIDENT_ROOT}" "$(dirname "${EVENT_LOG}")" \
  "$(dirname "${HEARTBEAT_FILE}")" "$(dirname "${HEALTH_SNAPSHOT_FILE}")" \
  "$(dirname "${CHECKPOINT_FILE}")"

declare -A file_offsets
declare -A signal_seen_epoch signal_minute signal_instrument signal_strategy signal_side signal_ts_ns
declare -A signal_csv_file signal_status signal_last_event signal_incident_written
declare -A signal_order_epoch signal_ctp_epoch signal_callback_epoch signal_fill_epoch
declare -A signal_client_order_id signal_exchange_order_id signal_order_ref signal_order_status
declare -A signal_session_key
declare -A client_trace
declare -A pending_ctp_epoch pending_ctp_order_ref pending_ctp_request_id pending_ctp_ts_ns
declare -A signal_ctp_ts_ns signal_callback_ts_ns
declare -A file_inodes file_sizes file_byte_offsets
declare -A candidate_epoch candidate_count decision_epoch decision_count decision_disposition
declare -A candidate_ts_ns candidate_event_ts_ns decision_ts_ns decision_event_ts_ns
declare -A disposition_epoch disposition_count disposition_ts_ns bar_finalized_by_event_ts_ns
declare -A bar_strategy_invalid_by_event_ts_ns
declare -A signal_fill_warning_written
declare -A previous_stage_status previous_stage_reason last_stage_alert_epoch
declare -A product_status product_instrument product_exchange product_tick_age product_tick_delay_ms
declare -A product_last_1m product_last_5m product_bar_complete product_strategy_minute
declare -A product_strategy_evaluations product_candidates product_allowed product_pending
declare -A product_reason product_schema product_tick_reversals product_duplicate_ticks
declare -A product_volume_regressions
declare -a bar_finalize_samples_ms bar_to_decision_samples_ms
declare -a candidate_to_disposition_samples_ms ctp_to_callback_samples_ms recent_recovery_events

initial_scan_done=0
last_core_state="unknown"
core_engine_pid=""
current_session_key="none"
last_summary_epoch=0
core_readiness_generation=0
core_readiness_mode="unknown"
core_readiness_last_processed_epoch=0
core_readiness_state="unknown"
core_readiness_pending_exit_count=0
core_readiness_unresolved_mapping_count=0
core_readiness_suppressed_instruments=""
core_readiness_recovery_complete="false"
core_readiness_trader_ready="false"
core_readiness_gateway_healthy="false"
core_readiness_settlement_confirmed="false"
pipeline_overall_status="unknown"
pipeline_trading_day=""
pipeline_session="none"
pipeline_warning_count=0
pipeline_critical_count=0
pipeline_late_ticks=0
pipeline_duplicate_ticks=0
pipeline_recent_late_ticks=0
pipeline_recent_duplicate_ticks=0
pipeline_tick_time_reversals=0
pipeline_volume_regressions=0
pipeline_duplicate_bars=0
pipeline_conflict_bars=0
pipeline_incomplete_bars=0
pipeline_missing_strategy_evaluations=0
pipeline_duplicate_dispositions=0
pipeline_strategy_trace_integrity_failures=0
pipeline_generation_mismatch_submissions=0
pipeline_unresolved_traces=0
pipeline_last_change_epoch=0
stage_runtime_status="unknown"
stage_runtime_reason="not_scanned"
stage_market_status="unknown"
stage_market_reason="not_scanned"
stage_bar_1m_status="unknown"
stage_bar_1m_reason="not_scanned"
stage_bar_5m_status="unknown"
stage_bar_5m_reason="not_scanned"
stage_strategy_status="unknown"
stage_strategy_reason="not_scanned"
stage_execution_status="unknown"
stage_execution_reason="not_scanned"

now_iso() {
  if [[ -n "${SIMNOW_MONITOR_FAKE_NOW:-}" ]]; then
    date -d "${SIMNOW_MONITOR_FAKE_NOW}" -Iseconds
  else
    date -Iseconds
  fi
}

now_epoch() {
  if [[ -n "${SIMNOW_MONITOR_FAKE_NOW:-}" ]]; then
    date -d "${SIMNOW_MONITOR_FAKE_NOW}" +%s
  else
    date +%s
  fi
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

resolve_session_key() {
  local run_dir=""
  local session_state=""
  if [[ -s "${CURRENT_RUN_FILE}" ]]; then
    run_dir="$(head -n 1 "${CURRENT_RUN_FILE}")"
    if [[ -n "${run_dir}" ]]; then
      basename "${run_dir}"
      return 0
    fi
  fi
  if [[ -s "${CURRENT_SESSION_FILE}" ]]; then
    session_state="$(head -n 1 "${CURRENT_SESSION_FILE}")"
    if [[ -n "${session_state}" ]]; then
      printf '%s\n' "${session_state}"
      return 0
    fi
  fi
  printf '%s\n' "none"
}

write_heartbeat() {
  local monitor_status="${1:-running}"
  local heartbeat_epoch
  local tmp_file
  local engine_log=""
  local signal_count=0
  local trace_id
  local cursor_lines=0
  local tracked_file

  heartbeat_epoch="$(now_epoch)"
  engine_log="$(current_engine_log 2>/dev/null || true)"
  for trace_id in "${!signal_seen_epoch[@]}"; do
    signal_count=$((signal_count + 1))
  done
  for tracked_file in "${!file_offsets[@]}"; do
    cursor_lines=$((cursor_lines + ${file_offsets[${tracked_file}]:-0}))
  done
  tmp_file="${HEARTBEAT_FILE}.tmp.${BASHPID}"
  cat > "${tmp_file}" <<EOF
{"schema_version":3,"heartbeat_epoch":${heartbeat_epoch},"heartbeat_ts":"$(json_escape "$(now_iso)")","monitor_pid":${BASHPID},"monitor_status":"$(json_escape "${monitor_status}")","core_state":"$(json_escape "${last_core_state}")","core_pid":"$(json_escape "${core_engine_pid}")","session_key":"$(json_escape "${current_session_key}")","signals_tracked":${signal_count},"cursor_lines":${cursor_lines},"generation":${core_readiness_generation},"readiness_mode":"$(json_escape "${core_readiness_mode}")","readiness_state":"$(json_escape "${core_readiness_state}")","readiness_last_processed_epoch":${core_readiness_last_processed_epoch},"pipeline_status":"$(json_escape "${pipeline_overall_status}")","pipeline_last_processed_epoch":${heartbeat_epoch},"pipeline_health_file":"$(json_escape "${HEALTH_SNAPSHOT_FILE}")","core_readiness_file":"$(json_escape "${CORE_READINESS_FILE}")","event_log":"$(json_escape "${EVENT_LOG}")","wal_file":"$(json_escape "${WAL_FILE}")","core_log":"$(json_escape "${engine_log}")"}
EOF
  sync -f "${tmp_file}" 2>/dev/null || true
  mv -f -- "${tmp_file}" "${HEARTBEAT_FILE}"
}

check_core_readiness() {
  local now_epoch
  local modified_epoch
  local age_seconds
  local content
  local next_state="fresh"
  local previous_state="${core_readiness_state}"

  if [[ "${last_core_state}" != "running" ]]; then
    core_readiness_state="core_stopped"
    return 0
  fi

  now_epoch="$(now_epoch)"
  if [[ ! -s "${CORE_READINESS_FILE}" ]]; then
    next_state="missing"
  else
    modified_epoch="$(stat -c %Y -- "${CORE_READINESS_FILE}" 2>/dev/null || printf '0')"
    if [[ ! "${modified_epoch}" =~ ^[0-9]+$ ]]; then
      modified_epoch=0
    fi
    age_seconds=$((now_epoch - modified_epoch))
    if (( age_seconds > CORE_READINESS_STALE_SECONDS )); then
      next_state="stale"
    else
      content="$(tr -d '\n\r' < "${CORE_READINESS_FILE}")"
      core_readiness_generation="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"generation"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n 1)"
      core_readiness_mode="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"mode"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p' | head -n 1)"
      core_readiness_pending_exit_count="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"pending_exit_count"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n 1)"
      core_readiness_unresolved_mapping_count="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"unresolved_mapping_count"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n 1)"
      core_readiness_suppressed_instruments="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"suppressed_instruments"[[:space:]]*:[[:space:]]*\[([^]]*)\].*/\1/p' | \
        tr -d '"[:space:]')"
      core_readiness_recovery_complete="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"recovery_complete"[[:space:]]*:[[:space:]]*(true|false).*/\1/p' | head -n 1)"
      core_readiness_trader_ready="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"trader_ready"[[:space:]]*:[[:space:]]*(true|false).*/\1/p' | head -n 1)"
      core_readiness_gateway_healthy="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"gateway_healthy"[[:space:]]*:[[:space:]]*(true|false).*/\1/p' | head -n 1)"
      core_readiness_settlement_confirmed="$(printf '%s\n' "${content}" | \
        LC_ALL=C sed -nE 's/.*"settlement_confirmed"[[:space:]]*:[[:space:]]*(true|false).*/\1/p' | head -n 1)"
      [[ "${core_readiness_generation}" =~ ^[0-9]+$ ]] || core_readiness_generation=0
      [[ -n "${core_readiness_mode}" ]] || core_readiness_mode="unknown"
      [[ "${core_readiness_pending_exit_count}" =~ ^[0-9]+$ ]] || core_readiness_pending_exit_count=0
      [[ "${core_readiness_unresolved_mapping_count}" =~ ^[0-9]+$ ]] || core_readiness_unresolved_mapping_count=0
      core_readiness_last_processed_epoch="${now_epoch}"
    fi
  fi

  core_readiness_state="${next_state}"
  if [[ "${next_state}" == "fresh" ]]; then
    if [[ "${previous_state}" == "missing" || "${previous_state}" == "stale" ]]; then
      write_event "core_readiness_recovered" "" "Core readiness heartbeat recovered" \
        "\"generation\":${core_readiness_generation},\"mode\":\"$(json_escape "${core_readiness_mode}")\""
      send_alert "info" "SimNow core readiness heartbeat recovered: generation=${core_readiness_generation} mode=${core_readiness_mode}"
    fi
    return 0
  fi
  if [[ "${previous_state}" != "${next_state}" ]]; then
    write_event "core_readiness_${next_state}" "" \
      "Core readiness heartbeat is ${next_state}" \
      "\"core_readiness_file\":\"$(json_escape "${CORE_READINESS_FILE}")\",\"stale_after_seconds\":${CORE_READINESS_STALE_SECONDS}"
    send_alert "critical" "SimNow core readiness heartbeat is ${next_state}: file=${CORE_READINESS_FILE} threshold=${CORE_READINESS_STALE_SECONDS}s"
  fi
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

cursor_start_line=0
cursor_complete_bytes=0

prepare_file_cursor() {
  local file_path="$1"
  local total_lines="$2"
  local current_inode=""
  local current_size="0"
  local previous_inode="${file_inodes[${file_path}]:-}"
  local previous_line="${file_offsets[${file_path}]:-}"
  local previous_bytes="${file_byte_offsets[${file_path}]:-0}"
  local trailing_newlines partial_bytes

  current_inode="$(stat -c '%d:%i' -- "${file_path}" 2>/dev/null || true)"
  current_size="$(stat -c '%s' -- "${file_path}" 2>/dev/null || printf '0')"
  [[ "${current_size}" =~ ^[0-9]+$ ]] || current_size=0
  if [[ -n "${previous_inode}" && "${current_inode}" != "${previous_inode}" ]]; then
    previous_line=0
    previous_bytes=0
  fi
  if [[ "${previous_bytes}" =~ ^[0-9]+$ ]] && (( current_size < previous_bytes )); then
    previous_line=0
    previous_bytes=0
  fi
  if [[ -n "${previous_line}" && "${previous_line}" =~ ^[0-9]+$ ]] &&
      (( total_lines < previous_line )); then
    previous_line=0
  fi
  if [[ -z "${previous_line}" ]]; then
    if [[ "${START_AT_END}" == "1" && "${initial_scan_done}" == "0" ]]; then
      previous_line="${total_lines}"
    else
      previous_line=0
    fi
  fi
  cursor_start_line="${previous_line}"
  cursor_complete_bytes="${current_size}"
  if (( current_size > 0 )); then
    trailing_newlines="$(tail -c 1 -- "${file_path}" | wc -l | tr -dc '0-9')"
    if [[ "${trailing_newlines}" == "0" ]]; then
      partial_bytes="$(tail -n 1 -- "${file_path}" | wc -c | tr -dc '0-9')"
      [[ "${partial_bytes}" =~ ^[0-9]+$ ]] || partial_bytes=0
      cursor_complete_bytes=$((current_size - partial_bytes))
    fi
  fi
  file_inodes["${file_path}"]="${current_inode}"
  file_sizes["${file_path}"]="${current_size}"
  file_byte_offsets["${file_path}"]="${cursor_complete_bytes}"
}

load_checkpoint() {
  local record_type
  local key
  local value1 value2 value3 value4 value5 value6 value7 value8 value9 value10 value11 value12
  [[ -s "${CHECKPOINT_FILE}" ]] || return 0
  if [[ "$(head -n 1 -- "${CHECKPOINT_FILE}")" != $'schema\t3' ]]; then
    echo "[warn] ignoring invalid monitor checkpoint: ${CHECKPOINT_FILE}" >&2
    return 0
  fi
  while IFS=$'\t' read -r record_type key value1 value2 value3 value4 value5 value6 value7 \
    value8 value9 value10 value11 value12; do
    case "${record_type}" in
      schema)
        [[ "${key}" == "3" ]] || return 0
        ;;
      cursor)
        [[ -n "${key}" ]] || continue
        file_inodes["${key}"]="${value1:-}"
        file_sizes["${key}"]="${value2:-0}"
        file_offsets["${key}"]="${value3:-0}"
        file_byte_offsets["${key}"]="${value4:-0}"
        ;;
      signal)
        [[ -n "${key}" ]] || continue
        signal_seen_epoch["${key}"]="${value1:-0}"
        signal_status["${key}"]="${value2:-observed}"
        signal_order_epoch["${key}"]="${value3:-}"
        signal_ctp_epoch["${key}"]="${value4:-}"
        signal_callback_epoch["${key}"]="${value5:-}"
        signal_fill_epoch["${key}"]="${value6:-}"
        signal_client_order_id["${key}"]="${value7:-}"
        signal_instrument["${key}"]="${value8:-}"
        signal_strategy["${key}"]="${value9:-}"
        signal_session_key["${key}"]="${value10:-none}"
        signal_last_event["${key}"]="${value11:-restored}"
        if [[ -n "${value7:-}" ]]; then
          client_trace["${value7}"]="${key}"
        fi
        ;;
      strategy_trace)
        [[ -n "${key}" ]] || continue
        candidate_epoch["${key}"]="${value1:-}"
        candidate_count["${key}"]="${value2:-0}"
        candidate_ts_ns["${key}"]="${value3:-}"
        candidate_event_ts_ns["${key}"]="${value4:-}"
        decision_epoch["${key}"]="${value5:-}"
        decision_count["${key}"]="${value6:-0}"
        decision_disposition["${key}"]="${value7:-}"
        decision_ts_ns["${key}"]="${value8:-}"
        decision_event_ts_ns["${key}"]="${value9:-}"
        disposition_epoch["${key}"]="${value10:-}"
        disposition_count["${key}"]="${value11:-0}"
        disposition_ts_ns["${key}"]="${value12:-}"
        ;;
      execution_ts)
        [[ -n "${key}" ]] || continue
        signal_ctp_ts_ns["${key}"]="${value1:-}"
        signal_callback_ts_ns["${key}"]="${value2:-}"
        ;;
      counter)
        case "${key}" in
          late_ticks) pipeline_late_ticks="${value1:-0}" ;;
          duplicate_ticks) pipeline_duplicate_ticks="${value1:-0}" ;;
          generation_mismatch_submissions) pipeline_generation_mismatch_submissions="${value1:-0}" ;;
        esac
        ;;
      stage)
        previous_stage_status["${key}"]="${value1:-unknown}"
        previous_stage_reason["${key}"]="${value2:-}"
        last_stage_alert_epoch["${key}"]="${value3:-0}"
        ;;
      recovery)
        [[ -n "${key}" ]] && recent_recovery_events+=("${key}")
        ;;
    esac
  done < "${CHECKPOINT_FILE}"
}

save_checkpoint() {
  local tmp_file="${CHECKPOINT_FILE}.tmp.${BASHPID}"
  local tracked_file
  local trace_id
  local stage recovery
  {
    printf 'schema\t3\n'
    for tracked_file in "${!file_offsets[@]}"; do
      printf 'cursor\t%s\t%s\t%s\t%s\t%s\n' "${tracked_file}" \
        "${file_inodes[${tracked_file}]:-}" "${file_sizes[${tracked_file}]:-0}" \
        "${file_offsets[${tracked_file}]:-0}" "${file_byte_offsets[${tracked_file}]:-0}"
    done
    for trace_id in "${!signal_seen_epoch[@]}"; do
      printf 'signal\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "${trace_id}" "${signal_seen_epoch[${trace_id}]:-0}" \
        "${signal_status[${trace_id}]:-observed}" "${signal_order_epoch[${trace_id}]:-}" \
        "${signal_ctp_epoch[${trace_id}]:-}" "${signal_callback_epoch[${trace_id}]:-}" \
        "${signal_fill_epoch[${trace_id}]:-}" "${signal_client_order_id[${trace_id}]:-}" \
        "${signal_instrument[${trace_id}]:-}" "${signal_strategy[${trace_id}]:-}" \
        "${signal_session_key[${trace_id}]:-none}" "${signal_last_event[${trace_id}]:-restored}"
    done
    for trace_id in "${!candidate_epoch[@]}" "${!decision_epoch[@]}" "${!disposition_epoch[@]}"; do
      [[ -n "${trace_id}" ]] || continue
      printf 'strategy_trace\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "${trace_id}" "${candidate_epoch[${trace_id}]:-}" "${candidate_count[${trace_id}]:-0}" \
        "${candidate_ts_ns[${trace_id}]:-}" "${candidate_event_ts_ns[${trace_id}]:-}" \
        "${decision_epoch[${trace_id}]:-}" \
        "${decision_count[${trace_id}]:-0}" "${decision_disposition[${trace_id}]:-}" \
        "${decision_ts_ns[${trace_id}]:-}" "${decision_event_ts_ns[${trace_id}]:-}" \
        "${disposition_epoch[${trace_id}]:-}" "${disposition_count[${trace_id}]:-0}" \
        "${disposition_ts_ns[${trace_id}]:-}"
    done | awk -F'\t' '!seen[$2]++'
    for trace_id in "${!signal_ctp_ts_ns[@]}" "${!signal_callback_ts_ns[@]}"; do
      [[ -n "${trace_id}" ]] || continue
      printf 'execution_ts\t%s\t%s\t%s\n' "${trace_id}" \
        "${signal_ctp_ts_ns[${trace_id}]:-}" "${signal_callback_ts_ns[${trace_id}]:-}"
    done | awk -F'\t' '!seen[$2]++'
    printf 'counter\tlate_ticks\t%s\n' "${pipeline_late_ticks}"
    printf 'counter\tduplicate_ticks\t%s\n' "${pipeline_duplicate_ticks}"
    printf 'counter\tgeneration_mismatch_submissions\t%s\n' \
      "${pipeline_generation_mismatch_submissions}"
    for stage in "${!previous_stage_status[@]}"; do
      printf 'stage\t%s\t%s\t%s\t%s\n' "${stage}" "${previous_stage_status[${stage}]:-unknown}" \
        "${previous_stage_reason[${stage}]:-}" "${last_stage_alert_epoch[${stage}]:-0}"
    done
    for recovery in "${recent_recovery_events[@]}"; do
      printf 'recovery\t%s\n' "${recovery}"
    done
  } > "${tmp_file}"
  sync -f "${tmp_file}" 2>/dev/null || true
  mv -f -- "${tmp_file}" "${CHECKPOINT_FILE}"
}

ensure_signal() {
  local trace_id="$1"
  local seen_epoch="${2:-}"
  [[ -n "${trace_id}" ]] || return 1
  if [[ -z "${signal_seen_epoch[${trace_id}]:-}" ]]; then
    signal_seen_epoch["${trace_id}"]="${seen_epoch:-$(date +%s)}"
    signal_status["${trace_id}"]="observed"
    signal_last_event["${trace_id}"]="observed"
    signal_session_key["${trace_id}"]="${current_session_key}"
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
  local log_ts_ns="${4:-}"
  [[ -n "${client_order_id}" ]] || return 0
  pending_ctp_epoch["${client_order_id}"]="$(ns_epoch_seconds "${log_ts_ns}")"
  pending_ctp_order_ref["${client_order_id}"]="${order_ref}"
  pending_ctp_request_id["${client_order_id}"]="${request_id}"
  pending_ctp_ts_ns["${client_order_id}"]="${log_ts_ns}"
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
  signal_ctp_ts_ns["${trace_id}"]="${pending_ctp_ts_ns[${client_order_id}]:-}"
  signal_order_ref["${trace_id}"]="${order_ref}"
  signal_status["${trace_id}"]="ctp_order_submitted"
  signal_last_event["${trace_id}"]="ctp_order_submitted"
  write_event "ctp_order_submitted" "${trace_id}" "CTP ReqOrderInsert accepted before order_submitted log" \
    "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"order_ref\":\"$(json_escape "${order_ref}")\",\"request_id\":\"$(json_escape "${request_id}")\",\"backfilled\":true"
  unset "pending_ctp_epoch[${client_order_id}]" "pending_ctp_order_ref[${client_order_id}]" \
    "pending_ctp_request_id[${client_order_id}]" "pending_ctp_ts_ns[${client_order_id}]"
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
  ensure_signal "${trace_id}" "$(now_epoch)"
  [[ -z "${signal_incident_written[${trace_id}]:-}" ]] || return 0
  signal_incident_written["${trace_id}"]=1
  signal_status["${trace_id}"]="incident"
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
  prepare_file_cursor "${csv_file}" "${total_lines}"
  start_line="${cursor_start_line}"
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
  local log_ts_ns="${5:-}"
  [[ -n "${trace_id}" ]] || return 0
  ensure_signal "${trace_id}" "$(ns_epoch_seconds "${log_ts_ns}")"
  remember_client_trace "${client_order_id}" "${trace_id}"
  signal_ctp_epoch["${trace_id}"]="$(ns_epoch_seconds "${log_ts_ns}")"
  signal_ctp_ts_ns["${trace_id}"]="${log_ts_ns}"
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
  signal_last_event["${trace_id}"]="rejected"
  echo "[reject] trace_id=${trace_id} reason=${reason} detail=${detail}"
  write_event "order_rejected" "${trace_id}" "${reason}" \
    "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"detail\":\"$(json_escape "${detail}")\",\"classification\":\"$(json_escape "${incident_reason}")\""
  if [[ "${incident_reason}" == "settlement_unconfirmed" ]]; then
    write_incident "${trace_id}" "${incident_reason}" "${reason}; ${detail}"
  fi
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
  local disposition
  local log_ts_ns
  local generation

  event="$(kv_field "${line}" "event")"
  log_ts_ns="$(kv_field "${line}" "ts_ns")"
  case "${event}" in
    signal_candidate)
      trace_id="$(kv_field "${line}" "trace_id")"
      if [[ -n "${trace_id}" ]]; then
        candidate_count["${trace_id}"]=$(( ${candidate_count[${trace_id}]:-0} + 1 ))
        candidate_epoch["${trace_id}"]="$(ns_epoch_seconds "${log_ts_ns}")"
        candidate_ts_ns["${trace_id}"]="${log_ts_ns}"
        candidate_event_ts_ns["${trace_id}"]="$(kv_field "${line}" "event_ts_ns")"
      fi
      ;;
    strategy_decision)
      trace_id="$(kv_field "${line}" "trace_id")"
      disposition="$(kv_field "${line}" "disposition")"
      if [[ -n "${trace_id}" ]]; then
        decision_count["${trace_id}"]=$(( ${decision_count[${trace_id}]:-0} + 1 ))
        decision_epoch["${trace_id}"]="$(ns_epoch_seconds "${log_ts_ns}")"
        decision_ts_ns["${trace_id}"]="${log_ts_ns}"
        decision_event_ts_ns["${trace_id}"]="$(kv_field "${line}" "event_ts_ns")"
        decision_disposition["${trace_id}"]="${disposition}"
      fi
      ;;
    execution_disposition)
      trace_id="$(kv_field "${line}" "trace_id")"
      disposition="$(kv_field "${line}" "disposition")"
      if [[ -n "${trace_id}" ]]; then
        disposition_count["${trace_id}"]=$(( ${disposition_count[${trace_id}]:-0} + 1 ))
        disposition_epoch["${trace_id}"]="$(ns_epoch_seconds "${log_ts_ns}")"
        disposition_ts_ns["${trace_id}"]="${log_ts_ns}"
        if [[ "${disposition}" != "ctp_submitted" ]]; then
          ensure_signal "${trace_id}" "$(ns_epoch_seconds "${log_ts_ns}")"
          signal_status["${trace_id}"]="rejected"
          signal_last_event["${trace_id}"]="execution_disposition:${disposition:-local_rejected}"
        fi
        generation="$(kv_field "${line}" "contract_generation")"
        if [[ "${disposition}" == "ctp_submitted" && "${generation}" =~ ^[0-9]+$ ]] &&
            (( generation != core_readiness_generation )); then
          pipeline_generation_mismatch_submissions=$((pipeline_generation_mismatch_submissions + 1))
          write_incident "${trace_id}" "old_generation_ctp_submission" \
            "execution generation=${generation}, readiness generation=${core_readiness_generation}"
        fi
      fi
      ;;
    market_late_tick_after_watermark)
      pipeline_late_ticks=$((pipeline_late_ticks + 1))
      ;;
    market_duplicate_tick_suppressed)
      pipeline_duplicate_ticks=$((pipeline_duplicate_ticks + 1))
      ;;
    market_bar_canonical_conflict)
      pipeline_conflict_bars=$((pipeline_conflict_bars + 1))
      ;;
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
        mark_ctp_submitted "${mapped_trace}" "${client_order_id}" "${order_ref}" "${request_id}" \
          "${log_ts_ns}"
      else
        remember_pending_ctp_submitted "${client_order_id}" "${order_ref}" "${request_id}" \
          "${log_ts_ns}"
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
  prepare_file_cursor "${log_file}" "${total_lines}"
  start_line="${cursor_start_line}"
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
  local callback_ts_ns="${10:-}"

  [[ -n "${trace_id}" ]] || return 0
  ensure_signal "${trace_id}" "$(ns_epoch_seconds "${callback_ts_ns}")"
  remember_client_trace "${client_order_id}" "${trace_id}"
  signal_callback_epoch["${trace_id}"]="$(ns_epoch_seconds "${callback_ts_ns}")"
  signal_callback_ts_ns["${trace_id}"]="${callback_ts_ns}"
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
    signal_status["${trace_id}"]="canceled"
    signal_last_event["${trace_id}"]="order_canceled"
    write_event "order_canceled" "${trace_id}" \
      "order callback reached terminal canceled state" \
      "\"client_order_id\":\"$(json_escape "${client_order_id}")\",\"reason\":\"$(json_escape "${reason}")\""
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
  local callback_ts_ns

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
  callback_ts_ns="$(json_number_field "${line}" "ts_ns")"
  mark_wal_callback "${trace_id}" "${client_order_id}" "${event_type}" "${status}" \
    "${filled_volume:-0}" "${reason}" "${status_msg}" "${exchange_order_id}" "${order_ref}" \
    "${callback_ts_ns}"
}

process_wal_file() {
  local wal_file="$1"
  local total_lines
  local start_line
  [[ -f "${wal_file}" ]] || return 0
  total_lines="$(wc -l < "${wal_file}" | tr -dc '0-9')"
  [[ -n "${total_lines}" ]] || total_lines=0
  prepare_file_cursor "${wal_file}" "${total_lines}"
  start_line="${cursor_start_line}"
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
  local day_dir
  [[ -d "${MARKET_DATA_DIR}" ]] || return 0
  day_dir="$(latest_trading_day_dir || true)"
  [[ -d "${day_dir}" ]] || return 0
  while IFS= read -r csv_file; do
    process_csv_file "${csv_file}"
  done < <(find "${day_dir}" -path '*/strategy/kama_5m.csv' -type f -print 2>/dev/null | sort)
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
  core_engine_pid="${pid}"
  if [[ "${state}" == "running" ]]; then
    current_session_key="$(resolve_session_key)"
  else
    current_session_key="none"
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
  [[ "${last_core_state}" == "running" ]] || return 0
  for trace_id in "${!signal_seen_epoch[@]}"; do
    [[ "${signal_session_key[${trace_id}]:-none}" == "${current_session_key}" ]] || continue
    [[ -z "${signal_fill_epoch[${trace_id}]:-}" ]] || continue
    [[ "${signal_status[${trace_id}]:-}" != "incident" ]] || continue
    [[ "${signal_status[${trace_id}]:-}" != "rejected" ]] || continue
    [[ "${signal_status[${trace_id}]:-}" != "canceled" ]] || continue
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
      if [[ -z "${signal_fill_warning_written[${trace_id}]:-}" ]]; then
        signal_fill_warning_written["${trace_id}"]=1
        write_event "active_order_without_fill" "${trace_id}" \
          "Order remains active without a fill after ${FILL_TIMEOUT_SECONDS}s" \
          "\"severity\":\"warning\",\"client_order_id\":\"$(json_escape "${signal_client_order_id[${trace_id}]:-}")\""
        send_alert "warn" "SimNow order still active without fill: trace_id=${trace_id} age=${age}s"
      fi
    fi
  done
}

check_trace_integrity_timeouts() {
  local current_epoch="$1"
  local trace_id
  local age
  for trace_id in "${!candidate_epoch[@]}"; do
    age=$((current_epoch - ${candidate_epoch[${trace_id}]:-${current_epoch}}))
    if [[ -z "${decision_epoch[${trace_id}]:-}" ]] &&
        (( age >= STRATEGY_DECISION_TIMEOUT_SECONDS )); then
      write_incident "${trace_id}" "candidate_without_strategy_decision" \
        "signal_candidate has no strategy_decision after ${STRATEGY_DECISION_TIMEOUT_SECONDS}s"
    fi
  done
  for trace_id in "${!decision_epoch[@]}"; do
    [[ "${decision_disposition[${trace_id}]:-}" == "allowed" ]] || continue
    age=$((current_epoch - ${decision_epoch[${trace_id}]:-${current_epoch}}))
    if [[ -z "${disposition_epoch[${trace_id}]:-}" ]] &&
        (( age >= EXECUTION_DISPOSITION_TIMEOUT_SECONDS )); then
      write_incident "${trace_id}" "allowed_signal_without_execution_disposition" \
        "allowed strategy decision has no final execution disposition after ${EXECUTION_DISPOSITION_TIMEOUT_SECONDS}s"
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

file_age_seconds() {
  local file_path="$1"
  local modified_epoch
  [[ -f "${file_path}" ]] || { printf '%s\n' -1; return 0; }
  modified_epoch="$(stat -c '%Y' -- "${file_path}" 2>/dev/null || printf '0')"
  [[ "${modified_epoch}" =~ ^[0-9]+$ && "${modified_epoch}" != "0" ]] || {
    printf '%s\n' -1
    return 0
  }
  printf '%s\n' $(( $(now_epoch) - modified_epoch ))
}

latest_trading_day_dir() {
  find "${MARKET_DATA_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'trading_day=*' -print \
    2>/dev/null | sort -r | head -n 1
}

discover_products() {
  local day_dir="$1"
  local configured="${PRODUCTS}"
  if [[ -z "${configured}" && -f "${CONFIG_PATH}" ]]; then
    configured="$(grep -E '^[[:space:]]*product_ids:' "${CONFIG_PATH}" | head -n 1 | \
      sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//' | tr -d "\"'[:space:]")"
  fi
  if [[ -n "${configured}" ]]; then
    printf '%s\n' "${configured}"
    return 0
  fi
  if [[ -d "${day_dir}/varieties" ]]; then
    find "${day_dir}/varieties" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null |
      sort | paste -sd, -
  fi
}

json_string_value() {
  local key="$1"
  local file_path="$2"
  [[ -f "${file_path}" ]] || return 0
  sed -nE 's/.*"'"${key}"'"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p' \
    "${file_path}" | head -n 1
}

json_number_value() {
  local key="$1"
  local file_path="$2"
  [[ -f "${file_path}" ]] || return 0
  sed -nE 's/.*"'"${key}"'"[[:space:]]*:[[:space:]]*(-?[0-9]+([.][0-9]+)?).*/\1/p' \
    "${file_path}" | head -n 1
}

session_spec_for_product() {
  local product="$1"
  local exchange="$2"
  [[ -f "${TRADING_SESSIONS_CONFIG}" ]] || return 0
  awk -v wanted_product="${product}" -v wanted_exchange="${exchange}" '
    function trim(value) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^"|"$/, "", value)
      return value
    }
    function flush() {
      if (entry_exchange != wanted_exchange) return
      if (entry_prefix == wanted_product) exact = entry_day "|" entry_night
      else if (entry_prefix == "") fallback = entry_day "|" entry_night
    }
    /^[[:space:]]*-[[:space:]]*exchange:/ {
      flush()
      entry_exchange=$0; sub(/.*exchange:[[:space:]]*/, "", entry_exchange)
      entry_exchange=trim(entry_exchange); entry_prefix=""; entry_day=""; entry_night=""
      next
    }
    /^[[:space:]]*instrument_prefix:/ {
      entry_prefix=$0; sub(/.*instrument_prefix:[[:space:]]*/, "", entry_prefix)
      entry_prefix=trim(entry_prefix); next
    }
    /^[[:space:]]*day:/ {
      entry_day=$0; sub(/.*day:[[:space:]]*/, "", entry_day)
      entry_day=trim(entry_day); next
    }
    /^[[:space:]]*night:/ {
      entry_night=$0; sub(/.*night:[[:space:]]*/, "", entry_night)
      entry_night=trim(entry_night); if (entry_night == "null") entry_night=""; next
    }
    END { flush(); if (exact != "") print exact; else print fallback }
  ' "${TRADING_SESSIONS_CONFIG}"
}

clock_to_minute() {
  local clock_text="$1"
  local hour minute
  [[ "${clock_text}" =~ ^([0-9]{2}):([0-9]{2})$ ]] || return 1
  hour="${BASH_REMATCH[1]}"
  minute="${BASH_REMATCH[2]}"
  printf '%s\n' $((10#${hour} * 60 + 10#${minute}))
}

ranges_contain_now() {
  local ranges="$1"
  local day_of_week="$2"
  local now_minute="$3"
  local now_second="${4:-0}"
  local range start_text end_text start_minute end_minute
  [[ -n "${ranges}" ]] || return 1
  IFS=',' read -r -a range_items <<< "${ranges}"
  for range in "${range_items[@]}"; do
    start_text="${range%-*}"
    end_text="${range#*-}"
    start_minute="$(clock_to_minute "${start_text}" 2>/dev/null || true)"
    end_minute="$(clock_to_minute "${end_text}" 2>/dev/null || true)"
    [[ "${start_minute}" =~ ^[0-9]+$ && "${end_minute}" =~ ^[0-9]+$ ]] || continue
    if (( start_minute <= end_minute )); then
      if (( day_of_week >= 1 && day_of_week <= 5 && now_minute >= start_minute &&
            now_minute <= end_minute &&
            (now_minute < end_minute || now_second <= TICK_STALE_SECONDS) )); then
        return 0
      fi
    else
      if (( now_minute >= start_minute && day_of_week >= 1 && day_of_week <= 5 )); then
        return 0
      fi
      if (( now_minute <= end_minute && day_of_week >= 2 && day_of_week <= 6 &&
            (now_minute < end_minute || now_second <= TICK_STALE_SECONDS) )); then
        return 0
      fi
    fi
  done
  return 1
}

product_session_state() {
  local product="$1"
  local exchange="$2"
  local spec day_ranges night_ranges
  local current_text day_of_week clock_text now_minute now_second
  spec="$(session_spec_for_product "${product}" "${exchange}")"
  day_ranges="${spec%%|*}"
  night_ranges="${spec#*|}"
  [[ "${spec}" == *'|'* ]] || night_ranges=""
  current_text="${SIMNOW_MONITOR_FAKE_NOW:-now}"
  day_of_week="$(date -d "${current_text}" +%u 2>/dev/null || printf '0')"
  clock_text="$(date -d "${current_text}" +%H:%M 2>/dev/null || printf '00:00')"
  now_minute="$(clock_to_minute "${clock_text}" 2>/dev/null || printf '0')"
  now_second="$(date -d "${current_text}" +%S 2>/dev/null || printf '0')"
  now_second=$((10#${now_second}))
  if ranges_contain_now "${day_ranges}" "${day_of_week}" "${now_minute}" "${now_second}"; then
    printf '%s\n' day
  elif ranges_contain_now "${night_ranges}" "${day_of_week}" "${now_minute}" "${now_second}"; then
    printf '%s\n' night
  else
    printf '%s\n' closed
  fi
}

worse_status() {
  local current="$1"
  local candidate="$2"
  local current_rank=0
  local candidate_rank=0
  case "${current}" in unhealthy) current_rank=4 ;; degraded) current_rank=3 ;; unknown) current_rank=2 ;; healthy) current_rank=1 ;; inactive) current_rank=0 ;; esac
  case "${candidate}" in unhealthy) candidate_rank=4 ;; degraded) candidate_rank=3 ;; unknown) candidate_rank=2 ;; healthy) candidate_rank=1 ;; inactive) candidate_rank=0 ;; esac
  if (( candidate_rank > current_rank )); then printf '%s\n' "${candidate}"; else printf '%s\n' "${current}"; fi
}

last_tick_summary() {
  local file_path="$1"
  local instrument_id="$2"
  local cutoff_ns
  [[ -f "${file_path}" ]] || return 0
  cutoff_ns="$(( $(now_epoch) - 900 ))000000000"
  awk -F, -v wanted="${instrument_id}" -v cutoff_ns="${cutoff_ns}" '
    NR == 1 { for (i=1; i<=NF; ++i) h[$i]=i; next }
    $(h["instrument_id"]) == "instrument_id" { next }
    wanted == "" || $(h["instrument_id"]) == wanted {
      instrument=$(h["instrument_id"]); exchange=$(h["exchange_id"])
      day=$(h["trading_day"]); action=$(h["action_day"]); update=$(h["update_time"])
      bid=$(h["bid_price_1"]); ask=$(h["ask_price_1"])
      exchange_ns=h["exchange_ts_ns"] ? $(h["exchange_ts_ns"]) : 0
      recv_ns=h["recv_ts_ns"] ? $(h["recv_ts_ns"]) : 0
      volume=h["volume"] ? $(h["volume"]) : 0
      last=h["last_price"] ? $(h["last_price"]) : 0
      open_interest=h["open_interest"] ? $(h["open_interest"]) : 0
      recent=(recv_ns < 1000000000000000 || recv_ns >= cutoff_ns)
      if (recent) {
        if (previous_exchange_ns > 0 && exchange_ns > 0 && exchange_ns < previous_exchange_ns) {
          reversals++
        }
        if (max_exchange_ns > 0 && exchange_ns > 0 && max_exchange_ns - exchange_ns > 3500000000) {
          late++
        }
        fingerprint=exchange_ns "|" last "|" bid "|" ask "|" volume "|" open_interest
        if (++payload_seen[fingerprint] > 1) duplicates++
        if (previous_exchange_ns > 0 && exchange_ns >= previous_exchange_ns &&
            previous_day == day && volume < previous_volume) volume_regressions++
        if (!(bid > 0 && ask > 0 && bid <= ask)) invalid_books++
        previous_exchange_ns=exchange_ns
        previous_volume=volume
        previous_day=day
        if (exchange_ns > max_exchange_ns) max_exchange_ns=exchange_ns
      }
    }
    END { if (instrument != "") print instrument "\t" exchange "\t" day "\t" action "\t" update "\t" bid "\t" ask "\t" exchange_ns "\t" recv_ns "\t" reversals+0 "\t" late+0 "\t" duplicates+0 "\t" volume_regressions+0 "\t" invalid_books+0 }
  ' < <({ head -n 1 -- "${file_path}"; tail -n 5000 -- "${file_path}"; })
}

bar_file_summary() {
  local file_path="$1"
  local instrument_id="$2"
  [[ -f "${file_path}" ]] || return 0
  awk -F, -v wanted="${instrument_id}" '
    NR == 1 {
      for (i=1; i<=NF; ++i) h[$i]=i
      schema=(h["is_complete"] && h["strategy_eligible"] && h["has_conflict"]) ? "v2" : "legacy"
      next
    }
    wanted == "" || $(h["instrument_id"]) == wanted {
      rows++
      instrument=$(h["instrument_id"]); minute=$(h["minute"])
      key=instrument "|" minute
      if (++seen[key] > 1) duplicates++
      open_price=$(h["open"])+0; high_price=$(h["high"])+0
      low_price=$(h["low"])+0; close_price=$(h["close"])+0
      if (low_price > high_price || open_price < low_price || open_price > high_price ||
          close_price < low_price || close_price > high_price) invalid++
      complete=schema == "v2" ? $(h["is_complete"])+0 : 1
      eligible=schema == "v2" ? $(h["strategy_eligible"])+0 : 1
      conflict=schema == "v2" ? $(h["has_conflict"])+0 : 0
      replay=(schema == "v2" && h["is_recovery_replay"]) ? $(h["is_recovery_replay"])+0 : 0
      endpoint=(schema == "v2" && h["is_session_endpoint"]) ? $(h["is_session_endpoint"])+0 : 0
      expected=(schema == "v2" && h["expected_source_bars"]) ? $(h["expected_source_bars"])+0 : 0
      observed=(schema == "v2" && h["observed_source_bars"]) ? $(h["observed_source_bars"])+0 : 0
      if (!complete) incomplete++
      if (conflict) conflicts++
      if ((endpoint || replay || !complete || conflict) && eligible) invalid_eligible++
      if (complete && expected > 0 && observed != expected) source_mismatch++
      previous_complete=last_complete; last_complete=complete
      last_eligible=eligible; last_conflict=conflict; last_replay=replay; last_endpoint=endpoint
      last_period_end=(schema == "v2" && h["period_end_ts_ns"]) ? $(h["period_end_ts_ns"]) : 0
      last_finalized=(schema == "v2" && h["finalized_ts_ns"]) ? $(h["finalized_ts_ns"]) : 0
    }
    END {
      recovered=(rows >= 2 && previous_complete && last_complete) ? 1 : 0
      if (rows > 0) print schema "\t" rows+0 "\t" duplicates+0 "\t" conflicts+0 "\t" incomplete+0 "\t" invalid+0 "\t" invalid_eligible+0 "\t" source_mismatch+0 "\t" minute "\t" last_complete+0 "\t" last_eligible+0 "\t" last_conflict+0 "\t" last_replay+0 "\t" last_endpoint+0 "\t" last_period_end+0 "\t" last_finalized+0 "\t" recovered+0
    }
  ' "${file_path}"
}

collect_bar_latency_samples() {
  local file_path="$1"
  local instrument_id="$2"
  local cutoff_ns="$3"
  [[ -f "${file_path}" ]] || return 0
  awk -F, -v wanted="${instrument_id}" -v cutoff_ns="${cutoff_ns}" '
    NR == 1 {
      for (i=1; i<=NF; ++i) h[$i]=i
      next
    }
    h["ts_ns"] && h["period_end_ts_ns"] && h["finalized_ts_ns"] &&
      (wanted == "" || $(h["instrument_id"]) == wanted) {
      event_ns=$(h["ts_ns"]); period_end=$(h["period_end_ts_ns"])
      finalized=$(h["finalized_ts_ns"])
      if (finalized >= cutoff_ns && finalized >= period_end && period_end > 0) {
        invalid=0
        if (h["is_complete"] && $(h["is_complete"])+0 != 1) invalid=1
        if (h["strategy_eligible"] && $(h["strategy_eligible"])+0 != 1) invalid=1
        if (h["is_session_endpoint"] && $(h["is_session_endpoint"])+0 == 1) invalid=1
        if (h["is_recovery_replay"] && $(h["is_recovery_replay"])+0 == 1) invalid=1
        if (h["has_conflict"] && $(h["has_conflict"])+0 == 1) invalid=1
        print event_ns "\t" finalized "\t" int((finalized-period_end)/1000000) "\t" invalid
      }
    }
  ' "${file_path}"
}

percentile_triplet() {
  local -a samples=("$@")
  local count=${#samples[@]}
  local sorted index50 index95 index99
  if (( count == 0 )); then
    printf '%s\n' '-1 -1 -1'
    return 0
  fi
  sorted="$(printf '%s\n' "${samples[@]}" | sort -n | paste -sd' ' -)"
  read -r -a samples <<< "${sorted}"
  index50=$(( (count * 50 + 99) / 100 - 1 ))
  index95=$(( (count * 95 + 99) / 100 - 1 ))
  index99=$(( (count * 99 + 99) / 100 - 1 ))
  printf '%s %s %s\n' "${samples[${index50}]}" "${samples[${index95}]}" \
    "${samples[${index99}]}"
}

last_strategy_summary() {
  local file_path="$1"
  local instrument_id="$2"
  [[ -f "${file_path}" ]] || return 0
  awk -F, -v wanted="${instrument_id}" '
    NR == 1 { for (i=1; i<=NF; ++i) h[$i]=i; next }
    wanted == "" || $(h["instrument"]) == wanted {
      rows++; minute=$(h["minute"]); raw=$(h["raw_signal"]); blocked=$(h["blocked_reason"])
      if (raw != "") candidates++
      if (raw != "" && blocked == "none") allowed++
    }
    END { if (rows > 0) print minute "\t" rows "\t" candidates "\t" allowed }
  ' "${file_path}"
}

update_stage_transition() {
  local stage="$1"
  local status="$2"
  local reason="$3"
  local previous="${previous_stage_status[${stage}]:-unknown}"
  local previous_reason="${previous_stage_reason[${stage}]:-}"
  local current_epoch
  current_epoch="$(now_epoch)"
  if [[ "${status}" != "${previous}" || "${reason}" != "${previous_reason}" ]]; then
    pipeline_last_change_epoch="${current_epoch}"
    write_event "pipeline_stage_transition" "" "${stage}: ${previous} -> ${status}" \
      "\"stage\":\"$(json_escape "${stage}")\",\"previous_status\":\"$(json_escape "${previous}")\",\"status\":\"$(json_escape "${status}")\",\"reason\":\"$(json_escape "${reason}")\""
    if [[ "${status}" == "unhealthy" || "${status}" == "degraded" ]]; then
      if (( current_epoch - ${last_stage_alert_epoch[${stage}]:-0} >= ALERT_COOLDOWN_SECONDS )); then
        send_alert "$([[ "${status}" == "unhealthy" ]] && printf critical || printf warn)" \
          "SimNow pipeline ${stage} ${status}: ${reason}"
        last_stage_alert_epoch["${stage}"]="${current_epoch}"
      fi
    elif [[ "${previous}" == "unhealthy" || "${previous}" == "degraded" ]]; then
      recent_recovery_events+=("$(now_iso) ${stage}: ${previous} -> ${status}")
      if (( ${#recent_recovery_events[@]} > 20 )); then
        recent_recovery_events=("${recent_recovery_events[@]: -20}")
      fi
      send_alert info "SimNow pipeline ${stage} recovered: ${status}"
    fi
  fi
  previous_stage_status["${stage}"]="${status}"
  previous_stage_reason["${stage}"]="${reason}"
}

collect_pipeline_health() {
  local day_dir products_csv
  local product dominant_path instrument exchange session_state
  local tick_file bar_1m_file bar_5m_file strategy_file tick_summary
  local tick_instrument _tick_day _tick_action _tick_time bid ask exchange_ns recv_ns tick_age delay_ms
  local tick_reversals tick_late tick_duplicates tick_volume_regressions tick_invalid_books
  local summary_1m summary_5m strategy_summary
  local schema_1m _rows_1m duplicates_1m conflicts_1m incomplete_1m invalid_1m invalid_eligible_1m mismatch_1m minute_1m complete_1m _eligible_1m _conflict_1m _replay_1m _endpoint_1m _period_end_1m _finalized_1m _recovered_1m
  local schema_5m _rows_5m duplicates_5m conflicts_5m incomplete_5m invalid_5m invalid_eligible_5m mismatch_5m minute_5m complete_5m eligible_5m _conflict_5m replay_5m endpoint_5m _period_end_5m _finalized_5m recovered_5m
  local strategy_minute strategy_rows strategy_candidates strategy_allowed
  local market_status bar1_status bar5_status strategy_status current_product_status
  local market_reason bar1_reason bar5_reason strategy_reason
  local active_products=0 total_products=0 bar_age strategy_age
  local eligible_count baseline_count pending_count trace_id
  local execution_incidents=0 execution_rejected=0 execution_active=0 execution_filled=0
  local execution_stale_active=0
  local wal_integrity wal_duplicate_trades wal_unresolved_trades
  local cutoff_ns event_key finalized_ns latency_ms invalid_for_strategy
  local recent_duplicate_tick_total=0 recent_late_tick_total=0

  pipeline_warning_count=0
  pipeline_critical_count=0
  pipeline_duplicate_bars=0
  pipeline_conflict_bars=0
  pipeline_incomplete_bars=0
  pipeline_missing_strategy_evaluations=0
  pipeline_duplicate_dispositions=0
  pipeline_unresolved_traces=0
  pipeline_strategy_trace_integrity_failures=0
  pipeline_tick_time_reversals=0
  pipeline_volume_regressions=0
  bar_finalize_samples_ms=()
  bar_to_decision_samples_ms=()
  candidate_to_disposition_samples_ms=()
  ctp_to_callback_samples_ms=()
  for event_key in "${!bar_finalized_by_event_ts_ns[@]}"; do
    unset "bar_finalized_by_event_ts_ns[${event_key}]"
  done
  for event_key in "${!bar_strategy_invalid_by_event_ts_ns[@]}"; do
    unset "bar_strategy_invalid_by_event_ts_ns[${event_key}]"
  done
  cutoff_ns="$(( $(now_epoch) - 900 ))000000000"
  stage_market_status="inactive"; stage_market_reason="outside_trading_session"
  stage_bar_1m_status="inactive"; stage_bar_1m_reason="outside_trading_session"
  stage_bar_5m_status="inactive"; stage_bar_5m_reason="outside_trading_session"
  stage_strategy_status="inactive"; stage_strategy_reason="outside_trading_session"
  stage_execution_status="healthy"; stage_execution_reason="no_unresolved_execution"
  pipeline_session="closed"

  day_dir="$(latest_trading_day_dir || true)"
  pipeline_trading_day="${day_dir##*/trading_day=}"
  [[ -n "${day_dir}" && "${pipeline_trading_day}" != "${day_dir}" ]] || pipeline_trading_day=""
  products_csv="$(discover_products "${day_dir}" || true)"
  products_csv="${products_csv// /}"
  monitored_products_csv="${products_csv}"
  IFS=',' read -r -a monitored_products <<< "${products_csv}"

  for product in "${monitored_products[@]}"; do
    [[ -n "${product}" ]] || continue
    total_products=$((total_products + 1))
    dominant_path="${QUANT_ROOT}/runtime/ctp_instruments/${product}_dominant_contract.json"
    instrument="$(json_string_value current_instrument_id "${dominant_path}")"
    [[ -n "${instrument}" ]] || instrument="$(json_string_value instrument_id "${dominant_path}")"
    exchange="$(json_string_value exchange_id "${dominant_path}")"
    tick_file="${day_dir}/varieties/${product}/market/ticks.csv"
    bar_1m_file="${day_dir}/varieties/${product}/market/bars_1m.csv"
    bar_5m_file="${day_dir}/varieties/${product}/market/bars_5m.csv"
    strategy_file="${day_dir}/varieties/${product}/strategy/kama_5m.csv"

    tick_summary="$(last_tick_summary "${tick_file}" "${instrument}" || true)"
    IFS=$'\t' read -r tick_instrument tick_exchange _tick_day _tick_action _tick_time bid ask exchange_ns recv_ns tick_reversals tick_late tick_duplicates tick_volume_regressions tick_invalid_books <<< "${tick_summary}"
    for numeric_name in tick_reversals tick_late tick_duplicates tick_volume_regressions tick_invalid_books; do
      [[ "${!numeric_name:-}" =~ ^[0-9]+$ ]] || printf -v "${numeric_name}" '%s' 0
    done
    pipeline_tick_time_reversals=$((pipeline_tick_time_reversals + tick_reversals))
    pipeline_volume_regressions=$((pipeline_volume_regressions + tick_volume_regressions))
    recent_duplicate_tick_total=$((recent_duplicate_tick_total + tick_duplicates))
    recent_late_tick_total=$((recent_late_tick_total + tick_late))
    [[ -n "${instrument}" ]] || instrument="${tick_instrument:-}"
    [[ -n "${exchange}" ]] || exchange="${tick_exchange:-}"
    session_state="$(product_session_state "${product}" "${exchange}")"
    if [[ "${session_state}" != "closed" ]]; then
      active_products=$((active_products + 1))
      pipeline_session="${session_state}"
    fi

    tick_age="$(file_age_seconds "${tick_file}")"
    if [[ "${recv_ns:-0}" =~ ^[0-9]{16,}$ ]]; then
      tick_age=$(( $(now_epoch) - ${recv_ns:0:${#recv_ns}-9} ))
      (( tick_age >= 0 )) || tick_age=0
    fi
    delay_ms=-1
    if [[ "${exchange_ns:-0}" =~ ^[0-9]{16,}$ && "${recv_ns:-0}" =~ ^[0-9]{16,}$ ]] &&
        (( recv_ns >= exchange_ns )); then
      delay_ms=$(( (recv_ns - exchange_ns) / 1000000 ))
    fi

    market_status="healthy"; market_reason="tick_fresh"
    if [[ "${session_state}" == "closed" ]]; then
      market_status="inactive"; market_reason="outside_trading_session"
    elif [[ "${last_core_state}" != "running" ]]; then
      market_status="unhealthy"; market_reason="core_engine_not_running"
    elif [[ -z "${tick_summary}" ]]; then
      market_status="unhealthy"; market_reason="dominant_tick_missing"
    elif (( tick_age > TICK_STALE_SECONDS )); then
      market_status="unhealthy"; market_reason="dominant_tick_stale"
    elif ! awk -v bid="${bid:-0}" -v ask="${ask:-0}" 'BEGIN { exit !(bid > 0 && ask > 0 && bid <= ask) }'; then
      market_status="unhealthy"; market_reason="invalid_top_of_book"
    elif (( delay_ms > 5000 )); then
      market_status="degraded"; market_reason="market_event_delay_high"
    fi
    if (( tick_late > 0 )); then
      if [[ ",${core_readiness_suppressed_instruments}," == *",${instrument},"* ]]; then
        market_status="unhealthy"; market_reason="late_tick_open_suppressed"
      else
        market_status="$(worse_status "${market_status}" degraded)"
        market_reason="late_tick_after_watermark"
      fi
    elif (( tick_duplicates > 0 )); then
      market_status="$(worse_status "${market_status}" degraded)"
      market_reason="duplicate_tick_payload"
    elif (( tick_volume_regressions > 0 )); then
      market_status="$(worse_status "${market_status}" degraded)"
      market_reason="cumulative_volume_regression"
    elif (( tick_invalid_books > 0 )); then
      market_status="$(worse_status "${market_status}" degraded)"
      market_reason="recent_invalid_top_of_book"
    fi
    eligible_count="$(json_number_value eligible_count "${dominant_path}")"
    baseline_count="$(json_number_value baseline_count "${dominant_path}")"
    [[ "${eligible_count}" =~ ^[0-9]+$ ]] || eligible_count=0
    [[ "${baseline_count}" =~ ^[0-9]+$ ]] || baseline_count=0
    if (( eligible_count > 0 && baseline_count < eligible_count )); then
      market_status="$(worse_status "${market_status}" degraded)"
      market_reason="candidate_tick_baseline_incomplete"
    fi

    summary_1m="$(bar_file_summary "${bar_1m_file}" "${instrument}" || true)"
    IFS=$'\t' read -r schema_1m _rows_1m duplicates_1m conflicts_1m incomplete_1m invalid_1m invalid_eligible_1m mismatch_1m minute_1m complete_1m _eligible_1m _conflict_1m _replay_1m _endpoint_1m _period_end_1m _finalized_1m _recovered_1m <<< "${summary_1m}"
    summary_5m="$(bar_file_summary "${bar_5m_file}" "${instrument}" || true)"
    IFS=$'\t' read -r schema_5m _rows_5m duplicates_5m conflicts_5m incomplete_5m invalid_5m invalid_eligible_5m mismatch_5m minute_5m complete_5m eligible_5m _conflict_5m replay_5m endpoint_5m _period_end_5m _finalized_5m recovered_5m <<< "${summary_5m}"
    for numeric_name in duplicates_1m conflicts_1m incomplete_1m invalid_1m invalid_eligible_1m mismatch_1m duplicates_5m conflicts_5m incomplete_5m invalid_5m invalid_eligible_5m mismatch_5m; do
      [[ "${!numeric_name:-}" =~ ^[0-9]+$ ]] || printf -v "${numeric_name}" '%s' 0
    done
    pipeline_duplicate_bars=$((pipeline_duplicate_bars + duplicates_1m + duplicates_5m))
    pipeline_conflict_bars=$((pipeline_conflict_bars + conflicts_1m + conflicts_5m))
    pipeline_incomplete_bars=$((pipeline_incomplete_bars + incomplete_1m + incomplete_5m))
    while IFS=$'\t' read -r event_key finalized_ns latency_ms invalid_for_strategy; do
      [[ "${event_key}" =~ ^[0-9]+$ && "${finalized_ns}" =~ ^[0-9]+$ && "${latency_ms}" =~ ^[0-9]+$ ]] || continue
      bar_finalized_by_event_ts_ns["${event_key}"]="${finalized_ns}"
      bar_strategy_invalid_by_event_ts_ns["${event_key}"]="${invalid_for_strategy:-0}"
      bar_finalize_samples_ms+=("${latency_ms}")
    done < <(collect_bar_latency_samples "${bar_5m_file}" "${instrument}" "${cutoff_ns}")

    bar1_status="healthy"; bar1_reason="canonical_1m_complete"
    bar5_status="healthy"; bar5_reason="canonical_5m_complete"
    if [[ "${session_state}" == "closed" ]]; then
      bar1_status="inactive"; bar1_reason="outside_trading_session"
      bar5_status="inactive"; bar5_reason="outside_trading_session"
    elif [[ -z "${summary_1m}" ]]; then
      bar1_status="unhealthy"; bar1_reason="bar_1m_missing"
    elif (( duplicates_1m > 0 || conflicts_1m > 0 || invalid_1m > 0 || invalid_eligible_1m > 0 || mismatch_1m > 0 )); then
      bar1_status="unhealthy"; bar1_reason="bar_1m_integrity_failure"
    elif [[ "${complete_1m:-1}" != "1" ]]; then
      bar1_status="unhealthy"; bar1_reason="latest_bar_1m_incomplete"
    elif [[ "${schema_1m}" == "legacy" ]]; then
      bar1_status="degraded"; bar1_reason="bar_1m_schema_legacy"
    fi
    if [[ "${session_state}" != "closed" && -n "${summary_1m}" ]]; then
      bar_age="$(file_age_seconds "${bar_1m_file}")"
      if (( bar_age > 60 + BAR_CRITICAL_SECONDS )); then
        bar1_status="unhealthy"; bar1_reason="bar_1m_publish_overdue"
      elif (( bar_age > 60 + BAR_WARN_SECONDS )); then
        bar1_status="$(worse_status "${bar1_status}" degraded)"; bar1_reason="bar_1m_publish_delayed"
      fi
    fi
    if [[ "${session_state}" == "closed" ]]; then
      :
    elif [[ -z "${summary_5m}" ]]; then
      bar5_status="unhealthy"; bar5_reason="bar_5m_missing"
    elif (( duplicates_5m > 0 || conflicts_5m > 0 || invalid_5m > 0 || invalid_eligible_5m > 0 || mismatch_5m > 0 )); then
      bar5_status="unhealthy"; bar5_reason="bar_5m_integrity_failure"
    elif [[ "${complete_5m:-1}" != "1" && "${recovered_5m:-0}" != "1" ]]; then
      bar5_status="unhealthy"; bar5_reason="latest_bar_5m_incomplete"
    elif (( incomplete_5m > 0 )) && [[ "${recovered_5m:-0}" != "1" ]]; then
      bar5_status="unhealthy"; bar5_reason="bar_5m_recovery_pending"
    elif [[ "${schema_5m}" == "legacy" ]]; then
      bar5_status="degraded"; bar5_reason="bar_5m_schema_legacy"
    fi
    if [[ "${session_state}" != "closed" && -n "${summary_5m}" ]]; then
      bar_age="$(file_age_seconds "${bar_5m_file}")"
      if (( bar_age > 300 + BAR_CRITICAL_SECONDS )); then
        bar5_status="unhealthy"; bar5_reason="bar_5m_publish_overdue"
      elif (( bar_age > 300 + BAR_WARN_SECONDS )); then
        bar5_status="$(worse_status "${bar5_status}" degraded)"; bar5_reason="bar_5m_publish_delayed"
      fi
    fi

    strategy_summary="$(last_strategy_summary "${strategy_file}" "${instrument}" || true)"
    IFS=$'\t' read -r strategy_minute strategy_rows strategy_candidates strategy_allowed <<< "${strategy_summary}"
    [[ "${strategy_rows:-}" =~ ^[0-9]+$ ]] || strategy_rows=0
    [[ "${strategy_candidates:-}" =~ ^[0-9]+$ ]] || strategy_candidates=0
    [[ "${strategy_allowed:-}" =~ ^[0-9]+$ ]] || strategy_allowed=0
    strategy_status="healthy"; strategy_reason="eligible_bars_evaluated"
    if [[ "${session_state}" == "closed" ]]; then
      strategy_status="inactive"; strategy_reason="outside_trading_session"
    elif [[ "${eligible_5m:-0}" == "1" && "${endpoint_5m:-0}" != "1" && "${replay_5m:-0}" != "1" ]]; then
      strategy_age="$(file_age_seconds "${bar_5m_file}")"
      if [[ -z "${strategy_summary}" || "${strategy_minute}" != "${minute_5m}" ]]; then
        if (( strategy_age > STRATEGY_DECISION_TIMEOUT_SECONDS )); then
          strategy_status="unhealthy"; strategy_reason="eligible_5m_missing_strategy_evaluation"
          pipeline_missing_strategy_evaluations=$((pipeline_missing_strategy_evaluations + 1))
        else
          strategy_status="unknown"; strategy_reason="strategy_evaluation_pending"
        fi
      fi
    fi

    pending_count=0
    for trace_id in "${!signal_seen_epoch[@]}"; do
      [[ "${signal_instrument[${trace_id}]:-}" == "${instrument}" ]] || continue
      case "${signal_status[${trace_id}]:-}" in filled|rejected|canceled|incident) ;; *) pending_count=$((pending_count + 1)) ;; esac
    done
    current_product_status="inactive"
    current_product_status="$(worse_status "${current_product_status}" "${market_status}")"
    current_product_status="$(worse_status "${current_product_status}" "${bar1_status}")"
    current_product_status="$(worse_status "${current_product_status}" "${bar5_status}")"
    current_product_status="$(worse_status "${current_product_status}" "${strategy_status}")"
    product_status["${product}"]="${current_product_status}"
    product_instrument["${product}"]="${instrument:-unknown}"
    product_exchange["${product}"]="${exchange:-unknown}"
    product_tick_age["${product}"]="${tick_age:-1}"
    product_tick_delay_ms["${product}"]="${delay_ms:-1}"
    product_tick_reversals["${product}"]="${tick_reversals}"
    product_duplicate_ticks["${product}"]="${tick_duplicates}"
    product_volume_regressions["${product}"]="${tick_volume_regressions}"
    product_last_1m["${product}"]="${minute_1m:-}"
    product_last_5m["${product}"]="${minute_5m:-}"
    product_bar_complete["${product}"]="${complete_5m:-0}"
    product_strategy_minute["${product}"]="${strategy_minute:-}"
    product_strategy_evaluations["${product}"]="${strategy_rows}"
    product_candidates["${product}"]="${strategy_candidates}"
    product_allowed["${product}"]="${strategy_allowed}"
    product_pending["${product}"]="${pending_count}"
    product_reason["${product}"]="${market_reason};${bar1_reason};${bar5_reason};${strategy_reason}"
    product_schema["${product}"]="1m:${schema_1m:-missing},5m:${schema_5m:-missing}"

    stage_market_status="$(worse_status "${stage_market_status}" "${market_status}")"
    stage_bar_1m_status="$(worse_status "${stage_bar_1m_status}" "${bar1_status}")"
    stage_bar_5m_status="$(worse_status "${stage_bar_5m_status}" "${bar5_status}")"
    stage_strategy_status="$(worse_status "${stage_strategy_status}" "${strategy_status}")"
    [[ "${market_status}" == "healthy" ]] || stage_market_reason="${product}:${market_reason}"
    [[ "${bar1_status}" == "healthy" ]] || stage_bar_1m_reason="${product}:${bar1_reason}"
    [[ "${bar5_status}" == "healthy" ]] || stage_bar_5m_reason="${product}:${bar5_reason}"
    [[ "${strategy_status}" == "healthy" ]] || stage_strategy_reason="${product}:${strategy_reason}"
  done
  (( recent_duplicate_tick_total <= pipeline_duplicate_ticks )) || \
    pipeline_duplicate_ticks="${recent_duplicate_tick_total}"
  (( recent_late_tick_total <= pipeline_late_ticks )) || pipeline_late_ticks="${recent_late_tick_total}"
  pipeline_recent_duplicate_ticks="${recent_duplicate_tick_total}"
  pipeline_recent_late_ticks="${recent_late_tick_total}"

  for trace_id in "${!decision_ts_ns[@]}"; do
    event_key="${decision_event_ts_ns[${trace_id}]:-}"
    finalized_ns="${bar_finalized_by_event_ts_ns[${event_key}]:-}"
    if [[ "${decision_ts_ns[${trace_id}]:-}" =~ ^[0-9]+$ && "${finalized_ns}" =~ ^[0-9]+$ ]] &&
        (( decision_ts_ns[${trace_id}] >= cutoff_ns && decision_ts_ns[${trace_id}] >= finalized_ns )); then
      bar_to_decision_samples_ms+=("$(( (decision_ts_ns[${trace_id}] - finalized_ns) / 1000000 ))")
    fi
  done
  for trace_id in "${!disposition_ts_ns[@]}"; do
    if [[ "${candidate_ts_ns[${trace_id}]:-}" =~ ^[0-9]+$ && "${disposition_ts_ns[${trace_id}]:-}" =~ ^[0-9]+$ ]] &&
        (( disposition_ts_ns[${trace_id}] >= cutoff_ns && disposition_ts_ns[${trace_id}] >= candidate_ts_ns[${trace_id}] )); then
      candidate_to_disposition_samples_ms+=("$(( (disposition_ts_ns[${trace_id}] - candidate_ts_ns[${trace_id}]) / 1000000 ))")
    fi
  done
  for trace_id in "${!signal_callback_ts_ns[@]}"; do
    if [[ "${signal_ctp_ts_ns[${trace_id}]:-}" =~ ^[0-9]+$ && "${signal_callback_ts_ns[${trace_id}]:-}" =~ ^[0-9]+$ ]] &&
        (( signal_callback_ts_ns[${trace_id}] >= cutoff_ns && signal_callback_ts_ns[${trace_id}] >= signal_ctp_ts_ns[${trace_id}] )); then
      ctp_to_callback_samples_ms+=("$(( (signal_callback_ts_ns[${trace_id}] - signal_ctp_ts_ns[${trace_id}]) / 1000000 ))")
    fi
  done

  for trace_id in "${!candidate_epoch[@]}"; do
    if (( ${candidate_count[${trace_id}]:-0} > 1 || ${decision_count[${trace_id}]:-0} > 1 )); then
      pipeline_strategy_trace_integrity_failures=$((pipeline_strategy_trace_integrity_failures + 1))
      write_incident "${trace_id}" "duplicate_candidate_or_gate_result" \
        "candidate_count=${candidate_count[${trace_id}]:-0} decision_count=${decision_count[${trace_id}]:-0}"
      continue
    fi
    event_key="${candidate_event_ts_ns[${trace_id}]:-}"
    if [[ "${candidate_ts_ns[${trace_id}]:-}" =~ ^[0-9]+$ ]] &&
        (( candidate_ts_ns[${trace_id}] >= cutoff_ns )); then
      if [[ -z "${bar_finalized_by_event_ts_ns[${event_key}]:-}" ]]; then
        pipeline_strategy_trace_integrity_failures=$((pipeline_strategy_trace_integrity_failures + 1))
        write_incident "${trace_id}" "candidate_without_canonical_5m_bar" \
          "event_ts_ns=${event_key}"
      elif [[ "${bar_strategy_invalid_by_event_ts_ns[${event_key}]:-0}" == "1" ]]; then
        pipeline_strategy_trace_integrity_failures=$((pipeline_strategy_trace_integrity_failures + 1))
        write_incident "${trace_id}" "ineligible_5m_bar_signal_penetration" \
          "event_ts_ns=${event_key}"
      fi
    fi
  done

  if (( active_products > 0 )); then
    [[ "${stage_market_status}" != "inactive" ]] || { stage_market_status="unknown"; stage_market_reason="no_products_discovered"; }
    [[ "${stage_bar_1m_status}" != "inactive" ]] || { stage_bar_1m_status="unknown"; stage_bar_1m_reason="no_products_discovered"; }
    [[ "${stage_bar_5m_status}" != "inactive" ]] || { stage_bar_5m_status="unknown"; stage_bar_5m_reason="no_products_discovered"; }
    [[ "${stage_strategy_status}" != "inactive" ]] || { stage_strategy_status="unknown"; stage_strategy_reason="no_products_discovered"; }
  fi
  [[ "${stage_market_status}" != "healthy" ]] || stage_market_reason="all_dominant_ticks_fresh"
  [[ "${stage_bar_1m_status}" != "healthy" ]] || stage_bar_1m_reason="all_canonical_1m_complete"
  [[ "${stage_bar_5m_status}" != "healthy" ]] || stage_bar_5m_reason="all_canonical_5m_complete"
  [[ "${stage_strategy_status}" != "healthy" ]] || stage_strategy_reason="all_eligible_5m_evaluated"
  if (( pipeline_strategy_trace_integrity_failures > 0 )); then
    stage_strategy_status="unhealthy"
    stage_strategy_reason="candidate_gate_or_bar_eligibility_failure"
  fi

  if [[ "${last_core_state}" == "running" ]]; then
    stage_runtime_status="healthy"; stage_runtime_reason="core_and_readiness_fresh"
    if [[ "${core_readiness_state}" != "fresh" ]]; then
      stage_runtime_status="unhealthy"; stage_runtime_reason="readiness_${core_readiness_state}"
    elif [[ "${core_readiness_mode}" == "Ready" &&
            ( "${core_readiness_recovery_complete}" != "true" ||
              "${core_readiness_trader_ready}" != "true" ||
              "${core_readiness_gateway_healthy}" != "true" ||
              "${core_readiness_settlement_confirmed}" != "true" ) ]]; then
      stage_runtime_status="unhealthy"; stage_runtime_reason="ready_without_complete_recovery"
    elif (( core_readiness_unresolved_mapping_count > 0 )); then
      stage_runtime_status="unhealthy"; stage_runtime_reason="unresolved_order_trade_mapping"
    elif (( core_readiness_pending_exit_count > 0 )) && [[ "${core_readiness_mode}" == "Ready" ]]; then
      stage_runtime_status="unhealthy"; stage_runtime_reason="pending_exit_while_ready"
    elif (( core_readiness_pending_exit_count > 0 )); then
      stage_runtime_status="degraded"; stage_runtime_reason="pending_exit_waiting_for_execution"
    elif [[ "${core_readiness_mode}" != "Ready" ]]; then
      stage_runtime_status="degraded"; stage_runtime_reason="permission_${core_readiness_mode}"
    fi
  elif (( active_products > 0 )); then
    stage_runtime_status="unhealthy"; stage_runtime_reason="core_engine_stopped_in_trading_session"
  else
    stage_runtime_status="inactive"; stage_runtime_reason="outside_trading_session"
  fi

  for trace_id in "${!signal_seen_epoch[@]}"; do
    case "${signal_status[${trace_id}]:-}" in
      incident) execution_incidents=$((execution_incidents + 1)) ;;
      rejected) execution_rejected=$((execution_rejected + 1)) ;;
      filled) execution_filled=$((execution_filled + 1)) ;;
      canceled) ;;
      *)
        execution_active=$((execution_active + 1))
        [[ -z "${signal_fill_warning_written[${trace_id}]:-}" ]] || \
          execution_stale_active=$((execution_stale_active + 1))
        ;;
    esac
  done
  for trace_id in "${!disposition_count[@]}"; do
    if (( ${disposition_count[${trace_id}]:-0} > 1 )); then
      pipeline_duplicate_dispositions=$((pipeline_duplicate_dispositions + 1))
    fi
  done
  pipeline_unresolved_traces="${execution_active}"
  if (( execution_incidents > 0 || pipeline_duplicate_dispositions > 0 ||
        pipeline_generation_mismatch_submissions > 0 || core_readiness_unresolved_mapping_count > 0 )); then
    stage_execution_status="unhealthy"; stage_execution_reason="execution_incident_or_identity_failure"
  elif (( execution_stale_active > 0 )); then
    stage_execution_status="degraded"; stage_execution_reason="active_order_over_180_seconds"
  elif (( execution_active > 0 )); then
    stage_execution_status="healthy"; stage_execution_reason="execution_in_progress"
  elif (( execution_rejected > 0 )); then
    stage_execution_status="degraded"; stage_execution_reason="execution_rejection_terminal"
  elif (( active_products == 0 )); then
    stage_execution_status="inactive"; stage_execution_reason="outside_trading_session"
  fi

  wal_integrity="$(awk -v current_day="${pipeline_trading_day}" -v current_run="${current_session_key}" '
    /"event_type":"trade_fill"/ {
      if (current_run == "" || current_run == "none") next
      trade=""; day=""; inferred_day=""; exchange=""; account=""; run_id=""
      if (match($0, /"trade_id":"[^"]*"/)) { trade=substr($0,RSTART+12,RLENGTH-13) }
      if (match($0, /"trading_day":"[^"]*"/)) { day=substr($0,RSTART+15,RLENGTH-16) }
      if (match($0, /"exchange_id":"[^"]*"/)) { exchange=substr($0,RSTART+15,RLENGTH-16) }
      if (match($0, /"account_id":"[^"]*"/)) { account=substr($0,RSTART+14,RLENGTH-15) }
      if (match($0, /"run_id":"[^"]*"/)) { run_id=substr($0,RSTART+10,RLENGTH-11) }
      if (run_id != current_run) next
      if (match($0, /"run_id":"simnow-auto-[0-9]{8}/)) {
        run_token=substr($0,RSTART,RLENGTH)
        sub(/.*simnow-auto-/, "", run_token)
        inferred_day=substr(run_token,1,8)
      }
      event_day=(day != "" ? day : inferred_day)
      if (current_day != "" && event_day != current_day) next
      if (trade == "" || day == "" || exchange == "" || account == "") unresolved++
      else { key=account "|" day "|" exchange "|" trade; if (++seen[key] > 1) duplicate++ }
    }
    END { print duplicate+0 "\t" unresolved+0 }
  ' "${WAL_FILE}" 2>/dev/null || printf '0\t0')"
  IFS=$'\t' read -r wal_duplicate_trades wal_unresolved_trades <<< "${wal_integrity}"
  if (( wal_duplicate_trades > 0 || wal_unresolved_trades > 0 )); then
    stage_execution_status="unhealthy"; stage_execution_reason="wal_trade_identity_failure"
  fi

  pipeline_overall_status="inactive"
  for candidate_status in "${stage_runtime_status}" "${stage_market_status}" "${stage_bar_1m_status}" \
    "${stage_bar_5m_status}" "${stage_strategy_status}" "${stage_execution_status}"; do
    pipeline_overall_status="$(worse_status "${pipeline_overall_status}" "${candidate_status}")"
    [[ "${candidate_status}" == "unhealthy" ]] && pipeline_critical_count=$((pipeline_critical_count + 1))
    [[ "${candidate_status}" == "degraded" || "${candidate_status}" == "unknown" ]] && pipeline_warning_count=$((pipeline_warning_count + 1))
  done
  if (( active_products == 0 )) &&
      [[ "${stage_runtime_status}" == "healthy" || "${stage_runtime_status}" == "inactive" ]] &&
      [[ "${stage_execution_status}" == "inactive" ]]; then
    pipeline_overall_status="inactive"
  fi
  update_stage_transition runtime "${stage_runtime_status}" "${stage_runtime_reason}"
  update_stage_transition market_data "${stage_market_status}" "${stage_market_reason}"
  update_stage_transition bar_1m "${stage_bar_1m_status}" "${stage_bar_1m_reason}"
  update_stage_transition bar_5m "${stage_bar_5m_status}" "${stage_bar_5m_reason}"
  update_stage_transition strategy "${stage_strategy_status}" "${stage_strategy_reason}"
  update_stage_transition execution "${stage_execution_status}" "${stage_execution_reason}"
}

write_pipeline_health() {
  local tmp_file="${HEALTH_SNAPSHOT_FILE}.tmp.${BASHPID}"
  local generated_epoch generated_ns product trace_id
  local product_index=0 trace_index=0 cursor_count=0
  local cursor_index=0 recovery_index=0 tracked_file
  local bar_p50 bar_p95 bar_p99 decision_p50 decision_p95 decision_p99
  local disposition_p50 disposition_p95 disposition_p99 callback_p50 callback_p95 callback_p99
  generated_epoch="$(now_epoch)"
  generated_ns="${generated_epoch}000000000"
  read -r bar_p50 bar_p95 bar_p99 <<< "$(percentile_triplet "${bar_finalize_samples_ms[@]}")"
  read -r decision_p50 decision_p95 decision_p99 <<< "$(percentile_triplet "${bar_to_decision_samples_ms[@]}")"
  read -r disposition_p50 disposition_p95 disposition_p99 <<< "$(percentile_triplet "${candidate_to_disposition_samples_ms[@]}")"
  read -r callback_p50 callback_p95 callback_p99 <<< "$(percentile_triplet "${ctp_to_callback_samples_ms[@]}")"
  for _ in "${!file_offsets[@]}"; do cursor_count=$((cursor_count + 1)); done
  {
    printf '{\n'
    printf '  "schema_version": 3,\n'
    printf '  "generated_epoch": %s,\n' "${generated_epoch}"
    printf '  "generated_ts_ns": %s,\n' "${generated_ns}"
    printf '  "generated_at": "%s",\n' "$(json_escape "$(now_iso)")"
    printf '  "overall_status": "%s",\n' "$(json_escape "${pipeline_overall_status}")"
    printf '  "session": "%s",\n' "$(json_escape "${pipeline_session}")"
    printf '  "session_key": "%s",\n' "$(json_escape "${current_session_key}")"
    printf '  "trading_day": "%s",\n' "$(json_escape "${pipeline_trading_day}")"
    printf '  "generation": %s,\n' "${core_readiness_generation}"
    printf '  "readiness_mode": "%s",\n' "$(json_escape "${core_readiness_mode}")"
    printf '  "warning_count": %s,\n' "${pipeline_warning_count}"
    printf '  "critical_count": %s,\n' "${pipeline_critical_count}"
    printf '  "last_change_epoch": %s,\n' "${pipeline_last_change_epoch}"
    printf '  "runtime_status": "%s", "runtime_reason": "%s",\n' "${stage_runtime_status}" "$(json_escape "${stage_runtime_reason}")"
    printf '  "market_data_status": "%s", "market_data_reason": "%s",\n' "${stage_market_status}" "$(json_escape "${stage_market_reason}")"
    printf '  "bar_1m_status": "%s", "bar_1m_reason": "%s",\n' "${stage_bar_1m_status}" "$(json_escape "${stage_bar_1m_reason}")"
    printf '  "bar_5m_status": "%s", "bar_5m_reason": "%s",\n' "${stage_bar_5m_status}" "$(json_escape "${stage_bar_5m_reason}")"
    printf '  "strategy_status": "%s", "strategy_reason": "%s",\n' "${stage_strategy_status}" "$(json_escape "${stage_strategy_reason}")"
    printf '  "execution_status": "%s", "execution_reason": "%s",\n' "${stage_execution_status}" "$(json_escape "${stage_execution_reason}")"
    printf '  "pending_exit_count": %s, "unresolved_mapping_count": %s,\n' "${core_readiness_pending_exit_count}" "${core_readiness_unresolved_mapping_count}"
    printf '  "late_tick_count": %s, "duplicate_tick_count": %s, "tick_time_reversal_count": %s,\n' "${pipeline_late_ticks}" "${pipeline_duplicate_ticks}" "${pipeline_tick_time_reversals}"
    printf '  "volume_regression_count": %s, "duplicate_bar_count": %s, "conflict_bar_count": %s,\n' "${pipeline_volume_regressions}" "${pipeline_duplicate_bars}" "${pipeline_conflict_bars}"
    printf '  "incomplete_bar_count": %s, "missing_strategy_evaluation_count": %s,\n' "${pipeline_incomplete_bars}" "${pipeline_missing_strategy_evaluations}"
    printf '  "duplicate_disposition_count": %s, "unresolved_trace_count": %s,\n' "${pipeline_duplicate_dispositions}" "${pipeline_unresolved_traces}"
    printf '  "strategy_trace_integrity_failure_count": %s, "generation_mismatch_submission_count": %s,\n' \
      "${pipeline_strategy_trace_integrity_failures}" "${pipeline_generation_mismatch_submissions}"
    printf '  "session_counts": {"late_ticks":%s,"duplicate_ticks":%s,"duplicate_bars":%s,"conflict_bars":%s,"incomplete_bars":%s,"missing_strategy_evaluations":%s,"duplicate_dispositions":%s},\n' \
      "${pipeline_late_ticks}" "${pipeline_duplicate_ticks}" "${pipeline_duplicate_bars}" \
      "${pipeline_conflict_bars}" "${pipeline_incomplete_bars}" \
      "${pipeline_missing_strategy_evaluations}" "${pipeline_duplicate_dispositions}"
    printf '  "window_15m_counts": {"late_ticks":%s,"duplicate_ticks":%s,"bar_finalize_samples":%s,"bar_to_decision_samples":%s,"candidate_to_disposition_samples":%s,"ctp_to_callback_samples":%s},\n' \
      "${pipeline_recent_late_ticks}" "${pipeline_recent_duplicate_ticks}" \
      "${#bar_finalize_samples_ms[@]}" "${#bar_to_decision_samples_ms[@]}" \
      "${#candidate_to_disposition_samples_ms[@]}" "${#ctp_to_callback_samples_ms[@]}"
    printf '  "cursor_count": %s,\n' "${cursor_count}"
    printf '  "input_cursors": [\n'
    for tracked_file in "${!file_offsets[@]}"; do
      (( cursor_index > 0 )) && printf ',\n'
      printf '    {"path":"%s","inode":"%s","byte_offset":%s,"size_bytes":%s,"lines":%s}' \
        "$(json_escape "${tracked_file}")" "$(json_escape "${file_inodes[${tracked_file}]:-}")" \
        "${file_byte_offsets[${tracked_file}]:-0}" "${file_sizes[${tracked_file}]:-0}" \
        "${file_offsets[${tracked_file}]:-0}"
      cursor_index=$((cursor_index + 1))
    done
    printf '\n  ],\n'
    printf '  "stages": {\n'
    printf '    "runtime": {"status":"%s","reason":"%s"},\n' "${stage_runtime_status}" "$(json_escape "${stage_runtime_reason}")"
    printf '    "market_data": {"status":"%s","reason":"%s"},\n' "${stage_market_status}" "$(json_escape "${stage_market_reason}")"
    printf '    "bar_1m": {"status":"%s","reason":"%s"},\n' "${stage_bar_1m_status}" "$(json_escape "${stage_bar_1m_reason}")"
    printf '    "bar_5m": {"status":"%s","reason":"%s"},\n' "${stage_bar_5m_status}" "$(json_escape "${stage_bar_5m_reason}")"
    printf '    "strategy": {"status":"%s","reason":"%s"},\n' "${stage_strategy_status}" "$(json_escape "${stage_strategy_reason}")"
    printf '    "execution": {"status":"%s","reason":"%s"}\n' "${stage_execution_status}" "$(json_escape "${stage_execution_reason}")"
    printf '  },\n'
    printf '  "latencies_ms": {"bar_finalize_p50":%s,"bar_finalize_p95":%s,"bar_finalize_p99":%s,"bar_to_decision_p50":%s,"bar_to_decision_p95":%s,"bar_to_decision_p99":%s,"candidate_to_disposition_p50":%s,"candidate_to_disposition_p95":%s,"candidate_to_disposition_p99":%s,"ctp_to_callback_p50":%s,"ctp_to_callback_p95":%s,"ctp_to_callback_p99":%s},\n' \
      "${bar_p50}" "${bar_p95}" "${bar_p99}" "${decision_p50}" "${decision_p95}" "${decision_p99}" \
      "${disposition_p50}" "${disposition_p95}" "${disposition_p99}" "${callback_p50}" "${callback_p95}" "${callback_p99}"
    printf '  "products": [\n'
    IFS=',' read -r -a output_products <<< "${monitored_products_csv:-}"
    for product in "${output_products[@]}"; do
      [[ -n "${product}" ]] || continue
      (( product_index > 0 )) && printf ',\n'
      printf '    {"product_id":"%s","instrument_id":"%s","exchange_id":"%s","status":"%s","reason":"%s","schema":"%s","tick_age_seconds":%s,"tick_delay_ms":%s,"tick_time_reversals":%s,"duplicate_ticks":%s,"volume_regressions":%s,"last_1m":"%s","last_5m":"%s","bar_5m_complete":%s,"strategy_minute":"%s","strategy_evaluations":%s,"candidates":%s,"allowed":%s,"pending_traces":%s}' \
        "$(json_escape "${product}")" "$(json_escape "${product_instrument[${product}]:-unknown}")" \
        "$(json_escape "${product_exchange[${product}]:-unknown}")" "${product_status[${product}]:-unknown}" \
        "$(json_escape "${product_reason[${product}]:-unknown}")" "$(json_escape "${product_schema[${product}]:-unknown}")" \
        "${product_tick_age[${product}]:--1}" "${product_tick_delay_ms[${product}]:--1}" \
        "${product_tick_reversals[${product}]:-0}" "${product_duplicate_ticks[${product}]:-0}" \
        "${product_volume_regressions[${product}]:-0}" \
        "$(json_escape "${product_last_1m[${product}]:-}")" "$(json_escape "${product_last_5m[${product}]:-}")" \
        "${product_bar_complete[${product}]:-0}" "$(json_escape "${product_strategy_minute[${product}]:-}")" \
        "${product_strategy_evaluations[${product}]:-0}" "${product_candidates[${product}]:-0}" \
        "${product_allowed[${product}]:-0}" "${product_pending[${product}]:-0}"
      product_index=$((product_index + 1))
    done
    printf '\n  ],\n'
    printf '  "recent_traces": [\n'
    for trace_id in "${!signal_seen_epoch[@]}"; do
      (( trace_index >= 12 )) && break
      (( trace_index > 0 )) && printf ',\n'
      printf '    {"trace_id":"%s","instrument_id":"%s","strategy_id":"%s","status":"%s","last_event":"%s","client_order_id":"%s"}' \
        "$(json_escape "${trace_id}")" "$(json_escape "${signal_instrument[${trace_id}]:-}")" \
        "$(json_escape "${signal_strategy[${trace_id}]:-}")" "$(json_escape "${signal_status[${trace_id}]:-unknown}")" \
        "$(json_escape "${signal_last_event[${trace_id}]:-}")" "$(json_escape "${signal_client_order_id[${trace_id}]:-}")"
      trace_index=$((trace_index + 1))
    done
    printf '\n  ],\n'
    printf '  "issues": ['
    local issue_separator=""
    for stage_pair in "runtime|${stage_runtime_status}|${stage_runtime_reason}" "market_data|${stage_market_status}|${stage_market_reason}" "bar_1m|${stage_bar_1m_status}|${stage_bar_1m_reason}" "bar_5m|${stage_bar_5m_status}|${stage_bar_5m_reason}" "strategy|${stage_strategy_status}|${stage_strategy_reason}" "execution|${stage_execution_status}|${stage_execution_reason}"; do
      IFS='|' read -r issue_stage issue_status issue_reason <<< "${stage_pair}"
      [[ "${issue_status}" == "healthy" || "${issue_status}" == "inactive" ]] && continue
      printf '%s"%s:%s:%s"' "${issue_separator}" "$(json_escape "${issue_stage}")" "$(json_escape "${issue_status}")" "$(json_escape "${issue_reason}")"
      issue_separator=,
    done
    printf '],\n  "recent_recoveries": ['
    for recovery in "${recent_recovery_events[@]}"; do
      (( recovery_index > 0 )) && printf ','
      printf '"%s"' "$(json_escape "${recovery}")"
      recovery_index=$((recovery_index + 1))
    done
    printf ']\n}\n'
  } > "${tmp_file}"
  sync -f "${tmp_file}" 2>/dev/null || true
  mv -f -- "${tmp_file}" "${HEALTH_SNAPSHOT_FILE}"
}

scan_once() {
  local now_epoch
  check_core_engine
  check_core_readiness
  scan_kama_csv_files
  scan_core_log
  process_wal_file "${WAL_FILE}"
  now_epoch="$(now_epoch)"
  check_signal_timeouts "${now_epoch}"
  check_trace_integrity_timeouts "${now_epoch}"
  collect_pipeline_health
  write_pipeline_health
  print_summary_if_due "${now_epoch}"
  initial_scan_done=1
  save_checkpoint
  write_heartbeat "running"
}

load_checkpoint
echo "[start] SimNow pipeline health monitor"
echo "[config] market_data_dir=${MARKET_DATA_DIR} wal_file=${WAL_FILE} run_root=${RUN_ROOT} monitor_root=${MONITOR_ROOT} core_readiness_file=${CORE_READINESS_FILE} pipeline_health_file=${HEALTH_SNAPSHOT_FILE}"
write_event "monitor_started" "" "SimNow pipeline health monitor started" \
  "\"market_data_dir\":\"$(json_escape "${MARKET_DATA_DIR}")\",\"wal_file\":\"$(json_escape "${WAL_FILE}")\",\"run_root\":\"$(json_escape "${RUN_ROOT}")\",\"heartbeat_file\":\"$(json_escape "${HEARTBEAT_FILE}")\",\"core_readiness_file\":\"$(json_escape "${CORE_READINESS_FILE}")\",\"pipeline_health_file\":\"$(json_escape "${HEALTH_SNAPSHOT_FILE}")\""

on_monitor_exit() {
  write_heartbeat "stopped" 2>/dev/null || true
}
trap on_monitor_exit EXIT

on_monitor_error() {
  local rc="$1"
  local line="$2"
  trap - ERR EXIT
  echo "[error] pipeline monitor failed rc=${rc} line=${line}" >&2
  write_heartbeat "self_error" 2>/dev/null || true
  exit 3
}
trap 'on_monitor_error "$?" "${LINENO}"' ERR

if (( ONCE == 1 )); then
  scan_once
  if (( STRICT_EXIT == 1 )); then
    case "${pipeline_overall_status}" in
      unhealthy) exit 2 ;;
      degraded|unknown) exit 1 ;;
    esac
  fi
  exit 0
fi

while true; do
  scan_once
  sleep "${POLL_SECONDS}"
done
