#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

ENV_FILE="${ENV_FILE:-${QUANT_ROOT}/.env}"
CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/sim/ctp.yaml}"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build}"
CORE_ENGINE_BIN="${CORE_ENGINE_BIN:-${BUILD_DIR}/core_engine}"
SIMNOW_PROBE_BIN="${SIMNOW_PROBE_BIN:-${BUILD_DIR}/simnow_probe}"
RUN_ID="${SIMNOW_RUN_ID:-simnow-$(date +%Y%m%dT%H%M%S)}"
RUN_ROOT="${SIMNOW_RUN_ROOT:-${QUANT_ROOT}/runtime/simnow_trading}"
RUN_SECONDS="${SIMNOW_RUN_SECONDS:-0}"
PROBE_SECONDS="${SIMNOW_PROBE_SECONDS:-5}"
PROBE_TIMEOUT_SECONDS="${SIMNOW_PROBE_TIMEOUT_SECONDS:-120}"
HEALTH_INTERVAL_MS="${SIMNOW_HEALTH_INTERVAL_MS:-1000}"
INSTRUMENT_TIMEOUT_SECONDS="${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-45}"
MIN_FREE_MB="${SIMNOW_MIN_FREE_MB:-2048}"
LOG_MAX_BYTES="${SIMNOW_LOG_MAX_BYTES:-104857600}"
LOG_RETENTION_DAYS="${SIMNOW_LOG_RETENTION_DAYS:-14}"
STARTUP_GRACE_SECONDS="${SIMNOW_STARTUP_GRACE_SECONDS:-3}"
BACKGROUND=1
SKIP_PROBE=0
PROBE_ONLY=0
DRY_RUN=0
ALLOW_EXISTING=0
STOP_EXISTING=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Start SimNow trading with production-oriented preflight checks.

Options:
  --env-file <path>              Environment file to source (default: ${ENV_FILE})
  --config <path>                CTP yaml config (default: ${CONFIG_PATH})
  --build-dir <path>             Build directory (default: ${BUILD_DIR})
  --core-engine-bin <path>       core_engine binary (default: ${CORE_ENGINE_BIN})
  --simnow-probe-bin <path>      simnow_probe binary (default: ${SIMNOW_PROBE_BIN})
  --run-id <value>               Run id for logs/PID (default: ${RUN_ID})
  --run-root <path>              Run output root (default: ${RUN_ROOT})
  --run-seconds <int>            Stop core_engine after N seconds; 0 means unlimited (default: ${RUN_SECONDS})
  --probe-seconds <int>          simnow_probe monitor seconds before trading (default: ${PROBE_SECONDS})
  --probe-timeout-seconds <int>  Hard timeout for simnow_probe (default: ${PROBE_TIMEOUT_SECONDS})
  --health-interval-ms <int>     simnow_probe health interval (default: ${HEALTH_INTERVAL_MS})
  --instrument-timeout-seconds <int>
                                  simnow_probe instrument query timeout (default: ${INSTRUMENT_TIMEOUT_SECONDS})
  --min-free-mb <int>            Required free disk space under run root (default: ${MIN_FREE_MB})
  --log-max-bytes <int>          Rotate log files above this size (default: ${LOG_MAX_BYTES})
  --log-retention-days <int>     Delete old rotated logs after N days (default: ${LOG_RETENTION_DAYS})
  --skip-probe                   Start core_engine without running simnow_probe first
  --probe-only                   Run the pre-trade probe and do not start core_engine
  --foreground                   Run core_engine in foreground and tee logs
  --background                   Run core_engine in background (default)
  --allow-existing               Exit successfully when an existing core_engine is already alive
  --stop-existing                Stop any existing core_engine before starting this run
  --dry-run                      Print resolved command/env summary and exit
  -h, --help                     Show this help

Alert hooks:
  SIMNOW_ALERT_WEBHOOK_URL       POST a generic JSON text alert to this URL
  SIMNOW_ALERT_EMAIL_TO          Send mail if the local mail command exists
  SIMNOW_ALERT_COMMAND           Run command with ALERT_SEVERITY and ALERT_MESSAGE in env
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

  if [[ -z "${message}" ]]; then
    return 0
  fi

  echo "[alert:${severity}] ${message}" >&2

  if [[ -n "${SIMNOW_ALERT_WEBHOOK_URL:-}" ]] && command -v curl >/dev/null 2>&1; then
    escaped_message="$(json_escape "[${severity}] ${message}")"
    payload="{\"msgtype\":\"text\",\"text\":{\"content\":\"${escaped_message}\"}}"
    curl -fsS -m 10 -H 'Content-Type: application/json' \
      -d "${payload}" "${SIMNOW_ALERT_WEBHOOK_URL}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_EMAIL_TO:-}" ]] && command -v mail >/dev/null 2>&1; then
    printf '%s\n' "${message}" | mail -s "[quant-hft][${severity}] SimNow trading" \
      "${SIMNOW_ALERT_EMAIL_TO}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${SIMNOW_ALERT_COMMAND:-}" ]]; then
    ALERT_SEVERITY="${severity}" ALERT_MESSAGE="${message}" \
      bash -lc "${SIMNOW_ALERT_COMMAND}" >/dev/null 2>&1 || true
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

pid_is_alive() {
  local process_pid="${1:-}"
  [[ "${process_pid}" =~ ^[0-9]+$ ]] || return 1
  kill -0 "${process_pid}" 2>/dev/null
}

stop_pid() {
  local process_pid="$1"
  local reason="${2:-requested}"
  local waited_seconds=0

  pid_is_alive "${process_pid}" || return 0
  echo "[step] stopping existing core_engine pid=${process_pid} reason=${reason}"
  kill -TERM "${process_pid}" 2>/dev/null || true
  while pid_is_alive "${process_pid}" && [[ ${waited_seconds} -lt 20 ]]; do
    sleep 1
    waited_seconds=$((waited_seconds + 1))
  done
  if pid_is_alive "${process_pid}"; then
    echo "[warn] core_engine pid=${process_pid} did not stop after TERM; sending KILL"
    kill -KILL "${process_pid}" 2>/dev/null || true
  fi
}

find_existing_core_engine_pids() {
  local process_pid
  local process_args
  while read -r process_pid process_args; do
    [[ -n "${process_pid:-}" ]] || continue
    [[ "${process_pid}" == "$$" || "${process_pid}" == "${BASHPID}" ]] && continue
    if [[ "${process_args:-}" == *"${CORE_ENGINE_BIN}"* || "${process_args:-}" == *"/core_engine"* ]]; then
      if [[ "${process_args:-}" == *"${CONFIG_PATH}"* || "${process_args:-}" == *"$(basename "${CONFIG_PATH}")"* ]]; then
        printf '%s\n' "${process_pid}"
      fi
    fi
  done < <(ps -eo pid=,args=)
}

check_free_disk() {
  local path="$1"
  local min_free_mb="$2"
  local free_mb

  mkdir -p "${path}"
  free_mb="$(df -Pm "${path}" | awk 'NR == 2 {print $4}')"
  [[ "${free_mb}" =~ ^[0-9]+$ ]] || die "unable to determine free disk space for ${path}"
  if (( free_mb < min_free_mb )); then
    die "free disk space under ${path} is ${free_mb}MB, below required ${min_free_mb}MB"
  fi
}

rotate_file_if_needed() {
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
  mv "${file_path}" "${rotated_path}"
  if command -v gzip >/dev/null 2>&1; then
    gzip -f "${rotated_path}" || true
  fi
  echo "[info] rotated log: ${file_path}"
}

cleanup_old_logs() {
  local root_dir="$1"
  local retention_days="$2"
  [[ -d "${root_dir}" ]] || return 0
  find "${root_dir}" -type f \( -name '*.log.*' -o -name '*.log.*.gz' \) \
    -mtime "+${retention_days}" -delete 2>/dev/null || true
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) require_value "$1" "${2:-}"; ENV_FILE="$2"; shift 2 ;;
    --config|--ctp-config-path) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; shift 2 ;;
    --build-dir)
      require_value "$1" "${2:-}"
      BUILD_DIR="$2"
      CORE_ENGINE_BIN="${BUILD_DIR}/core_engine"
      SIMNOW_PROBE_BIN="${BUILD_DIR}/simnow_probe"
      shift 2
      ;;
    --core-engine-bin|--core-bin) require_value "$1" "${2:-}"; CORE_ENGINE_BIN="$2"; shift 2 ;;
    --simnow-probe-bin|--probe-bin) require_value "$1" "${2:-}"; SIMNOW_PROBE_BIN="$2"; shift 2 ;;
    --run-id) require_value "$1" "${2:-}"; RUN_ID="$2"; shift 2 ;;
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; shift 2 ;;
    --run-seconds) require_value "$1" "${2:-}"; RUN_SECONDS="$2"; shift 2 ;;
    --probe-seconds) require_value "$1" "${2:-}"; PROBE_SECONDS="$2"; shift 2 ;;
    --probe-timeout-seconds) require_value "$1" "${2:-}"; PROBE_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --health-interval-ms) require_value "$1" "${2:-}"; HEALTH_INTERVAL_MS="$2"; shift 2 ;;
    --instrument-timeout-seconds) require_value "$1" "${2:-}"; INSTRUMENT_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --min-free-mb) require_value "$1" "${2:-}"; MIN_FREE_MB="$2"; shift 2 ;;
    --log-max-bytes) require_value "$1" "${2:-}"; LOG_MAX_BYTES="$2"; shift 2 ;;
    --log-retention-days) require_value "$1" "${2:-}"; LOG_RETENTION_DAYS="$2"; shift 2 ;;
    --skip-probe) SKIP_PROBE=1; shift ;;
    --probe-only) PROBE_ONLY=1; shift ;;
    --foreground) BACKGROUND=0; shift ;;
    --background) BACKGROUND=1; shift ;;
    --allow-existing) ALLOW_EXISTING=1; shift ;;
    --stop-existing) STOP_EXISTING=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "${ENV_FILE}" ]] || die "--env-file is empty"
[[ -n "${CONFIG_PATH}" ]] || die "--config is empty"
[[ -n "${RUN_ID}" ]] || die "--run-id is empty"
is_non_negative_int "${RUN_SECONDS}" || die "--run-seconds must be a non-negative integer"
is_positive_int "${PROBE_SECONDS}" || die "--probe-seconds must be a positive integer"
is_positive_int "${PROBE_TIMEOUT_SECONDS}" || die "--probe-timeout-seconds must be a positive integer"
is_positive_int "${HEALTH_INTERVAL_MS}" || die "--health-interval-ms must be a positive integer"
is_positive_int "${INSTRUMENT_TIMEOUT_SECONDS}" || die "--instrument-timeout-seconds must be a positive integer"
is_positive_int "${MIN_FREE_MB}" || die "--min-free-mb must be a positive integer"
is_positive_int "${LOG_MAX_BYTES}" || die "--log-max-bytes must be a positive integer"
is_non_negative_int "${LOG_RETENTION_DAYS}" || die "--log-retention-days must be a non-negative integer"
is_non_negative_int "${STARTUP_GRACE_SECONDS}" || die "SIMNOW_STARTUP_GRACE_SECONDS must be a non-negative integer"

cd "${QUANT_ROOT}"

[[ -f "${ENV_FILE}" ]] || die "env file not found: ${ENV_FILE}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

export CTP_CONFIG_PATH="${CONFIG_PATH}"
export CTP_SIM_MARKET_FRONT="${CTP_SIM_MARKET_FRONT:-tcp://182.254.243.31:30011}"
export CTP_SIM_TRADER_FRONT="${CTP_SIM_TRADER_FRONT:-tcp://182.254.243.31:30001}"
export CTP_SIM_IS_PRODUCTION_MODE="${CTP_SIM_IS_PRODUCTION_MODE:-true}"
export CTP_SIM_ENABLE_REAL_API="${CTP_SIM_ENABLE_REAL_API:-true}"

LOCK_DIR="${SIMNOW_LOCK_DIR:-${RUN_ROOT}/locks}"
LOCK_FILE="${LOCK_DIR}/core_engine_start.lock"
CURRENT_PID_FILE="${SIMNOW_CURRENT_PID_FILE:-${RUN_ROOT}/current_core_engine.pid}"
CURRENT_RUN_FILE="${SIMNOW_CURRENT_RUN_FILE:-${RUN_ROOT}/current_run_dir}"
CURRENT_LOG_FILE="${SIMNOW_CURRENT_LOG_FILE:-${RUN_ROOT}/current_core_engine_log}"
mkdir -p "${RUN_ROOT}" "${LOCK_DIR}"

if command -v flock >/dev/null 2>&1; then
  exec 9>"${LOCK_FILE}"
  flock -n 9 || die "another SimNow core_engine start is already in progress: ${LOCK_FILE}"
else
  LOCK_FALLBACK_DIR="${LOCK_FILE}.d"
  mkdir "${LOCK_FALLBACK_DIR}" 2>/dev/null || die "another SimNow core_engine start is already in progress: ${LOCK_FALLBACK_DIR}"
  trap 'rm -rf "${LOCK_FALLBACK_DIR}"' EXIT
fi

[[ -f "${CONFIG_PATH}" ]] || die "config file not found: ${CONFIG_PATH}"
[[ -x "${CORE_ENGINE_BIN}" ]] || die "core_engine binary is not executable: ${CORE_ENGINE_BIN}"
[[ -x "${SIMNOW_PROBE_BIN}" ]] || die "simnow_probe binary is not executable: ${SIMNOW_PROBE_BIN}"
[[ "${CTP_SIM_IS_PRODUCTION_MODE}" == "true" ]] || die "CTP_SIM_IS_PRODUCTION_MODE must be true for trading-hours SimNow fronts"
[[ "${CTP_SIM_ENABLE_REAL_API}" == "true" ]] || die "CTP_SIM_ENABLE_REAL_API must be true to start SimNow trading"
[[ -n "${CTP_SIM_BROKER_ID:-}" ]] || die "CTP_SIM_BROKER_ID is missing"
[[ -n "${CTP_SIM_USER_ID:-}" ]] || die "CTP_SIM_USER_ID is missing"
[[ -n "${CTP_SIM_INVESTOR_ID:-}" ]] || die "CTP_SIM_INVESTOR_ID is missing"
[[ -n "${CTP_SIM_PASSWORD:-}" ]] || die "CTP_SIM_PASSWORD is missing"
[[ -n "${CTP_SIM_AUTH_CODE:-}" ]] || die "CTP_SIM_AUTH_CODE is missing"
[[ -n "${CTP_SIM_APP_ID:-}" ]] || die "CTP_SIM_APP_ID is missing"

case "${CTP_SIM_MARKET_FRONT}|${CTP_SIM_TRADER_FRONT}" in
  tcp://182.254.243.31:30011\|tcp://182.254.243.31:30001|\
  tcp://182.254.243.31:30012\|tcp://182.254.243.31:30002|\
  tcp://182.254.243.31:30013\|tcp://182.254.243.31:30003)
    ;;
  *)
    die "CTP_SIM_MARKET_FRONT/CTP_SIM_TRADER_FRONT must point to a SimNow trading-hours group (30011/30001, 30012/30002, or 30013/30003)"
    ;;
esac

check_free_disk "${RUN_ROOT}" "${MIN_FREE_MB}"
cleanup_old_logs "${RUN_ROOT}" "${LOG_RETENTION_DAYS}"

existing_pids=""
if [[ -f "${CURRENT_PID_FILE}" ]]; then
  existing_pid="$(tr -dc '0-9' < "${CURRENT_PID_FILE}" || true)"
  if pid_is_alive "${existing_pid}"; then
    existing_pids="${existing_pid}"
  fi
fi
detected_pids="$(find_existing_core_engine_pids | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"
if [[ -n "${detected_pids}" ]]; then
  existing_pids="$(printf '%s %s\n' "${existing_pids}" "${detected_pids}" | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"
fi

if [[ -n "${existing_pids}" ]]; then
  if [[ ${STOP_EXISTING} -eq 1 ]]; then
    for existing_pid in ${existing_pids}; do
      stop_pid "${existing_pid}" "stop-existing"
    done
    rm -f "${CURRENT_PID_FILE}"
  elif [[ ${ALLOW_EXISTING} -eq 1 ]]; then
    echo "[ok] existing core_engine already alive: ${existing_pids}"
    [[ -f "${CURRENT_RUN_FILE}" ]] && echo "[info] current run dir: $(cat "${CURRENT_RUN_FILE}")"
    exit 0
  else
    die "core_engine is already running for this config: ${existing_pids}; use --allow-existing or --stop-existing"
  fi
fi

RUN_DIR="${RUN_ROOT}/${RUN_ID}"
mkdir -p "${RUN_DIR}"
PROBE_LOG="${RUN_DIR}/simnow_probe.log"
ENGINE_LOG="${RUN_DIR}/core_engine.log"
PID_FILE="${RUN_DIR}/core_engine.pid"
SUMMARY_FILE="${RUN_DIR}/run_summary.env"

rotate_file_if_needed "${PROBE_LOG}" "${LOG_MAX_BYTES}"
rotate_file_if_needed "${ENGINE_LOG}" "${LOG_MAX_BYTES}"

cat > "${SUMMARY_FILE}" <<EOF
run_id=${RUN_ID}
run_dir=${RUN_DIR}
config_path=${CONFIG_PATH}
market_front=${CTP_SIM_MARKET_FRONT}
trader_front=${CTP_SIM_TRADER_FRONT}
production_mode=${CTP_SIM_IS_PRODUCTION_MODE}
real_api=${CTP_SIM_ENABLE_REAL_API}
run_seconds=${RUN_SECONDS}
probe_seconds=${PROBE_SECONDS}
probe_timeout_seconds=${PROBE_TIMEOUT_SECONDS}
health_interval_ms=${HEALTH_INTERVAL_MS}
instrument_timeout_seconds=${INSTRUMENT_TIMEOUT_SECONDS}
min_free_mb=${MIN_FREE_MB}
log_max_bytes=${LOG_MAX_BYTES}
engine_log=${ENGINE_LOG}
probe_log=${PROBE_LOG}
pid_file=${PID_FILE}
candidate_groups=group1(30011/30001),group2(30012/30002),group3(30013/30003)
EOF

core_cmd=("${CORE_ENGINE_BIN}" --config "${CONFIG_PATH}")
if [[ "${RUN_SECONDS}" != "0" ]]; then
  core_cmd+=(--run-seconds "${RUN_SECONDS}")
fi

echo "[info] SimNow trading run id: ${RUN_ID}"
echo "[info] run dir: ${RUN_DIR}"
echo "[info] primary front: md=${CTP_SIM_MARKET_FRONT} td=${CTP_SIM_TRADER_FRONT}"
echo "[info] automatic candidates: group1 -> group2 -> group3"

if [[ ${DRY_RUN} -eq 1 ]]; then
  echo "[dry-run] disk check: ${RUN_ROOT} free >= ${MIN_FREE_MB}MB"
  echo "[dry-run] duplicate check: no live core_engine detected"
  echo "[dry-run] probe: timeout ${PROBE_TIMEOUT_SECONDS}s ${SIMNOW_PROBE_BIN} ${CONFIG_PATH} --monitor-seconds ${PROBE_SECONDS} --health-interval-ms ${HEALTH_INTERVAL_MS} --instrument-timeout-seconds ${INSTRUMENT_TIMEOUT_SECONDS}"
  printf '[dry-run] core_engine:'
  printf ' %q' "${core_cmd[@]}"
  printf '\n'
  exit 0
fi

if [[ ${SKIP_PROBE} -eq 0 ]]; then
  echo "[step] running safe SimNow probe before trading"
  if ! timeout "${PROBE_TIMEOUT_SECONDS}s" "${SIMNOW_PROBE_BIN}" "${CONFIG_PATH}" \
      --monitor-seconds "${PROBE_SECONDS}" \
      --health-interval-ms "${HEALTH_INTERVAL_MS}" \
      --instrument-timeout-seconds "${INSTRUMENT_TIMEOUT_SECONDS}" \
      > "${PROBE_LOG}" 2>&1; then
    send_alert "critical" "simnow_probe failed before starting core_engine; run_dir=${RUN_DIR}"
    echo "error: simnow_probe failed; redacted tail follows" >&2
    sed -E 's/((investor_id|account_id)=")[^"]*(")/\1<redacted>\3/g; s/((balance|available|curr_margin|frozen_margin|close_profit|position_profit)=")[-0-9.eE+]*(")/\1<redacted>\3/g' "${PROBE_LOG}" | tail -n 80 >&2
    exit 1
  fi
  echo "[ok] simnow_probe passed: ${PROBE_LOG}"
else
  echo "[warn] skipping simnow_probe by request"
fi

if [[ ${PROBE_ONLY} -eq 1 ]]; then
  echo "[ok] probe-only completed"
  exit 0
fi

printf '%s\n' "${RUN_DIR}" > "${CURRENT_RUN_FILE}"
printf '%s\n' "${ENGINE_LOG}" > "${CURRENT_LOG_FILE}"

echo "[step] starting core_engine"
if [[ ${BACKGROUND} -eq 1 ]]; then
  nohup "${core_cmd[@]}" > "${ENGINE_LOG}" 2>&1 &
  pid=$!
  printf '%s\n' "${pid}" > "${PID_FILE}"
  printf '%s\n' "${pid}" > "${CURRENT_PID_FILE}"

  if (( STARTUP_GRACE_SECONDS > 0 )); then
    sleep "${STARTUP_GRACE_SECONDS}"
  fi
  if ! pid_is_alive "${pid}"; then
    send_alert "critical" "core_engine exited during startup grace period; run_dir=${RUN_DIR}"
    echo "error: core_engine exited during startup; log tail follows" >&2
    tail -n 80 "${ENGINE_LOG}" >&2 || true
    exit 1
  fi

  echo "[ok] core_engine started in background"
  echo "[info] pid: ${pid}"
  echo "[info] pid file: ${PID_FILE}"
  echo "[info] current pid file: ${CURRENT_PID_FILE}"
  echo "[info] log: ${ENGINE_LOG}"
else
  echo "[info] running in foreground; log is also written to ${ENGINE_LOG}"
  "${core_cmd[@]}" 2>&1 | tee -a "${ENGINE_LOG}"
fi