#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

DEFAULT_RUN_ROOT="${QUANT_ROOT}/runtime/trading/runs/simnow"
LEGACY_RUN_ROOT="${QUANT_ROOT}/runtime/simnow_trading"
RUN_ROOT="${SIMNOW_RUN_ROOT:-${DEFAULT_RUN_ROOT}}"
if [[ -z "${SIMNOW_RUN_ROOT:-}" && ! -f "${RUN_ROOT}/current_core_engine.pid" && \
      -f "${LEGACY_RUN_ROOT}/current_core_engine.pid" ]]; then
  RUN_ROOT="${LEGACY_RUN_ROOT}"
fi
CONFIG_PATH="${CTP_CONFIG_PATH:-}"
CORE_ENGINE_BIN="${CORE_ENGINE_BIN:-${QUANT_ROOT}/build-gcc/core_engine}"
SYSTEMD_UNIT=""
SYSTEMD_SCOPE=""
STOP_TIMEOUT_SECONDS="${SIMNOW_STOP_TIMEOUT_SECONDS:-30}"
TARGET_PID=""
SUPERVISOR_PID=""
STOP_SUPERVISOR=0
STOP_SYSTEMD=0
KILL_AFTER_TIMEOUT=1
DRY_RUN=0
RUN_ROOT_SET_BY_CLI=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Stop the currently running SimNow core_engine safely.

Options:
  --run-root <path>          SimNow run root (default: ${RUN_ROOT})
  --config <path>            Optional config path retained for invocation compatibility
  --core-engine-bin <path>   core_engine binary path hint (default: ${CORE_ENGINE_BIN})
  --pid <pid>                Stop a specific core_engine PID instead of reading current state
  --timeout-seconds <int>    Seconds to wait after TERM before escalation (default: ${STOP_TIMEOUT_SECONDS})
  --no-kill                  Do not send KILL if TERM does not stop the process
  --stop-supervisor          Stop one explicitly identified shell supervisor first
  --supervisor-pid <pid>     Exact reviewed supervisor PID (required with --stop-supervisor)
  --stop-systemd             Stop an explicitly identified systemd unit before core_engine
  --systemd-unit <name>      Exact reviewed .service name (required with --stop-systemd/--all)
  --systemd-scope <scope>    Exact manager scope: user or system (required with --stop-systemd/--all)
  --all                      Deprecated safe alias for --stop-systemd; never scans host processes
  --dry-run                  Print actions without stopping anything
  -h, --help                 Show this help
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

process_args() {
  local process_pid="$1"
  ps -p "${process_pid}" -o args= 2>/dev/null || true
}

is_core_engine_pid() {
  local process_pid="$1"
  local args
  args="$(process_args "${process_pid}")"
  [[ "${args}" == *"/core_engine"* || "${args}" == *"core_engine"* ]]
}

current_pid_file() {
  printf '%s/current_core_engine.pid\n' "${RUN_ROOT}"
}

read_current_pid() {
  local pid_file
  pid_file="$(current_pid_file)"
  [[ -f "${pid_file}" ]] || return 1
  tr -dc '0-9' < "${pid_file}"
}

is_supervisor_pid() {
  local process_pid="$1"
  [[ "$(process_args "${process_pid}")" == *"supervise_simnow_trading.sh"* ]]
}

supervisor_holds_run_root_lock() {
  local process_pid="$1"
  local expected_lock fd_link fd_target
  expected_lock="$(readlink -f -- "${RUN_ROOT}/locks/supervisor.lock" 2>/dev/null || true)"
  [[ -n "${expected_lock}" && -d "/proc/${process_pid}/fd" ]] || return 1
  for fd_link in "/proc/${process_pid}/fd/"*; do
    [[ -e "${fd_link}" ]] || continue
    fd_target="$(readlink -f -- "${fd_link}" 2>/dev/null || true)"
    [[ "${fd_target}" == "${expected_lock}" ]] && return 0
  done
  return 1
}

run_cmd() {
  if [[ ${DRY_RUN} -eq 1 ]]; then
    printf '[dry-run]'
  else
    printf '[cmd]'
  fi
  printf ' %q' "$@"
  printf '\n'
  if [[ ${DRY_RUN} -eq 0 ]]; then
    "$@"
  fi
}

stop_pid() {
  local process_pid="$1"
  local label="$2"
  local waited_seconds=0

  if ! pid_is_alive "${process_pid}"; then
    echo "[ok] ${label} pid=${process_pid} is not running"
    return 0
  fi

  echo "[step] stopping ${label} pid=${process_pid}"
  run_cmd kill -TERM "${process_pid}"
  if [[ ${DRY_RUN} -eq 1 ]]; then
    return 0
  fi

  while pid_is_alive "${process_pid}" && (( waited_seconds < STOP_TIMEOUT_SECONDS )); do
    sleep 1
    waited_seconds=$((waited_seconds + 1))
  done

  if pid_is_alive "${process_pid}"; then
    if [[ ${KILL_AFTER_TIMEOUT} -eq 1 ]]; then
      echo "[warn] ${label} pid=${process_pid} did not stop after ${STOP_TIMEOUT_SECONDS}s; sending KILL"
      run_cmd kill -KILL "${process_pid}"
      sleep 1
    else
      echo "[warn] ${label} pid=${process_pid} is still running after ${STOP_TIMEOUT_SECONDS}s"
      return 1
    fi
  fi

  if pid_is_alive "${process_pid}"; then
    echo "[error] ${label} pid=${process_pid} is still running"
    return 1
  fi

  echo "[ok] ${label} pid=${process_pid} stopped"
}

resolve_target_pid() {
  local current_pid
  if [[ -n "${TARGET_PID}" ]]; then
    printf '%s\n' "${TARGET_PID}"
    return 0
  fi

  current_pid="$(read_current_pid || true)"
  if [[ -n "${current_pid}" ]]; then
    printf '%s\n' "${current_pid}"
    return 0
  fi
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; RUN_ROOT_SET_BY_CLI=1; shift 2 ;;
    --config|--ctp-config-path) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; shift 2 ;;
    --core-engine-bin) require_value "$1" "${2:-}"; CORE_ENGINE_BIN="$2"; shift 2 ;;
    --pid) require_value "$1" "${2:-}"; TARGET_PID="$2"; shift 2 ;;
    --timeout-seconds) require_value "$1" "${2:-}"; STOP_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --no-kill) KILL_AFTER_TIMEOUT=0; shift ;;
    --stop-supervisor) STOP_SUPERVISOR=1; shift ;;
    --supervisor-pid) require_value "$1" "${2:-}"; SUPERVISOR_PID="$2"; shift 2 ;;
    --stop-systemd) STOP_SYSTEMD=1; shift ;;
    --systemd-unit) require_value "$1" "${2:-}"; SYSTEMD_UNIT="$2"; shift 2 ;;
    --systemd-scope) require_value "$1" "${2:-}"; SYSTEMD_SCOPE="$2"; shift 2 ;;
    --all) STOP_SYSTEMD=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

is_non_negative_int "${STOP_TIMEOUT_SECONDS}" || die "--timeout-seconds must be a non-negative integer"

SYSTEMCTL_SCOPE_ARGS=()
if [[ ${STOP_SYSTEMD} -eq 1 ]]; then
  [[ -n "${SYSTEMD_UNIT}" ]] ||
    die "--systemd-unit is required with --stop-systemd/--all; do not infer the active supervisor"
  [[ -n "${SYSTEMD_SCOPE}" ]] ||
    die "--systemd-scope user|system is required with --stop-systemd/--all"
  [[ "${SYSTEMD_UNIT}" =~ ^[A-Za-z0-9_.:@-]+\.service$ ]] ||
    die "--systemd-unit must be one explicit .service name using only letters, digits, dot, underscore, colon, at, or hyphen"
  case "${SYSTEMD_SCOPE}" in
    user) SYSTEMCTL_SCOPE_ARGS=(--user) ;;
    system) SYSTEMCTL_SCOPE_ARGS=() ;;
    *) die "--systemd-scope must be one of: user, system" ;;
  esac
  [[ ${STOP_SUPERVISOR} -eq 0 && -z "${SUPERVISOR_PID}" && -z "${TARGET_PID}" ]] ||
    die "systemd stop cannot be combined with process PID or shell-supervisor options"
else
  [[ -z "${SYSTEMD_UNIT}" && -z "${SYSTEMD_SCOPE}" ]] ||
    die "--systemd-unit/--systemd-scope require --stop-systemd or --all"
fi

if [[ ${STOP_SUPERVISOR} -eq 1 ]]; then
  [[ ${RUN_ROOT_SET_BY_CLI} -eq 1 ]] ||
    die "--stop-supervisor requires an explicit --run-root identity"
  [[ "${SUPERVISOR_PID}" =~ ^[1-9][0-9]*$ ]] ||
    die "--stop-supervisor requires an explicit positive --supervisor-pid"
elif [[ -n "${SUPERVISOR_PID}" ]]; then
  die "--supervisor-pid requires --stop-supervisor"
fi

cd "${QUANT_ROOT}"

if [[ ${STOP_SYSTEMD} -eq 1 ]]; then
  command -v systemctl >/dev/null 2>&1 ||
    die "systemctl is required before any supervised core_engine can be stopped"
  echo "[step] stopping ${SYSTEMD_SCOPE} systemd unit ${SYSTEMD_UNIT}"
  run_cmd systemctl "${SYSTEMCTL_SCOPE_ARGS[@]}" stop "${SYSTEMD_UNIT}"
  if [[ ${DRY_RUN} -eq 0 ]] &&
    systemctl "${SYSTEMCTL_SCOPE_ARGS[@]}" is-active --quiet "${SYSTEMD_UNIT}"; then
    die "systemd unit is still active after stop: scope=${SYSTEMD_SCOPE} unit=${SYSTEMD_UNIT}"
  fi
  if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "[ok] systemd-managed SimNow stop dry-run completed; no process scan performed"
  else
    echo "[ok] systemd unit is inactive; no process scan performed"
  fi
  exit 0
else
  echo "[warn] no systemd unit was stopped; a systemd-managed core_engine may restart"
fi

if [[ ${STOP_SUPERVISOR} -eq 1 ]]; then
  pid_is_alive "${SUPERVISOR_PID}" || die "reviewed supervisor pid is not running: ${SUPERVISOR_PID}"
  is_supervisor_pid "${SUPERVISOR_PID}" ||
    die "pid ${SUPERVISOR_PID} is not a SimNow shell supervisor"
  supervisor_holds_run_root_lock "${SUPERVISOR_PID}" ||
    die "pid ${SUPERVISOR_PID} does not own the reviewed run-root supervisor lock: ${RUN_ROOT}"
  stop_pid "${SUPERVISOR_PID}" "simnow supervisor"
fi

target_pid="$(resolve_target_pid || true)"
if [[ -z "${target_pid}" ]]; then
  echo "[ok] no current core_engine process found"
  exit 0
fi

if ! [[ "${target_pid}" =~ ^[0-9]+$ ]]; then
  die "invalid pid: ${target_pid}"
fi
if pid_is_alive "${target_pid}" && ! is_core_engine_pid "${target_pid}"; then
  die "pid ${target_pid} is not a core_engine process: $(process_args "${target_pid}")"
fi

stop_pid "${target_pid}" "core_engine"

pid_file="$(current_pid_file)"
current_pid="$(read_current_pid || true)"
if [[ "${current_pid}" == "${target_pid}" && ${DRY_RUN} -eq 0 ]]; then
  rm -f "${pid_file}"
fi

if [[ ${DRY_RUN} -eq 1 ]]; then
  echo "[ok] SimNow trading stop dry-run completed"
else
  echo "[ok] SimNow trading stop completed"
fi
