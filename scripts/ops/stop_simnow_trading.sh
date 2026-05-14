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
SYSTEMD_UNIT="${SIMNOW_SYSTEMD_UNIT:-quant-hft-simnow-trading.service}"
STOP_TIMEOUT_SECONDS="${SIMNOW_STOP_TIMEOUT_SECONDS:-30}"
TARGET_PID=""
STOP_SUPERVISOR=0
STOP_SYSTEMD=0
KILL_AFTER_TIMEOUT=1
DRY_RUN=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Stop the currently running SimNow core_engine safely.

Options:
  --run-root <path>          SimNow run root (default: ${RUN_ROOT})
  --config <path>            Optional config path used to filter discovered core_engine processes
  --core-engine-bin <path>   core_engine binary path hint (default: ${CORE_ENGINE_BIN})
  --pid <pid>                Stop a specific core_engine PID instead of reading current state
  --timeout-seconds <int>    Seconds to wait after TERM before escalation (default: ${STOP_TIMEOUT_SECONDS})
  --no-kill                  Do not send KILL if TERM does not stop the process
  --stop-supervisor          Stop shell supervisor processes before stopping core_engine
  --stop-systemd             Stop the user systemd unit before stopping core_engine
  --all                      Equivalent to --stop-systemd --stop-supervisor
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

find_core_engine_pids() {
  local process_pid
  local process_args_text
  while read -r process_pid process_args_text; do
    [[ -n "${process_pid:-}" ]] || continue
    [[ "${process_pid}" == "$$" || "${process_pid}" == "${BASHPID}" ]] && continue
    [[ "${process_args_text:-}" == *"/core_engine"* ||
      "${process_args_text:-}" == *"${CORE_ENGINE_BIN}"* ]] || continue
    if [[ -n "${CONFIG_PATH}" ]]; then
      [[ "${process_args_text}" == *"${CONFIG_PATH}"* ||
        "${process_args_text}" == *"$(basename "${CONFIG_PATH}")"* ]] || continue
    fi
    printf '%s\n' "${process_pid}"
  done < <(ps -eo pid=,args=)
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
  local discovered
  if [[ -n "${TARGET_PID}" ]]; then
    printf '%s\n' "${TARGET_PID}"
    return 0
  fi

  current_pid="$(read_current_pid || true)"
  if [[ -n "${current_pid}" ]]; then
    printf '%s\n' "${current_pid}"
    return 0
  fi

  discovered="$(find_core_engine_pids | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"
  [[ -n "${discovered}" ]] || return 1
  if (( $(printf '%s\n' ${discovered} | wc -l) > 1 )); then
    die "multiple core_engine processes found (${discovered}); pass --pid or --config"
  fi
  printf '%s\n' "${discovered}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; shift 2 ;;
    --config|--ctp-config-path) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; shift 2 ;;
    --core-engine-bin) require_value "$1" "${2:-}"; CORE_ENGINE_BIN="$2"; shift 2 ;;
    --pid) require_value "$1" "${2:-}"; TARGET_PID="$2"; shift 2 ;;
    --timeout-seconds) require_value "$1" "${2:-}"; STOP_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --no-kill) KILL_AFTER_TIMEOUT=0; shift ;;
    --stop-supervisor) STOP_SUPERVISOR=1; shift ;;
    --stop-systemd) STOP_SYSTEMD=1; shift ;;
    --all) STOP_SYSTEMD=1; STOP_SUPERVISOR=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

is_non_negative_int "${STOP_TIMEOUT_SECONDS}" || die "--timeout-seconds must be a non-negative integer"

cd "${QUANT_ROOT}"

if [[ ${STOP_SYSTEMD} -eq 1 ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    echo "[step] stopping user systemd unit ${SYSTEMD_UNIT}"
    run_cmd systemctl --user stop "${SYSTEMD_UNIT}"
  else
    echo "[warn] systemctl is not available; skipping systemd stop"
  fi
elif command -v systemctl >/dev/null 2>&1 &&
  systemctl --user is-active --quiet "${SYSTEMD_UNIT}" 2>/dev/null; then
  echo "[warn] user systemd unit ${SYSTEMD_UNIT} is active; use --stop-systemd or --all to prevent restart"
fi

if [[ ${STOP_SUPERVISOR} -eq 1 ]]; then
  supervisor_pids="$(find_supervisor_pids | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"
  if [[ -n "${supervisor_pids}" ]]; then
    for supervisor_pid in ${supervisor_pids}; do
      stop_pid "${supervisor_pid}" "simnow supervisor" || true
    done
  else
    echo "[ok] no supervise_simnow_trading.sh process found"
  fi
else
  supervisor_pids="$(find_supervisor_pids | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true)"
  if [[ -n "${supervisor_pids}" ]]; then
    echo "[warn] supervisor process is running (${supervisor_pids}); use --stop-supervisor or --all to prevent restart"
  fi
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
