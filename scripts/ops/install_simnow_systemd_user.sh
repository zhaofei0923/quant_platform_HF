#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
UNIT_NAME="quant-hft-simnow-trading.service"
UNIT_SRC="${SIMNOW_SYSTEMD_UNIT_SRC:-${QUANT_ROOT}/infra/systemd/${UNIT_NAME}}"
UNIT_DIR="${SYSTEMD_USER_UNIT_DIR:-${HOME}/.config/systemd/user}"
ENABLE=0
START=0
DISABLE=0
ENABLE_LINGER=0
DRY_RUN=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Install the user-level systemd unit for unattended SimNow trading.

Options:
  --unit-src <path>      Unit file to install (default: ${UNIT_SRC})
  --unit-dir <path>      User unit directory (default: ${UNIT_DIR})
  --enable               Enable service at user-session boot
  --start                Start service after install
  --enable-now           Enable and start service after install
  --disable              Stop and disable the installed service
  --enable-linger        Run loginctl enable-linger for the current user when available
  --dry-run              Print commands without changing systemd state
  -h, --help             Show this help
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

run_cmd() {
  printf '[cmd]'
  printf ' %q' "$@"
  printf '\n'
  if [[ ${DRY_RUN} -eq 0 ]]; then
    "$@"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --unit-src) require_value "$1" "${2:-}"; UNIT_SRC="$2"; shift 2 ;;
    --unit-dir) require_value "$1" "${2:-}"; UNIT_DIR="$2"; shift 2 ;;
    --enable) ENABLE=1; shift ;;
    --start) START=1; shift ;;
    --enable-now) ENABLE=1; START=1; shift ;;
    --disable) DISABLE=1; shift ;;
    --enable-linger) ENABLE_LINGER=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
[[ -f "${UNIT_SRC}" ]] || die "unit file not found: ${UNIT_SRC}"

if [[ ${DISABLE} -eq 1 ]]; then
  run_cmd systemctl --user disable --now "${UNIT_NAME}"
  run_cmd systemctl --user daemon-reload
  exit 0
fi

run_cmd mkdir -p "${UNIT_DIR}"
run_cmd install -m 0644 "${UNIT_SRC}" "${UNIT_DIR}/${UNIT_NAME}"
run_cmd systemctl --user daemon-reload

if [[ ${ENABLE_LINGER} -eq 1 ]]; then
  if command -v loginctl >/dev/null 2>&1; then
    run_cmd loginctl enable-linger "${USER}"
  else
    echo "[warn] loginctl is not available; skipping linger setup" >&2
  fi
fi

if [[ ${ENABLE} -eq 1 ]]; then
  run_cmd systemctl --user enable "${UNIT_NAME}"
fi

if [[ ${START} -eq 1 ]]; then
  run_cmd systemctl --user start "${UNIT_NAME}"
fi

echo "[ok] installed ${UNIT_NAME} into ${UNIT_DIR}"