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

Retire or disable the legacy direct user-level SimNow unit.

This installer no longer installs, enables, or starts trading. Formal deployments use the
packaged quant-hft-simnow-account@.service after an operator reviews the exact account_ref.

Options:
  --unit-src <path>      Accepted for compatibility; never installed
  --unit-dir <path>      Accepted for compatibility; never written
  --enable               Retired; exits without changing systemd state
  --start                Retired; exits without changing systemd state
  --enable-now           Retired; exits without changing systemd state
  --disable              Stop and disable the installed service
  --enable-linger        Retired; exits without changing systemd state
  --dry-run              With --disable, print commands without changing systemd state
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

if [[ ${DISABLE} -eq 1 ]]; then
  [[ ${ENABLE} -eq 0 && ${START} -eq 0 && ${ENABLE_LINGER} -eq 0 ]] ||
    die "--disable cannot be combined with retired install/start options"
  command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
  run_cmd systemctl --user disable --now "${UNIT_NAME}"
  run_cmd systemctl --user daemon-reload
  echo "[ok] disabled legacy ${UNIT_NAME}; no replacement account was inferred"
  exit 0
fi

die "legacy ${UNIT_NAME} installation is retired; use the packaged quant-hft-simnow-account@.service with an explicitly reviewed account_ref (no account is inferred)"
