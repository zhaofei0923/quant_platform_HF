#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

ENV_FILE="${ENV_FILE:-${QUANT_ROOT}/runtime/simnow.env}"
CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/sim/ctp_sim_trade_candidates.yaml}"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build-gcc}"
REFRESH_BIN="${SIMNOW_CONTRACT_REFRESH_BIN:-${BUILD_DIR}/simnow_contract_universe_refresh}"
CACHE_ROOT="${SIMNOW_INSTRUMENT_CACHE_ROOT:-${QUANT_ROOT}/runtime/ctp_instruments}"
TIMEOUT_SECONDS="${SIMNOW_INSTRUMENT_TIMEOUT_SECONDS:-60}"
LOCK_FILE="${SIMNOW_CONTRACT_REFRESH_LOCK:-${QUANT_ROOT}/runtime/trading/locks/contract_universe_refresh.lock}"
FORCE=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Refresh the SimNow C/HC instrument universe with product-scoped CTP queries.

Options:
  --env-file <path>          Environment file (default: ${ENV_FILE})
  --config <path>            CTP config (default: ${CONFIG_PATH})
  --build-dir <path>         Build directory (default: ${BUILD_DIR})
  --refresh-bin <path>       Refresh binary (default: ${REFRESH_BIN})
  --cache-root <path>        Cache root (default: ${CACHE_ROOT})
  --timeout-seconds <int>    Timeout per query (default: ${TIMEOUT_SECONDS})
  --force                    Query even when the broker-day manifest is current
  -h, --help                 Show this help
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_value() {
  [[ -n "${2:-}" ]] || die "$1 requires a value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) require_value "$1" "${2:-}"; ENV_FILE="$2"; shift 2 ;;
    --config) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; shift 2 ;;
    --build-dir)
      require_value "$1" "${2:-}"
      BUILD_DIR="$2"
      REFRESH_BIN="${BUILD_DIR}/simnow_contract_universe_refresh"
      shift 2
      ;;
    --refresh-bin) require_value "$1" "${2:-}"; REFRESH_BIN="$2"; shift 2 ;;
    --cache-root) require_value "$1" "${2:-}"; CACHE_ROOT="$2"; shift 2 ;;
    --timeout-seconds) require_value "$1" "${2:-}"; TIMEOUT_SECONDS="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ "${TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]] || die "--timeout-seconds must be positive"
[[ -f "${ENV_FILE}" ]] || die "environment file not found: ${ENV_FILE}"
[[ -f "${CONFIG_PATH}" ]] || die "config file not found: ${CONFIG_PATH}"
[[ -x "${REFRESH_BIN}" ]] || die "refresh binary is not executable: ${REFRESH_BIN}"

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
export CTP_SIM_IS_PRODUCTION_MODE="${CTP_SIM_IS_PRODUCTION_MODE:-true}"
export CTP_SIM_ENABLE_REAL_API="${CTP_SIM_ENABLE_REAL_API:-true}"

for required in CTP_SIM_BROKER_ID CTP_SIM_USER_ID CTP_SIM_INVESTOR_ID CTP_SIM_PASSWORD \
  CTP_SIM_AUTH_CODE CTP_SIM_APP_ID CTP_SIM_MARKET_FRONT CTP_SIM_TRADER_FRONT; do
  [[ -n "${!required:-}" ]] || die "${required} is missing"
done

mkdir -p "$(dirname "${LOCK_FILE}")" "${CACHE_ROOT}"
if command -v flock >/dev/null 2>&1; then
  exec 8>"${LOCK_FILE}"
  flock -n 8 || die "another contract universe refresh is running: ${LOCK_FILE}"
fi

args=(--config "${CONFIG_PATH}" --cache-root "${CACHE_ROOT}" \
  --timeout-seconds "${TIMEOUT_SECONDS}")
if [[ ${FORCE} -eq 1 ]]; then
  args+=(--force)
fi

cd "${QUANT_ROOT}"
exec "${REFRESH_BIN}" "${args[@]}"
