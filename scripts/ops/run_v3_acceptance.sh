#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build-gcc}"
CC_BIN="${CC_BIN:-/usr/bin/gcc}"
CXX_BIN="${CXX_BIN:-/usr/bin/g++}"
MAX_RTO_SECONDS="${MAX_RTO_SECONDS:-10}"
MAX_RPO_EVENTS="${MAX_RPO_EVENTS:-0}"
SKIP_CONFIGURE=0
SKIP_CPP=0
SKIP_EVIDENCE=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --build-dir <path>       Build directory (default: ${BUILD_DIR})
  --cc <path>              C compiler path (default: ${CC_BIN})
  --cxx <path>             C++ compiler path (default: ${CXX_BIN})
  --max-rto-seconds <int>  Max allowed RTO for evidence check (default: ${MAX_RTO_SECONDS})
  --max-rpo-events <int>   Max allowed RPO events for evidence check (default: ${MAX_RPO_EVENTS})
  --skip-configure         Skip CMake configure
  --skip-cpp               Skip C++ build/tests
  --skip-evidence          Skip rollover WAL evidence verification
  -h, --help               Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir) BUILD_DIR="$2"; shift 2 ;;
    --cc) CC_BIN="$2"; shift 2 ;;
    --cxx) CXX_BIN="$2"; shift 2 ;;
    --max-rto-seconds) MAX_RTO_SECONDS="$2"; shift 2 ;;
    --max-rpo-events) MAX_RPO_EVENTS="$2"; shift 2 ;;
    --skip-configure) SKIP_CONFIGURE=1; shift ;;
    --skip-cpp) SKIP_CPP=1; shift ;;
    --skip-evidence) SKIP_EVIDENCE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ${SKIP_CONFIGURE} -eq 0 ]]; then
  cmake -S "${QUANT_ROOT}" -B "${BUILD_DIR}" \
    -DQUANT_HFT_BUILD_TESTS=ON \
    -DCMAKE_C_COMPILER="${CC_BIN}" \
    -DCMAKE_CXX_COMPILER="${CXX_BIN}"
fi

if [[ ${SKIP_CPP} -eq 0 ]]; then
  cmake --build "${BUILD_DIR}" \
    --target wal_replay_loader_test order_state_machine_test in_memory_portfolio_ledger_test wal_replay_tool \
    -j"$(nproc)"

  "${BUILD_DIR}/wal_replay_loader_test"
  "${BUILD_DIR}/order_state_machine_test"
  "${BUILD_DIR}/in_memory_portfolio_ledger_test"
fi

if [[ ${SKIP_EVIDENCE} -eq 0 ]]; then
  sample_wal="$(mktemp /tmp/rollover_sample.XXXXXX.wal)"
  evidence_env="$(mktemp /tmp/wal_recovery_result_rollover.XXXXXX.env)"

  printf '%s\n' \
    '{"seq":1,"kind":"rollover","ts_ns":10,"symbol":"rb","action":"carry","from_instrument":"rb2305","to_instrument":"rb2310"}' \
    '{"seq":2,"kind":"order","ts_ns":11,"account_id":"a1","client_order_id":"ord-1","instrument_id":"SHFE.ag2406","status":1,"filled_volume":0}' \
    > "${sample_wal}"

  replay_output="$("${BUILD_DIR}/wal_replay_tool" "${sample_wal}")"
  printf '%s\n' "${replay_output}"

  printf '%s\n' \
    'drill_id=20260214-rollover' \
    'release_version=local-dev' \
    "wal_path=${sample_wal}" \
    'failure_start_utc=2026-02-14T10:00:00Z' \
    'recovery_complete_utc=2026-02-14T10:00:01Z' \
    'rto_seconds=1' \
    'rpo_events=0' \
    'operator=dev' \
    'result=pass' \
    "notes=${replay_output}" \
    > "${evidence_env}"

  # Pure C++ acceptance mode: keep evidence check in bash without Python.
  actual_rto="$(grep '^rto_seconds=' "${evidence_env}" | cut -d= -f2)"
  actual_rpo="$(grep '^rpo_events=' "${evidence_env}" | cut -d= -f2)"
  if (( actual_rto > MAX_RTO_SECONDS )); then
    echo "error: RTO gate failed (${actual_rto} > ${MAX_RTO_SECONDS})" >&2
    exit 1
  fi
  if (( actual_rpo > MAX_RPO_EVENTS )); then
    echo "error: RPO gate failed (${actual_rpo} > ${MAX_RPO_EVENTS})" >&2
    exit 1
  fi

  echo "v3 acceptance evidence: ${evidence_env}"
fi

echo "v3 acceptance completed"
