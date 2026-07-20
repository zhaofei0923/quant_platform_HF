#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

ENV_FILE="${ENV_FILE:-${QUANT_ROOT}/runtime/simnow.env}"
CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/sim/ctp_sim_trade_candidates.yaml}"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build-gcc}"
RUN_ROOT="${SIMNOW_RUN_ROOT:-${QUANT_ROOT}/runtime/trading/runs/simnow}"
MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${QUANT_ROOT}/runtime/market_data/simnow}"
WAL_FILE="${SIMNOW_WAL_FILE:-${QUANT_ROOT}/runtime/trading/wal/simnow/events.wal}"
REPORT_ROOT="${SIMNOW_REPORT_ROOT:-${QUANT_ROOT}/runtime/trading/reports/simnow}"
EXPORT_ROOT="${SIMNOW_EXPORT_ROOT:-${QUANT_ROOT}/runtime/trading/exports/simnow}"
RECONCILE_ROOT="${SIMNOW_RECONCILE_ROOT:-${QUANT_ROOT}/runtime/trading/reconcile/simnow}"
OUTPUT_ROOT="${SIMNOW_PREFLIGHT_OUTPUT_ROOT:-${QUANT_ROOT}/runtime/trading/reports/simnow/preflight}"
SUPERVISE_SCRIPT="${SIMNOW_SUPERVISE_SCRIPT:-${SCRIPT_DIR}/supervise_simnow_trading.sh}"
START_SCRIPT="${SIMNOW_START_SCRIPT:-${SCRIPT_DIR}/start_simnow_trading.sh}"
STOP_SCRIPT="${SIMNOW_STOP_SCRIPT:-${SCRIPT_DIR}/stop_simnow_trading.sh}"
MONITOR_SCRIPT="${SIMNOW_MONITOR_SCRIPT:-${SCRIPT_DIR}/monitor_simnow_trading.sh}"
TRADING_WINDOWS="${SIMNOW_TRADING_WINDOWS:-night=21:00-02:35,day_am=09:00-11:35,day_pm=13:30-15:20}"
EXPECTED_WINDOWS="${SIMNOW_PREFLIGHT_EXPECTED_WINDOWS:-night=21:00-02:35,day_am=09:00-11:35,day_pm=13:30-15:20}"
EXPECTED_PRODUCTS="${SIMNOW_PREFLIGHT_EXPECTED_PRODUCTS:-c,hc}"
EOD_TIME="${SIMNOW_EOD_TIME:-15:25}"
STRICT_RECONCILE="${SIMNOW_STRICT_RECONCILE:-0}"
TEST_DATE="${SIMNOW_PREFLIGHT_DATE:-$(date -d '+1 day' +%Y%m%d)}"
PHASE="prestart"
TAIL_LINES=120
ORDER_REJECT_LIMIT=10
RUN_BUILD=1
RUN_GATES=1
RUN_TESTS=1
RUN_PROBE=1
REQUIRE_CLEAN=1

ENV_FILE_SET_BY_CLI=0
CONFIG_SET_BY_CLI=0
BUILD_DIR_SET_BY_CLI=0
RUN_ROOT_SET_BY_CLI=0
MARKET_DATA_DIR_SET_BY_CLI=0
WAL_FILE_SET_BY_CLI=0
REPORT_ROOT_SET_BY_CLI=0
EXPORT_ROOT_SET_BY_CLI=0
RECONCILE_ROOT_SET_BY_CLI=0
OUTPUT_ROOT_SET_BY_CLI=0
WINDOWS_SET_BY_CLI=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Run SimNow continuous-run readiness checks without printing credentials.

Options:
  --phase <prestart|post-start|eod>
                                  Check phase (default: ${PHASE})
  --test-date <YYYYMMDD|YYYY-MM-DD>
                                  Trading day under test (default: ${TEST_DATE})
  --env-file <path>               Environment file (default: ${ENV_FILE})
  --config <path>                 CTP yaml config (default: ${CONFIG_PATH})
  --build-dir <path>              Build directory (default: ${BUILD_DIR})
  --run-root <path>               SimNow run root (default: ${RUN_ROOT})
  --market-data-dir <path>        Market data root (default: ${MARKET_DATA_DIR})
  --wal-file <path>               WAL path (default: ${WAL_FILE})
  --report-root <path>            EOD report root (default: ${REPORT_ROOT})
  --export-root <path>            EOD export root (default: ${EXPORT_ROOT})
  --reconcile-root <path>         EOD reconcile root (default: ${RECONCILE_ROOT})
  --output-root <path>            Preflight report root (default: ${OUTPUT_ROOT})
  --windows <spec>                Expected supervisor windows (default: ${TRADING_WINDOWS})
  --expected-products <csv>       Required product_ids in config (default: ${EXPECTED_PRODUCTS})
  --tail-lines <int>              Core log tail lines for post-start (default: ${TAIL_LINES})
  --order-reject-limit <int>      Max order_rejected lines in post-start tail (default: ${ORDER_REJECT_LIMIT})
  --skip-build                    Do not build required binaries
  --skip-gates                    Do not run repo/dependency gates
  --skip-tests                    Do not run focused CTest regression
  --skip-probe                    Do not run real start_simnow_trading.sh --probe-only
  --allow-dirty                   Do not require a clean git worktree
  -h, --help                      Show this help

Recommended run before 2026-05-18:
  $0 --test-date 20260518 --phase prestart

Follow-up checks:
  $0 --test-date 20260518 --phase post-start
  $0 --test-date 20260518 --phase eod
USAGE
}

die() {
  echo "[fail] $*" >&2
  exit 1
}

warn() {
  echo "[warn] $*" >&2
}

info() {
  echo "[info] $*"
}

is_non_negative_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

require_value() {
  local option_name="$1"
  local option_value="${2:-}"
  [[ -n "${option_value}" ]] || die "${option_name} requires a value"
}

abs_path() {
  local path="$1"
  if [[ "${path}" == /* ]]; then
    printf '%s\n' "${path}"
  else
    printf '%s/%s\n' "${QUANT_ROOT}" "${path}"
  fi
}

normalize_date() {
  local input="$1"
  date -d "${input}" +%Y%m%d 2>/dev/null || return 1
}

date_dash() {
  local compact="$1"
  date -d "${compact}" +%F 2>/dev/null || return 1
}

next_weekday_compact() {
  local compact="$1"
  local dash
  local offset
  local candidate
  local weekday

  dash="$(date_dash "${compact}")" || return 1
  for offset in $(seq 1 7); do
    candidate="$(date -d "${dash} +${offset} day" +%Y%m%d)" || return 1
    weekday="$(date -d "${candidate}" +%u)" || return 1
    if [[ "${weekday}" =~ ^[1-5]$ ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

run_step() {
  local name="$1"
  shift
  local log_file="${REPORT_DIR}/${name}.log"

  info "step=${name}"
  if "$@" > "${log_file}" 2>&1; then
    echo "[ok] ${name}"
    return 0
  fi

  echo "[fail] ${name}; log=${log_file}" >&2
  tail -n 80 "${log_file}" >&2 || true
  exit 1
}

require_contains() {
  local file_path="$1"
  local pattern="$2"
  local label="$3"
  if ! grep -Fq -- "${pattern}" "${file_path}"; then
    echo "[fail] ${label}; missing pattern: ${pattern}" >&2
    echo "[fail] log=${file_path}" >&2
    tail -n 80 "${file_path}" >&2 || true
    exit 1
  fi
}

source_env_file() {
  [[ -f "${ENV_FILE}" ]] || die "env file not found: ${ENV_FILE}"
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a

  if [[ ${CONFIG_SET_BY_CLI} -eq 0 ]]; then
    CONFIG_PATH="${CTP_CONFIG_PATH:-${CONFIG_PATH}}"
  fi
  if [[ ${BUILD_DIR_SET_BY_CLI} -eq 0 ]]; then
    BUILD_DIR="${BUILD_DIR:-${BUILD_DIR}}"
  fi
  if [[ ${RUN_ROOT_SET_BY_CLI} -eq 0 ]]; then
    RUN_ROOT="${SIMNOW_RUN_ROOT:-${RUN_ROOT}}"
  fi
  if [[ ${MARKET_DATA_DIR_SET_BY_CLI} -eq 0 ]]; then
    MARKET_DATA_DIR="${SIMNOW_MARKET_DATA_DIR:-${MARKET_DATA_DIR}}"
  fi
  if [[ ${WAL_FILE_SET_BY_CLI} -eq 0 ]]; then
    WAL_FILE="${SIMNOW_WAL_FILE:-${WAL_FILE}}"
  fi
  if [[ ${REPORT_ROOT_SET_BY_CLI} -eq 0 ]]; then
    REPORT_ROOT="${SIMNOW_REPORT_ROOT:-${REPORT_ROOT}}"
  fi
  if [[ ${EXPORT_ROOT_SET_BY_CLI} -eq 0 ]]; then
    EXPORT_ROOT="${SIMNOW_EXPORT_ROOT:-${EXPORT_ROOT}}"
  fi
  if [[ ${RECONCILE_ROOT_SET_BY_CLI} -eq 0 ]]; then
    RECONCILE_ROOT="${SIMNOW_RECONCILE_ROOT:-${RECONCILE_ROOT}}"
  fi
  if [[ ${OUTPUT_ROOT_SET_BY_CLI} -eq 0 ]]; then
    OUTPUT_ROOT="${SIMNOW_PREFLIGHT_OUTPUT_ROOT:-${OUTPUT_ROOT}}"
  fi
  if [[ ${WINDOWS_SET_BY_CLI} -eq 0 ]]; then
    TRADING_WINDOWS="${SIMNOW_TRADING_WINDOWS:-${TRADING_WINDOWS}}"
  fi
  EOD_TIME="${SIMNOW_EOD_TIME:-${EOD_TIME}}"
  STRICT_RECONCILE="${SIMNOW_STRICT_RECONCILE:-${STRICT_RECONCILE}}"
}

write_summary() {
  cat > "${REPORT_DIR}/summary.env" <<EOF
phase=${PHASE}
test_date=${TEST_DATE_COMPACT}
env_file=${ENV_FILE}
config=${CONFIG_PATH}
build_dir=${BUILD_DIR}
run_root=${RUN_ROOT}
market_data_dir=${MARKET_DATA_DIR}
wal_file=${WAL_FILE}
report_root=${REPORT_ROOT}
export_root=${EXPORT_ROOT}
reconcile_root=${RECONCILE_ROOT}
trading_windows=${TRADING_WINDOWS}
eod_time=${EOD_TIME}
run_build=${RUN_BUILD}
run_gates=${RUN_GATES}
run_tests=${RUN_TESTS}
run_probe=${RUN_PROBE}
EOF
}

config_products() {
  [[ -f "${CONFIG_PATH}" ]] || return 1
  grep -E '^[[:space:]]*product_ids:' "${CONFIG_PATH}" | head -n 1 |
    sed -E "s/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/\"//g; s/'//g; s/[[:space:]]//g"
}

validate_env_prestart() {
  local log_file="${REPORT_DIR}/env_check.log"
  local key
  local products

  [[ -f "${CONFIG_PATH}" ]] || die "config file not found: ${CONFIG_PATH}"
  [[ "${CTP_SIM_IS_PRODUCTION_MODE:-}" == "true" ]] || die "CTP_SIM_IS_PRODUCTION_MODE must be true"
  [[ "${CTP_SIM_ENABLE_REAL_API:-}" == "true" ]] || die "CTP_SIM_ENABLE_REAL_API must be true"
  case "${CTP_SIM_MARKET_FRONT:-}|${CTP_SIM_TRADER_FRONT:-}" in
    tcp://182.254.243.31:30011\|tcp://182.254.243.31:30001|\
    tcp://182.254.243.31:30012\|tcp://182.254.243.31:30002|\
    tcp://182.254.243.31:30013\|tcp://182.254.243.31:30003)
      ;;
    *)
      die "CTP_SIM_MARKET_FRONT/CTP_SIM_TRADER_FRONT must use a SimNow trading-hours group"
      ;;
  esac

  for key in CTP_SIM_BROKER_ID CTP_SIM_USER_ID CTP_SIM_INVESTOR_ID CTP_SIM_PASSWORD \
    CTP_SIM_AUTH_CODE CTP_SIM_APP_ID; do
    [[ -n "${!key:-}" ]] || die "${key} is missing"
  done

  [[ "${TRADING_WINDOWS}" == "${EXPECTED_WINDOWS}" ]] ||
    die "SIMNOW_TRADING_WINDOWS mismatch: expected ${EXPECTED_WINDOWS}, got ${TRADING_WINDOWS}"

  products="$(config_products || true)"
  [[ "${products}" == "${EXPECTED_PRODUCTS}" ]] ||
    die "product_ids mismatch: expected ${EXPECTED_PRODUCTS}, got ${products:-<missing>}"

  {
    echo "CTP_SIM_IS_PRODUCTION_MODE=true"
    echo "CTP_SIM_ENABLE_REAL_API=true"
    echo "CTP_SIM_MARKET_FRONT=${CTP_SIM_MARKET_FRONT}"
    echo "CTP_SIM_TRADER_FRONT=${CTP_SIM_TRADER_FRONT}"
    echo "SIMNOW_TRADING_WINDOWS=${TRADING_WINDOWS}"
    echo "product_ids=${products}"
    for key in CTP_SIM_BROKER_ID CTP_SIM_USER_ID CTP_SIM_INVESTOR_ID CTP_SIM_PASSWORD \
      CTP_SIM_AUTH_CODE CTP_SIM_APP_ID; do
      echo "${key}=<set>"
    done
  } > "${log_file}"
  echo "[ok] env_check"
}

require_clean_worktree() {
  run_step git_status git status --short --branch
  if [[ -n "$(git status --porcelain)" ]]; then
    die "git worktree is not clean; rerun with --allow-dirty only for local dry tests"
  fi
}

verify_cmake_cache() {
  local cache_file="${BUILD_DIR}/CMakeCache.txt"
  [[ -f "${cache_file}" ]] || die "CMake cache not found: ${cache_file}"
  if command -v rg >/dev/null 2>&1; then
    run_step cmake_cache_flags rg -n 'QUANT_HFT_ENABLE_CTP_REAL_API|QUANT_HFT_BUILD_TESTS' "${cache_file}"
  else
    run_step cmake_cache_flags grep -nE 'QUANT_HFT_ENABLE_CTP_REAL_API|QUANT_HFT_BUILD_TESTS' "${cache_file}"
  fi
  grep -Fxq 'QUANT_HFT_ENABLE_CTP_REAL_API:BOOL=ON' "${cache_file}" ||
    die "build dir must be configured with QUANT_HFT_ENABLE_CTP_REAL_API=ON"
  grep -Fxq 'QUANT_HFT_BUILD_TESTS:BOOL=ON' "${cache_file}" ||
    die "build dir must be configured with QUANT_HFT_BUILD_TESTS=ON"
}

run_hard_checks() {
  if [[ ${REQUIRE_CLEAN} -eq 1 ]]; then
    require_clean_worktree
  else
    run_step git_status git status --short --branch
  fi

  verify_cmake_cache

  if [[ ${RUN_BUILD} -eq 1 ]]; then
    run_step build_required_targets cmake --build "${BUILD_DIR}" --target core_engine simnow_probe \
      simnow_contract_universe_refresh \
      daily_settlement simnow_wal_export_cli ops_health_report_cli ops_alert_report_cli \
      simnow_dashboard_cli "-j$(nproc)"
  fi

  if [[ ${RUN_GATES} -eq 1 ]]; then
    run_step git_diff_check git diff --check
    run_step repo_purity_check bash scripts/build/repo_purity_check.sh --repo-root .
    run_step dependency_audit bash scripts/build/dependency_audit.sh --build-dir "${BUILD_DIR}"
  fi

  if [[ ${RUN_TESTS} -eq 1 ]]; then
    run_step focused_ctest ctest --test-dir "${BUILD_DIR}" -R \
      '(CtpConfig|CtpConfigLoader|CtpGatewayAdapter|CTPTraderAdapter|CTPMdAdapter|WalReplayLoader|StrategyEngine|ExecutionEngine|SimnowSupervisorScriptTest|PreprodRehearsalGateScriptTest)' \
      --output-on-failure
  fi
}

find_live_simnow_processes() {
  local process_pid
  local args
  while read -r process_pid args; do
    [[ -n "${process_pid:-}" ]] || continue
    [[ "${process_pid}" == "$$" || "${process_pid}" == "${BASHPID}" ]] && continue
    [[ "${args:-}" == *"run_simnow_preflight_check.sh"* ]] && continue
    if [[ "${args:-}" == *"supervise_simnow_trading.sh"* || "${args:-}" == *"/core_engine"* ||
          "${args:-}" == *"simnow_probe"* ]]; then
      printf '%s %s\n' "${process_pid}" "${args}"
    fi
  done < <(ps -eo pid=,args=)
}

check_no_existing_processes() {
  local log_file="${REPORT_DIR}/process_check.log"
  find_live_simnow_processes > "${log_file}" || true
  if [[ -s "${log_file}" ]]; then
    echo "[fail] unexpected SimNow process is already running; log=${log_file}" >&2
    cat "${log_file}" >&2
    exit 1
  fi
  echo "[ok] process_check"
  run_step stop_dry_run "${STOP_SCRIPT}" --all --dry-run --run-root "${RUN_ROOT}" --config "${CONFIG_PATH}"
}

run_supervisor_schedule_check() {
  local label="$1"
  local fake_now="$2"
  local log_name="schedule_${label}"
  shift 2

  run_step "${log_name}" env SIMNOW_FAKE_NOW="${fake_now}" "${SUPERVISE_SCRIPT}" \
    --env-file "${ENV_FILE}" \
    --config "${CONFIG_PATH}" \
    --build-dir "${BUILD_DIR}" \
    --run-root "${RUN_ROOT}" \
    --market-data-dir "${MARKET_DATA_DIR}" \
    --wal-file "${WAL_FILE}" \
    --report-root "${REPORT_ROOT}" \
    --export-root "${EXPORT_ROOT}" \
    --reconcile-root "${RECONCILE_ROOT}" \
    --windows "${TRADING_WINDOWS}" \
    --dry-run

  for pattern in "$@"; do
    require_contains "${REPORT_DIR}/${log_name}.log" "${pattern}" "${log_name}"
  done
}

run_schedule_checks() {
  local next_trading_day
  next_trading_day="$(next_weekday_compact "${TEST_DATE_COMPACT}")"

  run_step schedule_now "${SUPERVISE_SCRIPT}" \
    --env-file "${ENV_FILE}" \
    --config "${CONFIG_PATH}" \
    --build-dir "${BUILD_DIR}" \
    --run-root "${RUN_ROOT}" \
    --market-data-dir "${MARKET_DATA_DIR}" \
    --wal-file "${WAL_FILE}" \
    --report-root "${REPORT_ROOT}" \
    --export-root "${EXPORT_ROOT}" \
    --reconcile-root "${RECONCILE_ROOT}" \
    --windows "${TRADING_WINDOWS}" \
    --dry-run

  run_supervisor_schedule_check day_am "${TEST_DATE_DASH} 09:01:00" \
    "decision=start_or_keep_alive" "trading_day=${TEST_DATE_COMPACT}" "range=09:00-11:35" \
    "${START_SCRIPT}" "${CONFIG_PATH}"
  run_supervisor_schedule_check day_pm "${TEST_DATE_DASH} 13:31:00" \
    "decision=start_or_keep_alive" "trading_day=${TEST_DATE_COMPACT}" "range=13:30-15:20" \
    "${START_SCRIPT}" "${CONFIG_PATH}"
  run_supervisor_schedule_check eod "${TEST_DATE_DASH} 15:26:00" \
    "decision=outside_trading_window" "eod_due=true trading_day=${TEST_DATE_COMPACT}"
  run_supervisor_schedule_check night "${TEST_DATE_DASH} 21:01:00" \
    "decision=start_or_keep_alive" "trading_day=${next_trading_day}" "range=21:00-02:35" \
    "${START_SCRIPT}" "${CONFIG_PATH}"
}

run_start_dry_run() {
  run_step start_dry_run "${START_SCRIPT}" \
    --env-file "${ENV_FILE}" \
    --config "${CONFIG_PATH}" \
    --build-dir "${BUILD_DIR}" \
    --run-root "${RUN_ROOT}" \
    --wal-file "${WAL_FILE}" \
    --run-id "${START_DRY_RUN_ID}" \
    --run-seconds 60 \
    --dry-run

  require_contains "${REPORT_DIR}/start_dry_run.log" "duplicate check: no live core_engine detected" \
    "start_dry_run"
  require_contains "${REPORT_DIR}/start_dry_run.log" "${BUILD_DIR}/core_engine" "start_dry_run"
  require_contains "${REPORT_DIR}/start_dry_run.log" "${CONFIG_PATH}" "start_dry_run"
}

run_probe_only() {
  if [[ ${RUN_PROBE} -eq 0 ]]; then
    echo "[skip] probe_only"
    return 0
  fi

  run_step probe_only "${START_SCRIPT}" \
    --env-file "${ENV_FILE}" \
    --config "${CONFIG_PATH}" \
    --build-dir "${BUILD_DIR}" \
    --run-root "${RUN_ROOT}" \
    --wal-file "${WAL_FILE}" \
    --run-id "${PROBE_RUN_ID}" \
    --probe-only \
    --preopen-connectivity-only

  require_contains "${REPORT_DIR}/probe_only.log" "[ok] probe-only completed" "probe_only"

  local probe_log
  probe_log="$(abs_path "${RUN_ROOT}")/${PROBE_RUN_ID}/simnow_probe.log"
  [[ -s "${probe_log}" ]] || die "probe log missing: ${probe_log}"
  grep -Fq 'event=probe_completed' "${probe_log}" || die "probe log does not contain probe_completed"
  grep -Fq 'event=health_status state="healthy"' "${probe_log}" ||
    die "probe log does not contain healthy status"
  grep -Fq 'event=dominant_contract_probe_summary mode="preopen_connectivity_only"' \
    "${probe_log}" || die "probe log does not contain preopen dominant summary"
  if grep -Eiq 'connect_failed|auth|authenticate|subscribe_failed|instrument_.*timeout|level=error' \
    "${probe_log}"; then
    die "probe log contains failure events: ${probe_log}"
  fi
}

run_prestart_phase() {
  run_hard_checks
  validate_env_prestart
  check_no_existing_processes
  run_schedule_checks
  run_start_dry_run
  run_probe_only
  echo "[ok] prestart checks passed"
}

run_post_start_phase() {
  local run_root_abs
  local wal_file_abs
  local engine_log
  local tail_file="${REPORT_DIR}/core_engine_tail.log"
  local reject_count

  run_root_abs="$(abs_path "${RUN_ROOT}")"
  wal_file_abs="$(abs_path "${WAL_FILE}")"

  run_step monitor_strict "${MONITOR_SCRIPT}" \
    --run-root "${RUN_ROOT}" \
    --market-data-dir "${MARKET_DATA_DIR}" \
    --wal-file "${WAL_FILE}" \
    --config "${CONFIG_PATH}" \
    --strict-exit
  require_contains "${REPORT_DIR}/monitor_strict.log" "core_engine=alive" "monitor_strict"

  if [[ -f "${wal_file_abs}" ]]; then
    [[ -w "${wal_file_abs}" ]] || die "WAL file is not writable: ${wal_file_abs}"
  else
    [[ -w "$(dirname "${wal_file_abs}")" ]] || die "WAL directory is not writable: $(dirname "${wal_file_abs}")"
  fi

  [[ -f "${run_root_abs}/current_core_engine_log" ]] ||
    die "current core log pointer missing: ${run_root_abs}/current_core_engine_log"
  engine_log="$(cat "${run_root_abs}/current_core_engine_log")"
  engine_log="$(abs_path "${engine_log}")"
  [[ -f "${engine_log}" ]] || die "current core log missing: ${engine_log}"
  tail -n "${TAIL_LINES}" "${engine_log}" > "${tail_file}"

  if grep -Eiq 'level=(critical|fatal)|connect_failed|subscribe_failed' "${tail_file}"; then
    die "core_engine tail contains critical connection/subscription failures: ${tail_file}"
  fi
  reject_count="$(grep -c 'order_rejected' "${tail_file}" || true)"
  if (( reject_count > ORDER_REJECT_LIMIT )); then
    die "core_engine tail contains ${reject_count} order_rejected lines, limit=${ORDER_REJECT_LIMIT}"
  fi

  echo "[ok] post-start checks passed"
}

run_eod_phase() {
  local run_root_abs
  local report_root_abs
  local day_report_dir
  local marker_file
  local required_file
  local export_log

  run_root_abs="$(abs_path "${RUN_ROOT}")"
  report_root_abs="$(abs_path "${REPORT_ROOT}")"
  day_report_dir="${report_root_abs}/${TEST_DATE_COMPACT}"
  marker_file="${run_root_abs}/eod/${TEST_DATE_COMPACT}.done"

  [[ -s "${marker_file}" ]] || die "EOD marker missing: ${marker_file}"
  for required_file in \
    "${day_report_dir}/daily_settlement_evidence.json" \
    "${day_report_dir}/settlement_diff.json" \
    "${day_report_dir}/simnow_daily_report.md" \
    "${day_report_dir}/ops_health_report.json" \
    "${day_report_dir}/ops_alert_report.json"; do
    [[ -s "${required_file}" ]] || die "required EOD artifact missing or empty: ${required_file}"
  done

  if [[ -f "${day_report_dir}/daily_settlement.log" ]] &&
    grep -Eiq 'critical|error:|failed' "${day_report_dir}/daily_settlement.log"; then
    die "daily settlement log contains failure text: ${day_report_dir}/daily_settlement.log"
  fi

  export_log="${day_report_dir}/simnow_export.log"
  if [[ -f "${export_log}" ]] && grep -Eiq 'critical|error:|failed' "${export_log}"; then
    if [[ "${STRICT_RECONCILE}" == "1" ]]; then
      die "SimNow export log contains failure text under strict reconcile: ${export_log}"
    fi
    warn "SimNow export log contains warning/failure text but strict_reconcile=0: ${export_log}"
  fi

  echo "[ok] eod checks passed"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --phase) require_value "$1" "${2:-}"; PHASE="$2"; shift 2 ;;
    --test-date) require_value "$1" "${2:-}"; TEST_DATE="$2"; shift 2 ;;
    --env-file) require_value "$1" "${2:-}"; ENV_FILE="$2"; ENV_FILE_SET_BY_CLI=1; shift 2 ;;
    --config|--ctp-config-path) require_value "$1" "${2:-}"; CONFIG_PATH="$2"; CONFIG_SET_BY_CLI=1; shift 2 ;;
    --build-dir) require_value "$1" "${2:-}"; BUILD_DIR="$2"; BUILD_DIR_SET_BY_CLI=1; shift 2 ;;
    --run-root) require_value "$1" "${2:-}"; RUN_ROOT="$2"; RUN_ROOT_SET_BY_CLI=1; shift 2 ;;
    --market-data-dir) require_value "$1" "${2:-}"; MARKET_DATA_DIR="$2"; MARKET_DATA_DIR_SET_BY_CLI=1; shift 2 ;;
    --wal-file) require_value "$1" "${2:-}"; WAL_FILE="$2"; WAL_FILE_SET_BY_CLI=1; shift 2 ;;
    --report-root) require_value "$1" "${2:-}"; REPORT_ROOT="$2"; REPORT_ROOT_SET_BY_CLI=1; shift 2 ;;
    --export-root) require_value "$1" "${2:-}"; EXPORT_ROOT="$2"; EXPORT_ROOT_SET_BY_CLI=1; shift 2 ;;
    --reconcile-root) require_value "$1" "${2:-}"; RECONCILE_ROOT="$2"; RECONCILE_ROOT_SET_BY_CLI=1; shift 2 ;;
    --output-root) require_value "$1" "${2:-}"; OUTPUT_ROOT="$2"; OUTPUT_ROOT_SET_BY_CLI=1; shift 2 ;;
    --windows) require_value "$1" "${2:-}"; TRADING_WINDOWS="$2"; EXPECTED_WINDOWS="$2"; WINDOWS_SET_BY_CLI=1; shift 2 ;;
    --expected-products) require_value "$1" "${2:-}"; EXPECTED_PRODUCTS="$2"; shift 2 ;;
    --tail-lines) require_value "$1" "${2:-}"; TAIL_LINES="$2"; shift 2 ;;
    --order-reject-limit) require_value "$1" "${2:-}"; ORDER_REJECT_LIMIT="$2"; shift 2 ;;
    --skip-build) RUN_BUILD=0; shift ;;
    --skip-gates) RUN_GATES=0; shift ;;
    --skip-tests) RUN_TESTS=0; shift ;;
    --skip-probe) RUN_PROBE=0; shift ;;
    --allow-dirty) REQUIRE_CLEAN=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "${PHASE}" in
  prestart|post-start|eod) ;;
  *) die "--phase must be one of: prestart, post-start, eod" ;;
esac
is_non_negative_int "${TAIL_LINES}" || die "--tail-lines must be a non-negative integer"
is_non_negative_int "${ORDER_REJECT_LIMIT}" || die "--order-reject-limit must be a non-negative integer"

TEST_DATE_COMPACT="$(normalize_date "${TEST_DATE}")" || die "invalid --test-date: ${TEST_DATE}"
TEST_DATE_DASH="$(date_dash "${TEST_DATE_COMPACT}")" || die "invalid --test-date: ${TEST_DATE}"
RUN_STAMP="$(date +%Y%m%dT%H%M%S)"
START_DRY_RUN_ID="${SIMNOW_PREFLIGHT_START_RUN_ID:-simnow-precheck-${TEST_DATE_COMPACT}-${RUN_STAMP}}"
PROBE_RUN_ID="${SIMNOW_PREFLIGHT_PROBE_RUN_ID:-simnow-probe-${TEST_DATE_COMPACT}-preopen-${RUN_STAMP}}"

cd "${QUANT_ROOT}"
source_env_file
REPORT_DIR="$(abs_path "${OUTPUT_ROOT}")/${TEST_DATE_COMPACT}-${PHASE}-${RUN_STAMP}"
mkdir -p "${REPORT_DIR}"
write_summary
info "report_dir=${REPORT_DIR}"

case "${PHASE}" in
  prestart) run_prestart_phase ;;
  post-start) run_post_start_phase ;;
  eod) run_eod_phase ;;
esac
