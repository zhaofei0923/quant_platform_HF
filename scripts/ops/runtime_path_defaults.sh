#!/usr/bin/env bash
# Loaded after the caller sources its configured environment. Never eval resolver output.
load_runtime_path_defaults() {
  export CTP_SIM_MARKET_FRONT="${CTP_SIM_MARKET_FRONT:-tcp://182.254.243.31:30011}"
  export CTP_SIM_TRADER_FRONT="${CTP_SIM_TRADER_FRONT:-tcp://182.254.243.31:30001}"
  export CTP_SIM_IS_PRODUCTION_MODE="${CTP_SIM_IS_PRODUCTION_MODE:-true}"
  export CTP_SIM_ENABLE_REAL_API="${CTP_SIM_ENABLE_REAL_API:-true}"
  local resolver="${RUNTIME_PATHS_BIN:-${BUILD_DIR}/runtime_paths_cli}"
  [[ -x "${resolver}" ]] || die "runtime_paths_cli is required for identity defaults: ${resolver}; rebuild or specify explicit paths"
  local resolved key value
  resolved="$(QUANT_HFT_WAL_FILE="${WAL_FILE:-${QUANT_HFT_WAL_FILE:-}}" \
    SIMNOW_WAL_FILE="${WAL_FILE:-${SIMNOW_WAL_FILE:-}}" \
    QUANT_HFT_MARKET_DATA_DIR="${MARKET_DATA_DIR:-${QUANT_HFT_MARKET_DATA_DIR:-}}" \
    "${resolver}" --config "${CONFIG_PATH}")" || die "runtime identity path resolution failed"
  while IFS='=' read -r key value; do
    [[ -n "${value}" ]] || die "runtime_paths_cli returned an empty path"
    case "${key}" in
      recovery_root) RESOLVED_RECOVERY_ROOT="${value}" ;;
      wal_file) RESOLVED_WAL_FILE="${value}" ;;
      market_data_dir) RESOLVED_MARKET_DATA_DIR="${value}" ;;
      readiness_file) RESOLVED_READINESS_FILE="${value}" ;;
      run_root) RESOLVED_RUN_ROOT="${value}" ;;
      report_root) RESOLVED_REPORT_ROOT="${value}" ;;
      export_root) RESOLVED_EXPORT_ROOT="${value}" ;;
      reconcile_root) RESOLVED_RECONCILE_ROOT="${value}" ;;
      *) die "runtime_paths_cli returned an unsupported key" ;;
    esac
  done <<< "${resolved}"
  [[ -n "${RESOLVED_RECOVERY_ROOT:-}" && -n "${RESOLVED_WAL_FILE:-}" && \
     -n "${RESOLVED_MARKET_DATA_DIR:-}" && -n "${RESOLVED_READINESS_FILE:-}" && \
     -n "${RESOLVED_RUN_ROOT:-}" && -n "${RESOLVED_REPORT_ROOT:-}" && \
     -n "${RESOLVED_EXPORT_ROOT:-}" && -n "${RESOLVED_RECONCILE_ROOT:-}" ]] || die "runtime_paths_cli output is incomplete"
}
