#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build}"
OUT_DIR="${OUT_DIR:-${QUANT_ROOT}/runtime/memory_diag/$(date +%Y%m%d_%H%M%S)}"
MODE="${MODE:-all}"
SAMPLE_INTERVAL_SEC="${SAMPLE_INTERVAL_SEC:-1}"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --mode <name>               Run mode: all|minimal|hotpath (default: ${MODE})
  --build-dir <path>          CMake build directory (default: ${BUILD_DIR})
  --out-dir <path>            Output directory (default: ${OUT_DIR})
  --sample-interval <sec>     RSS sampling interval seconds (default: ${SAMPLE_INTERVAL_SEC})
  -h, --help                  Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --build-dir) BUILD_DIR="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    --sample-interval) SAMPLE_INTERVAL_SEC="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

mkdir -p "${OUT_DIR}"

sample_rss_loop() {
  local pid="$1"
  local output_file="$2"
  while kill -0 "${pid}" >/dev/null 2>&1; do
    local now
    now="$(date -Is)"
    local rss_hwm
    rss_hwm="$(awk '/VmRSS|VmHWM/ {print $1"=" $2 $3}' "/proc/${pid}/status" 2>/dev/null | tr '\n' ' ' || true)"
    printf '%s %s\n' "${now}" "${rss_hwm}" >> "${output_file}"
    sleep "${SAMPLE_INTERVAL_SEC}"
  done
}

run_with_sampling() {
  local label="$1"
  shift
  local stdout_file="${OUT_DIR}/${label}.stdout.log"
  local stderr_file="${OUT_DIR}/${label}.stderr.log"
  local rss_file="${OUT_DIR}/${label}.rss.log"

  echo "[run] ${label}: $*"
  set +e
  /usr/bin/time -v "$@" > "${stdout_file}" 2> "${stderr_file}" &
  local pid=$!
  sample_rss_loop "${pid}" "${rss_file}" &
  local sampler_pid=$!
  wait "${pid}"
  local rc=$?
  wait "${sampler_pid}" || true
  set -e
  echo "exit_code=${rc}" > "${OUT_DIR}/${label}.meta"
}

run_minimal() {
  cmake --build "${BUILD_DIR}" --target backtest_benchmark_cli -j"$(nproc)"
  run_with_sampling backtest_benchmark_cli \
    "${BUILD_DIR}/backtest_benchmark_cli" \
    --runs 5 \
    --baseline_p95_ms 100 \
    --result_json "${OUT_DIR}/backtest_benchmark_result.json"
}

run_hotpath() {
  cmake --build "${BUILD_DIR}" --target hotpath_benchmark -j"$(nproc)"
  run_with_sampling hotpath_benchmark "${BUILD_DIR}/hotpath_benchmark"
}

case "${MODE}" in
  minimal) run_minimal ;;
  hotpath) run_hotpath ;;
  all)
    run_minimal
    run_hotpath
    ;;
  *)
    echo "error: invalid mode: ${MODE}" >&2
    exit 2
    ;;
esac

echo "Memory diagnosis completed. Output: ${OUT_DIR}"
