#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"

PYTHON_BIN="${PYTHON_BIN:-${QUANT_ROOT}/.venv/bin/python}"
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build}"
OUT_DIR="${OUT_DIR:-${QUANT_ROOT}/runtime/memory_diag/$(date +%Y%m%d_%H%M%S)}"

MODE="${MODE:-all}"
SAMPLE_INTERVAL_SEC="${SAMPLE_INTERVAL_SEC:-1}"

MAX_TICKS="${MAX_TICKS:-200000}"
HOTPATH_TICK_RATE="${HOTPATH_TICK_RATE:-8000}"
HOTPATH_ORDER_RATE="${HOTPATH_ORDER_RATE:-80}"
HOTPATH_DURATION_SEC="${HOTPATH_DURATION_SEC:-300}"
PYTHON_QUEUE_SIZE="${PYTHON_QUEUE_SIZE:-1000}"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --mode <name>               Run mode: all|pytest|minimal|hotpath (default: ${MODE})
  --python-bin <path>         Python executable (default: ${PYTHON_BIN})
  --build-dir <path>          CMake build directory (default: ${BUILD_DIR})
  --out-dir <path>            Output directory (default: ${OUT_DIR})
  --sample-interval <sec>     RSS sampling interval seconds (default: ${SAMPLE_INTERVAL_SEC})
  --max-ticks <int>           Replay max ticks for CLI run (default: ${MAX_TICKS})
  --hotpath-tick-rate <int>   hotpath_hybrid tick rate (default: ${HOTPATH_TICK_RATE})
  --hotpath-order-rate <int>  hotpath_hybrid order rate (default: ${HOTPATH_ORDER_RATE})
  --hotpath-duration <int>    hotpath_hybrid duration seconds (default: ${HOTPATH_DURATION_SEC})
  --python-queue-size <int>   hotpath_hybrid python queue size (default: ${PYTHON_QUEUE_SIZE})
  -h, --help                  Show this help

Examples:
  $0 --mode minimal
  $0 --mode pytest --sample-interval 1
  $0 --mode hotpath --hotpath-duration 600 --python-queue-size 200
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --python-bin)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --build-dir)
      BUILD_DIR="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --sample-interval)
      SAMPLE_INTERVAL_SEC="$2"
      shift 2
      ;;
    --max-ticks)
      MAX_TICKS="$2"
      shift 2
      ;;
    --hotpath-tick-rate)
      HOTPATH_TICK_RATE="$2"
      shift 2
      ;;
    --hotpath-order-rate)
      HOTPATH_ORDER_RATE="$2"
      shift 2
      ;;
    --hotpath-duration)
      HOTPATH_DURATION_SEC="$2"
      shift 2
      ;;
    --python-queue-size)
      PYTHON_QUEUE_SIZE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "error: python executable not found: ${PYTHON_BIN}" >&2
  exit 2
fi

mkdir -p "${OUT_DIR}"

capture_env() {
  {
    echo "timestamp=$(date -Is)"
    echo "mode=${MODE}"
    echo "quant_root=${QUANT_ROOT}"
    echo "python_bin=${PYTHON_BIN}"
    echo "build_dir=${BUILD_DIR}"
    echo "sample_interval_sec=${SAMPLE_INTERVAL_SEC}"
    echo "max_ticks=${MAX_TICKS}"
    echo "hotpath_tick_rate=${HOTPATH_TICK_RATE}"
    echo "hotpath_order_rate=${HOTPATH_ORDER_RATE}"
    echo "hotpath_duration_sec=${HOTPATH_DURATION_SEC}"
    echo "python_queue_size=${PYTHON_QUEUE_SIZE}"
    echo "kernel=$(uname -sr)"
  } > "${OUT_DIR}/environment.env"
}

capture_oom_snapshot() {
  local phase="$1"
  local output_file="${OUT_DIR}/oom_${phase}.log"
  if dmesg -T >/dev/null 2>&1; then
    dmesg -T | grep -Ei 'killed process|out of memory|oom' | tail -n 200 > "${output_file}" || true
  else
    echo "dmesg not accessible in current environment" > "${output_file}"
  fi
}

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
  local wrapper_pid=$!
  local target_pid="${wrapper_pid}"
  for _ in $(seq 1 20); do
    local maybe_child
    maybe_child="$(pgrep -P "${wrapper_pid}" | head -n 1 || true)"
    if [[ -n "${maybe_child}" ]]; then
      target_pid="${maybe_child}"
      break
    fi
    sleep 0.1
  done

  echo "wrapper_pid=${wrapper_pid}" >> "${OUT_DIR}/${label}.meta"
  echo "target_pid=${target_pid}" >> "${OUT_DIR}/${label}.meta"

  sample_rss_loop "${target_pid}" "${rss_file}" &
  local sampler_pid=$!

  wait "${wrapper_pid}"
  local cmd_status=$?
  wait "${sampler_pid}" || true
  set -e

  echo "exit_code=${cmd_status}" >> "${OUT_DIR}/${label}.meta"
  if [[ ${cmd_status} -ne 0 ]]; then
    echo "[warn] ${label} exited with status ${cmd_status}" >&2
  fi
}

run_pytest_minimal() {
  run_with_sampling \
    pytest_backtest_engine \
    "${PYTHON_BIN}" -m pytest \
    "${QUANT_ROOT}/python/tests/test_backtest_engine.py::test_backtest_engine_runs_with_python_strategy" \
    -q -s
}

run_pytest_suite() {
  run_with_sampling \
    pytest_backtest_replay \
    "${PYTHON_BIN}" -m pytest \
    "${QUANT_ROOT}/python/tests/test_backtest_replay.py::test_replay_emits_minute_bars_and_intents" \
    -q -s

  run_with_sampling \
    pytest_backtest_replay_cli \
    "${PYTHON_BIN}" -m pytest \
    "${QUANT_ROOT}/python/tests/test_backtest_replay_cli.py::test_replay_csv_core_sim_with_csv_emits_rollover_fields" \
    -q -s

  run_with_sampling \
    pytest_unified_strategy_engine \
    "${PYTHON_BIN}" -m pytest \
    "${QUANT_ROOT}/python/tests/test_unified_strategy_engine.py::test_unified_strategy_engine_runs_backtest_strategy" \
    -q -s
}

run_hotpath() {
  cmake --build "${BUILD_DIR}" --target hotpath_hybrid -j"$(nproc)"
  run_with_sampling \
    hotpath_hybrid \
    "${BUILD_DIR}/hotpath_hybrid" \
    --tick-rate "${HOTPATH_TICK_RATE}" \
    --order-rate "${HOTPATH_ORDER_RATE}" \
    --duration "${HOTPATH_DURATION_SEC}" \
    --python-queue-size "${PYTHON_QUEUE_SIZE}" \
    --output "${OUT_DIR}/hotpath_output.json"
}

capture_env
capture_oom_snapshot before

case "${MODE}" in
  minimal)
    run_pytest_minimal
    ;;
  pytest)
    run_pytest_suite
    ;;
  hotpath)
    run_hotpath
    ;;
  all)
    run_pytest_minimal
    run_pytest_suite
    run_hotpath
    ;;
  *)
    echo "error: invalid mode: ${MODE}" >&2
    exit 2
    ;;
esac

capture_oom_snapshot after

cat <<SUMMARY
Memory diagnosis run completed.
Output directory: ${OUT_DIR}
Key artifacts:
  - environment.env
  - oom_before.log / oom_after.log
  - *.rss.log (VmRSS/VmHWM timeline)
  - *.stderr.log (/usr/bin/time -v summary)
  - hotpath_output.json (if mode includes hotpath)
SUMMARY
