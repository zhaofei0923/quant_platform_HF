#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT
BUILD_DIR="${BUILD_DIR:-${QUANT_ROOT}/build-gcc}"
REPORT_ROOT="${SIMNOW_REPORT_ROOT:-${QUANT_ROOT}/runtime/trading/reports/simnow}"

TRADING_DAY="${TRADING_DAY:-}"
CTP_CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/prod/ctp.yaml}"
SETTLEMENT_BIN="${SETTLEMENT_BIN:-${BUILD_DIR}/daily_settlement}"
EVIDENCE_JSON="${EVIDENCE_JSON:-}"
DIFF_JSON="${DIFF_JSON:-}"
RUN_READINESS_GATES=0
BENCHMARK_RESULT_JSON="${BENCHMARK_RESULT_JSON:-}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-20}"
EXECUTE=0
STRICT_ORDER_TRADE_BACKFILL="${SETTLEMENT_STRICT_ORDER_TRADE_BACKFILL:-1}"

usage() {
  cat <<USAGE
Usage: $0 --trading-day <YYYYMMDD|YYYY-MM-DD> [options]

Options:
  --trading-day <value>           Trading day to settle
  --ctp-config-path <path>        CTP yaml path (default: ${CTP_CONFIG_PATH})
  --settlement-bin <path>         daily_settlement binary path (default: ${SETTLEMENT_BIN})
  --evidence-json <path>          Evidence json output path (default: ${EVIDENCE_JSON})
  --diff-json <path>              Reconcile diff json output path (default: ${DIFF_JSON})
  --run-readiness-gates           Run benchmark gate after settlement success
  --benchmark-result-json <path>  Benchmark report output path (default: ${BENCHMARK_RESULT_JSON})
  --benchmark-runs <int>          Benchmark runs (default: ${BENCHMARK_RUNS})
  --execute                       Execute for real (default: dry-run)
  --strict-order-trade-backfill  Require broker order/trade backfill (default)
  --allow-incomplete-backfill    Compatibility escape hatch; do not use for live settlement
  -h, --help                      Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --trading-day) TRADING_DAY="${2:-}"; shift 2 ;;
    --ctp-config-path) CTP_CONFIG_PATH="${2:-}"; shift 2 ;;
    --settlement-bin) SETTLEMENT_BIN="${2:-}"; shift 2 ;;
    --evidence-json) EVIDENCE_JSON="${2:-}"; shift 2 ;;
    --diff-json) DIFF_JSON="${2:-}"; shift 2 ;;
    --run-readiness-gates) RUN_READINESS_GATES=1; shift ;;
    --benchmark-result-json) BENCHMARK_RESULT_JSON="${2:-}"; shift 2 ;;
    --benchmark-runs) BENCHMARK_RUNS="${2:-}"; shift 2 ;;
    --execute) EXECUTE=1; shift ;;
    --strict-order-trade-backfill) STRICT_ORDER_TRADE_BACKFILL=1; shift ;;
    --allow-incomplete-backfill) STRICT_ORDER_TRADE_BACKFILL=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "${TRADING_DAY}" ]]; then
  echo "error: --trading-day is required" >&2
  exit 2
fi

EOD_DIR="${REPORT_ROOT}/${TRADING_DAY}"
EVIDENCE_JSON="${EVIDENCE_JSON:-${EOD_DIR}/daily_settlement_evidence.json}"
DIFF_JSON="${DIFF_JSON:-${EOD_DIR}/settlement_diff.json}"
BENCHMARK_RESULT_JSON="${BENCHMARK_RESULT_JSON:-${EOD_DIR}/daily_settlement_benchmark.json}"

mkdir -p "$(dirname "${EVIDENCE_JSON}")"
mkdir -p "$(dirname "${DIFF_JSON}")"
[[ "${STRICT_ORDER_TRADE_BACKFILL}" == "0" || "${STRICT_ORDER_TRADE_BACKFILL}" == "1" ]] || {
  echo "error: SETTLEMENT_STRICT_ORDER_TRADE_BACKFILL must be 0 or 1" >&2
  exit 2
}

if [[ ${EXECUTE} -eq 1 ]]; then
  rm -f -- "${EVIDENCE_JSON}"
  settlement_cmd=(
    "${SETTLEMENT_BIN}"
    --config "${CTP_CONFIG_PATH}"
    --trading-day "${TRADING_DAY}"
    --evidence-path "${EVIDENCE_JSON}"
    --diff-report-path "${DIFF_JSON}"
  )
  if [[ "${STRICT_ORDER_TRADE_BACKFILL}" == "1" ]]; then
    settlement_cmd+=(--strict-order-trade-backfill)
  fi
  "${settlement_cmd[@]}"

  [[ -s "${EVIDENCE_JSON}" ]] || {
    echo "error: daily settlement binary did not publish evidence: ${EVIDENCE_JSON}" >&2
    exit 1
  }
  grep -Eq '"evidence_complete"[[:space:]]*:[[:space:]]*true' "${EVIDENCE_JSON}" || {
    echo "error: daily settlement evidence is incomplete: ${EVIDENCE_JSON}" >&2
    exit 1
  }
  grep -Eq '"valid_success"[[:space:]]*:[[:space:]]*true' "${EVIDENCE_JSON}" || {
    echo "error: daily settlement evidence is not a validated success: ${EVIDENCE_JSON}" >&2
    exit 1
  }
  grep -Eq '"status"[[:space:]]*:[[:space:]]*"COMPLETED"' "${EVIDENCE_JSON}" || {
    echo "error: daily settlement evidence status is not COMPLETED: ${EVIDENCE_JSON}" >&2
    exit 1
  }
  [[ -s "${DIFF_JSON}" ]] || {
    echo "error: daily settlement binary did not publish reconcile diff: ${DIFF_JSON}" >&2
    exit 1
  }
else
  strict_arg=""
  if [[ "${STRICT_ORDER_TRADE_BACKFILL}" == "1" ]]; then
    strict_arg=" --strict-order-trade-backfill"
  fi
  echo "[dry-run] ${SETTLEMENT_BIN} --config ${CTP_CONFIG_PATH} --trading-day ${TRADING_DAY} --evidence-path ${EVIDENCE_JSON} --diff-report-path ${DIFF_JSON}${strict_arg}"
fi

if [[ ${RUN_READINESS_GATES} -eq 1 ]]; then
  mkdir -p "$(dirname "${BENCHMARK_RESULT_JSON}")"
  "${BUILD_DIR}/backtest_benchmark_cli" \
    --runs "${BENCHMARK_RUNS}" \
    --baseline_p95_ms 100 \
    --result_json "${BENCHMARK_RESULT_JSON}"
fi
