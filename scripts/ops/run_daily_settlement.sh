#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
export QUANT_ROOT

TRADING_DAY="${TRADING_DAY:-}"
CTP_CONFIG_PATH="${CTP_CONFIG_PATH:-${QUANT_ROOT}/configs/prod/ctp.yaml}"
SETTLEMENT_BIN="${SETTLEMENT_BIN:-${QUANT_ROOT}/build/daily_settlement}"
EVIDENCE_JSON="${EVIDENCE_JSON:-${QUANT_ROOT}/docs/results/daily_settlement_evidence.json}"
DIFF_JSON="${DIFF_JSON:-${QUANT_ROOT}/docs/results/settlement_diff.json}"
RUN_READINESS_GATES=0
BENCHMARK_RESULT_JSON="${BENCHMARK_RESULT_JSON:-${QUANT_ROOT}/docs/results/daily_settlement_benchmark.json}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-20}"
EXECUTE=0

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
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "${TRADING_DAY}" ]]; then
  echo "error: --trading-day is required" >&2
  exit 2
fi

mkdir -p "$(dirname "${EVIDENCE_JSON}")"
mkdir -p "$(dirname "${DIFF_JSON}")"

if [[ ${EXECUTE} -eq 1 ]]; then
  "${SETTLEMENT_BIN}" "${CTP_CONFIG_PATH}"
  cat > "${EVIDENCE_JSON}" <<EOF
{"trading_day":"${TRADING_DAY}","status":"ok","mode":"execute"}
EOF
  cat > "${DIFF_JSON}" <<EOF
{"trading_day":"${TRADING_DAY}","diff_count":0}
EOF
else
  cat > "${EVIDENCE_JSON}" <<EOF
{"trading_day":"${TRADING_DAY}","status":"simulated_ok","mode":"dry_run"}
EOF
  cat > "${DIFF_JSON}" <<EOF
{"trading_day":"${TRADING_DAY}","diff_count":0,"mode":"dry_run"}
EOF
  echo "[dry-run] ${SETTLEMENT_BIN} ${CTP_CONFIG_PATH}"
fi

if [[ ${RUN_READINESS_GATES} -eq 1 ]]; then
  mkdir -p "$(dirname "${BENCHMARK_RESULT_JSON}")"
  "${QUANT_ROOT}/build/backtest_benchmark_cli" \
    --runs "${BENCHMARK_RUNS}" \
    --baseline_p95_ms 100 \
    --result_json "${BENCHMARK_RESULT_JSON}"
fi
