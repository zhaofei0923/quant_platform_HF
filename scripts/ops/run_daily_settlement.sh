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
SETTLEMENT_PRICE_JSON="${SETTLEMENT_PRICE_JSON:-${QUANT_ROOT}/docs/results/settlement_price_input.json}"
PRICE_CACHE_DB="${PRICE_CACHE_DB:-${QUANT_ROOT}/runtime/settlement_price_cache.sqlite}"
PREFETCH_SETTLEMENT_PRICES=0
SETTLEMENT_PRICE_API_URL="${SETTLEMENT_PRICE_API_URL:-}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DAILY_SETTLEMENT_ORCHESTRATOR="${DAILY_SETTLEMENT_ORCHESTRATOR:-${QUANT_ROOT}/scripts/ops/daily_settlement_orchestrator.py}"
PRECISION_VERIFY_SCRIPT="${PRECISION_VERIFY_SCRIPT:-${QUANT_ROOT}/scripts/ops/verify_daily_settlement_precision.py}"
SETTLEMENT_BENCHMARK_SCRIPT="${SETTLEMENT_BENCHMARK_SCRIPT:-${QUANT_ROOT}/scripts/perf/run_daily_settlement_benchmark.py}"
RUN_READINESS_GATES=0
PRECISION_DATASET_JSON="${PRECISION_DATASET_JSON:-}"
PRECISION_RESULT_JSON="${PRECISION_RESULT_JSON:-${QUANT_ROOT}/docs/results/daily_settlement_precision_report.json}"
PRECISION_MIN_DAYS="${PRECISION_MIN_DAYS:-10}"
PRECISION_TOLERANCE="${PRECISION_TOLERANCE:-0.01}"
BENCHMARK_RESULT_JSON="${BENCHMARK_RESULT_JSON:-${QUANT_ROOT}/docs/results/daily_settlement_benchmark.json}"
BENCHMARK_POSITIONS="${BENCHMARK_POSITIONS:-10000}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-20}"
BENCHMARK_TARGET_P99_MS="${BENCHMARK_TARGET_P99_MS:-200}"
EXECUTE=0
FORCE=0
SHADOW=0
STRICT_BACKFILL=0

usage() {
  cat <<USAGE
Usage: $0 --trading-day <YYYYMMDD|YYYY-MM-DD> [options]

Options:
  --trading-day <value>              Trading day to settle
  --ctp-config-path <path>           CTP yaml path (default: ${CTP_CONFIG_PATH})
  --settlement-bin <path>            daily_settlement binary path (default: ${SETTLEMENT_BIN})
  --evidence-json <path>             Evidence json output path (default: ${EVIDENCE_JSON})
  --diff-json <path>                 Reconcile diff json output path (default: ${DIFF_JSON})
  --settlement-price-json <path>     Settlement price json path (default: ${SETTLEMENT_PRICE_JSON})
  --price-cache-db <path>            Settlement price cache db path (default: ${PRICE_CACHE_DB})
  --prefetch-settlement-prices       Prefetch settlement prices from API URL before run
  --settlement-price-api-url <url>   Settlement price API URL used by prefetch step
  --run-readiness-gates              Run precision+benchmark gates after settlement success
  --precision-dataset-json <path>    Precision dataset json for 10-day reconciliation gate
  --precision-result-json <path>     Precision report output path (default: ${PRECISION_RESULT_JSON})
  --precision-min-days <int>         Minimum required trading days (default: ${PRECISION_MIN_DAYS})
  --precision-tolerance <float>      Allowed absolute diff tolerance (default: ${PRECISION_TOLERANCE})
  --benchmark-result-json <path>     Benchmark report output path (default: ${BENCHMARK_RESULT_JSON})
  --benchmark-positions <int>        Position count for benchmark (default: ${BENCHMARK_POSITIONS})
  --benchmark-runs <int>             Benchmark runs (default: ${BENCHMARK_RUNS})
  --benchmark-target-p99-ms <float>  Benchmark P99 threshold in ms (default: ${BENCHMARK_TARGET_P99_MS})
  --execute                          Execute for real (default: dry-run)
  --force                            Force rerun even if completed
  --shadow                           Enable shadow settlement mode
  --strict-order-trade-backfill      Block if order/trade backfill query is unsupported
  -h, --help                         Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --trading-day)
      TRADING_DAY="${2:-}"
      shift 2
      ;;
    --ctp-config-path)
      CTP_CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --settlement-bin)
      SETTLEMENT_BIN="${2:-}"
      shift 2
      ;;
    --evidence-json)
      EVIDENCE_JSON="${2:-}"
      shift 2
      ;;
    --diff-json)
      DIFF_JSON="${2:-}"
      shift 2
      ;;
    --settlement-price-json)
      SETTLEMENT_PRICE_JSON="${2:-}"
      shift 2
      ;;
    --price-cache-db)
      PRICE_CACHE_DB="${2:-}"
      shift 2
      ;;
    --prefetch-settlement-prices)
      PREFETCH_SETTLEMENT_PRICES=1
      shift
      ;;
    --settlement-price-api-url)
      SETTLEMENT_PRICE_API_URL="${2:-}"
      shift 2
      ;;
    --run-readiness-gates)
      RUN_READINESS_GATES=1
      shift
      ;;
    --precision-dataset-json)
      PRECISION_DATASET_JSON="${2:-}"
      shift 2
      ;;
    --precision-result-json)
      PRECISION_RESULT_JSON="${2:-}"
      shift 2
      ;;
    --precision-min-days)
      PRECISION_MIN_DAYS="${2:-}"
      shift 2
      ;;
    --precision-tolerance)
      PRECISION_TOLERANCE="${2:-}"
      shift 2
      ;;
    --benchmark-result-json)
      BENCHMARK_RESULT_JSON="${2:-}"
      shift 2
      ;;
    --benchmark-positions)
      BENCHMARK_POSITIONS="${2:-}"
      shift 2
      ;;
    --benchmark-runs)
      BENCHMARK_RUNS="${2:-}"
      shift 2
      ;;
    --benchmark-target-p99-ms)
      BENCHMARK_TARGET_P99_MS="${2:-}"
      shift 2
      ;;
    --execute)
      EXECUTE=1
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --shadow)
      SHADOW=1
      shift
      ;;
    --strict-order-trade-backfill)
      STRICT_BACKFILL=1
      shift
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

if [[ -z "${TRADING_DAY}" ]]; then
  echo "error: --trading-day is required" >&2
  exit 2
fi

if [[ ${RUN_READINESS_GATES} -eq 1 && ${EXECUTE} -ne 1 ]]; then
  echo "error: --run-readiness-gates requires --execute" >&2
  exit 2
fi

if [[ ${RUN_READINESS_GATES} -eq 1 && -z "${PRECISION_DATASET_JSON}" ]]; then
  echo "error: --precision-dataset-json is required when --run-readiness-gates is enabled" >&2
  exit 2
fi

cmd=(
  "${PYTHON_BIN}"
  "${DAILY_SETTLEMENT_ORCHESTRATOR}"
  --settlement-bin "${SETTLEMENT_BIN}"
  --ctp-config-path "${CTP_CONFIG_PATH}"
  --trading-day "${TRADING_DAY}"
  --evidence-json "${EVIDENCE_JSON}"
  --diff-json "${DIFF_JSON}"
  --settlement-price-json "${SETTLEMENT_PRICE_JSON}"
  --price-cache-db "${PRICE_CACHE_DB}"
)

if [[ ${EXECUTE} -eq 1 ]]; then
  cmd+=(--execute)
fi
if [[ ${FORCE} -eq 1 ]]; then
  cmd+=(--force)
fi
if [[ ${SHADOW} -eq 1 ]]; then
  cmd+=(--shadow)
fi
if [[ ${STRICT_BACKFILL} -eq 1 ]]; then
  cmd+=(--strict-order-trade-backfill)
fi
if [[ ${PREFETCH_SETTLEMENT_PRICES} -eq 1 ]]; then
  cmd+=(--prefetch-settlement-prices)
fi
if [[ -n "${SETTLEMENT_PRICE_API_URL}" ]]; then
  cmd+=(--settlement-price-api-url "${SETTLEMENT_PRICE_API_URL}")
fi

"${cmd[@]}"

if [[ ${RUN_READINESS_GATES} -eq 1 ]]; then
  precision_cmd=(
    "${PYTHON_BIN}"
    "${PRECISION_VERIFY_SCRIPT}"
    --dataset-json "${PRECISION_DATASET_JSON}"
    --result-json "${PRECISION_RESULT_JSON}"
    --min-days "${PRECISION_MIN_DAYS}"
    --tolerance "${PRECISION_TOLERANCE}"
  )
  "${precision_cmd[@]}"

  benchmark_cmd=(
    "${PYTHON_BIN}"
    "${SETTLEMENT_BENCHMARK_SCRIPT}"
    --positions "${BENCHMARK_POSITIONS}"
    --runs "${BENCHMARK_RUNS}"
    --target-p99-ms "${BENCHMARK_TARGET_P99_MS}"
    --result-json "${BENCHMARK_RESULT_JSON}"
  )
  "${benchmark_cmd[@]}"
fi
