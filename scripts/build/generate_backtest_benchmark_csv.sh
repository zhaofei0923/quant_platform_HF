#!/usr/bin/env bash
set -euo pipefail

output="runtime/benchmarks/backtest/rb_perf_large.csv"
ticks="120000"
symbol="rb2405"
exchange="SHFE"
start_ts_ns="1704067200000000000"
step_ns="1000000"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --output PATH      Output CSV path (default: runtime/benchmarks/backtest/rb_perf_large.csv)
  --ticks N          Number of ticks to generate (default: 120000)
  --symbol SYMBOL    Instrument symbol (default: rb2405)
  --exchange EX      Exchange code (default: SHFE)
  --start-ts-ns N    Start timestamp in ns (default: 1704067200000000000)
  --step-ns N        Tick interval in ns (default: 1000000)
  -h, --help         Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      output="$2"
      shift 2
      ;;
    --ticks)
      ticks="$2"
      shift 2
      ;;
    --symbol)
      symbol="$2"
      shift 2
      ;;
    --exchange)
      exchange="$2"
      shift 2
      ;;
    --start-ts-ns)
      start_ts_ns="$2"
      shift 2
      ;;
    --step-ns)
      step_ns="$2"
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

if [[ "${ticks}" -le 0 ]]; then
  echo "error: ticks must be > 0" >&2
  exit 2
fi

format_price() {
  local centi="$1"
  local sign=""
  if [[ "${centi}" -lt 0 ]]; then
    sign="-"
    centi=$(( -centi ))
  fi
  printf '%s%d.%02d' "${sign}" $((centi / 100)) $((centi % 100))
}

mkdir -p "$(dirname "${output}")"

{
  echo "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,volume,turnover,open_interest"
  for ((i = 0; i < ticks; ++i)); do
    ts_ns=$((start_ts_ns + i * step_ns))

    cycle_component=$(( (i % 400) - 200 ))
    trend_component=$(( (i / 2000) % 60 - 30 ))
    micro_component=$(( (i % 13) - 6 ))

    last_price_centi=$((350000 + cycle_component + trend_component * 2 + micro_component))
    bid_price_centi=$((last_price_centi - 12))
    ask_price_centi=$((last_price_centi + 12))

    last_volume=$((1 + (i % 5)))
    bid_volume1=$((20 + (i % 11)))
    ask_volume1=$((22 + (i % 13)))
    volume=$((1000 + i))
    turnover_centi=$((volume * last_price_centi))
    open_interest=$((50000 + (i % 2000)))

    printf '%s,%s,%d,%s,%d,%s,%d,%s,%d,%d,%d,%d\n' \
      "${symbol}" \
      "${exchange}" \
      "${ts_ns}" \
      "$(format_price "${last_price_centi}")" \
      "${last_volume}" \
      "$(format_price "${bid_price_centi}")" \
      "${bid_volume1}" \
      "$(format_price "${ask_price_centi}")" \
      "${ask_volume1}" \
      "${volume}" \
      "${turnover_centi}" \
      "${open_interest}"
  done
} >"${output}"

echo "generated deterministic benchmark csv: ${output} (ticks=${ticks})"
