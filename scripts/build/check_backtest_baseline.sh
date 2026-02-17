#!/usr/bin/env bash
set -euo pipefail

baseline_json="tests/regression/backtest_consistency/baseline/legacy_python/backtest_baseline.json"
provenance_json="tests/regression/backtest_consistency/baseline/legacy_python/provenance.json"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --baseline-json PATH     Baseline JSON file path
  --provenance-json PATH   Provenance JSON file path
  -h, --help               Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline-json)
      baseline_json="$2"
      shift 2
      ;;
    --provenance-json)
      provenance_json="$2"
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

require_file() {
  local path="$1"
  local label="$2"
  if [[ ! -f "${path}" ]]; then
    echo "error: missing ${label}: ${path}" >&2
    exit 1
  fi
}

require_json_field() {
  local path="$1"
  local field="$2"
  if ! grep -Eq "\"${field}\"[[:space:]]*:[[:space:]]*\"[^\"]+\"" "${path}"; then
    echo "error: missing or empty field '${field}' in ${path}" >&2
    exit 1
  fi
}

require_file "${baseline_json}" "baseline json"
require_file "${provenance_json}" "baseline provenance"

for key in run_id mode spec replay deterministic summary; do
  if ! grep -Eq "\"${key}\"[[:space:]]*:" "${baseline_json}"; then
    echo "error: baseline json missing key '${key}': ${baseline_json}" >&2
    exit 1
  fi
done

for key in source legacy_commit generated_at owner dataset_signature; do
  require_json_field "${provenance_json}" "${key}"
done

if ! grep -Eq '"source"[[:space:]]*:[[:space:]]*"legacy_python"' "${provenance_json}"; then
  echo "error: provenance source must be 'legacy_python': ${provenance_json}" >&2
  exit 1
fi

echo "baseline availability check passed"
