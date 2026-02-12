#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="http://localhost:8083"
TRADING_CORE_CONNECTOR_FILE="infra/debezium/connector-trading-core.json"
OPS_AUDIT_CONNECTOR_FILE="infra/debezium/connector-ops-audit.json"
OUTPUT_FILE="docs/results/debezium_connector_init_result.env"
CURL_BIN="curl"
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --connect-url <url>                  Kafka Connect URL (default: http://localhost:8083)
  --trading-core-connector-file <path> Trading core connector json file
  --ops-audit-connector-file <path>    Ops audit connector json file
  --output-file <path>                 Evidence env output path
  --curl-bin <path>                    Curl binary (default: curl)
  --dry-run                            Print/record commands without executing (default)
  --execute                            Execute commands
  -h, --help                           Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --connect-url)
      CONNECT_URL="${2:-}"
      shift 2
      ;;
    --trading-core-connector-file)
      TRADING_CORE_CONNECTOR_FILE="${2:-}"
      shift 2
      ;;
    --ops-audit-connector-file)
      OPS_AUDIT_CONNECTOR_FILE="${2:-}"
      shift 2
      ;;
    --output-file)
      OUTPUT_FILE="${2:-}"
      shift 2
      ;;
    --curl-bin)
      CURL_BIN="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --execute)
      DRY_RUN=0
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

for file in "$TRADING_CORE_CONNECTOR_FILE" "$OPS_AUDIT_CONNECTOR_FILE"; do
  if [[ ! -f "$file" ]]; then
    echo "error: connector file not found: $file" >&2
    exit 2
  fi
done

extract_connector_name() {
  local file="$1"
  python3 - "$file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    payload = json.load(f)
name = payload.get("name", "").strip()
if not name:
    raise SystemExit("connector name is required")
print(name)
PY
}

extract_connector_config() {
  local file="$1"
  python3 - "$file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    payload = json.load(f)
config = payload.get("config")
if not isinstance(config, dict):
    raise SystemExit("connector config object is required")
print(json.dumps(config, separators=(",", ":")))
PY
}

apply_connector() {
  local file="$1"
  local name
  local config_json
  name="$(extract_connector_name "$file")"
  config_json="$(extract_connector_config "$file")"
  "$CURL_BIN" -sS --fail \
    -X PUT \
    -H "Content-Type: application/json" \
    --data "$config_json" \
    "${CONNECT_URL}/connectors/${name}/config" >/dev/null
}

verify_connector() {
  local file="$1"
  local name
  name="$(extract_connector_name "$file")"
  "$CURL_BIN" -sS --fail \
    "${CONNECT_URL}/connectors/${name}/status" >/dev/null
}

trading_core_name="$(extract_connector_name "$TRADING_CORE_CONNECTOR_FILE")"
ops_audit_name="$(extract_connector_name "$OPS_AUDIT_CONNECTOR_FILE")"

steps_name=("apply_trading_core" "verify_trading_core" "apply_ops_audit" "verify_ops_audit")
steps_cmd=(
  "$CURL_BIN -sS --fail -X PUT -H 'Content-Type: application/json' --data @${TRADING_CORE_CONNECTOR_FILE}:config ${CONNECT_URL}/connectors/${trading_core_name}/config"
  "$CURL_BIN -sS --fail ${CONNECT_URL}/connectors/${trading_core_name}/status"
  "$CURL_BIN -sS --fail -X PUT -H 'Content-Type: application/json' --data @${OPS_AUDIT_CONNECTOR_FILE}:config ${CONNECT_URL}/connectors/${ops_audit_name}/config"
  "$CURL_BIN -sS --fail ${CONNECT_URL}/connectors/${ops_audit_name}/status"
)
steps_status=()
steps_duration_ms=()
failed_step=""

for idx in "${!steps_name[@]}"; do
  name="${steps_name[$idx]}"
  command_text="${steps_cmd[$idx]}"
  started_ns=$(date +%s%N)

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $command_text"
    status="simulated_ok"
  else
    set +e
    if [[ "$name" == "apply_trading_core" ]]; then
      apply_connector "$TRADING_CORE_CONNECTOR_FILE"
    elif [[ "$name" == "verify_trading_core" ]]; then
      verify_connector "$TRADING_CORE_CONNECTOR_FILE"
    elif [[ "$name" == "apply_ops_audit" ]]; then
      apply_connector "$OPS_AUDIT_CONNECTOR_FILE"
    else
      verify_connector "$OPS_AUDIT_CONNECTOR_FILE"
    fi
    rc=$?
    set -e
    if [[ $rc -eq 0 ]]; then
      status="ok"
    else
      status="failed"
      if [[ -z "$failed_step" ]]; then
        failed_step="$name"
      fi
    fi
  fi

  finished_ns=$(date +%s%N)
  elapsed_ms=$(( (finished_ns - started_ns) / 1000000 ))
  if [[ $elapsed_ms -lt 0 ]]; then
    elapsed_ms=0
  fi
  steps_status+=("$status")
  steps_duration_ms+=("$elapsed_ms")

  if [[ "$status" == "failed" ]]; then
    break
  fi
done

success="true"
if [[ -n "$failed_step" ]]; then
  success="false"
fi

mkdir -p "$(dirname "$OUTPUT_FILE")"
{
  echo "DEBEZIUM_CONNECTOR_INIT_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "DEBEZIUM_CONNECTOR_INIT_SUCCESS=$success"
  echo "DEBEZIUM_CONNECTOR_INIT_FAILED_STEP=$failed_step"
  echo "DEBEZIUM_CONNECTOR_INIT_CONNECT_URL=$CONNECT_URL"
  echo "DEBEZIUM_CONNECTOR_INIT_TRADING_CORE_FILE=$TRADING_CORE_CONNECTOR_FILE"
  echo "DEBEZIUM_CONNECTOR_INIT_OPS_AUDIT_FILE=$OPS_AUDIT_CONNECTOR_FILE"
  echo "DEBEZIUM_CONNECTOR_INIT_TRADING_CORE_NAME=$trading_core_name"
  echo "DEBEZIUM_CONNECTOR_INIT_OPS_AUDIT_NAME=$ops_audit_name"
  echo "DEBEZIUM_CONNECTOR_INIT_TOTAL_STEPS=${#steps_status[@]}"
  for idx in "${!steps_status[@]}"; do
    n=$((idx + 1))
    echo "STEP_${n}_NAME=${steps_name[$idx]}"
    echo "STEP_${n}_STATUS=${steps_status[$idx]}"
    echo "STEP_${n}_DURATION_MS=${steps_duration_ms[$idx]}"
    echo "STEP_${n}_COMMAND=${steps_cmd[$idx]}"
  done
} > "$OUTPUT_FILE"

echo "$OUTPUT_FILE"
if [[ "$success" == "true" ]]; then
  exit 0
fi
exit 2

