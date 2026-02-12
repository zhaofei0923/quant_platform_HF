#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="http://127.0.0.1:8083"
CONNECTOR_FILE="infra/debezium/connectors/trading_core.json"
CONNECTOR_NAME=""
OUTPUT_FILE="docs/results/debezium_connector_register_result.env"
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --connect-url <url>       Kafka Connect REST base URL (default: http://127.0.0.1:8083)
  --connector-file <path>   Connector JSON file path
  --connector-name <name>   Connector name override
  --output-file <path>      Evidence env output path
  --dry-run                 Print/record commands without executing (default)
  --execute                 Execute commands
  -h, --help                Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --connect-url)
      CONNECT_URL="${2:-}"
      shift 2
      ;;
    --connector-file)
      CONNECTOR_FILE="${2:-}"
      shift 2
      ;;
    --connector-name)
      CONNECTOR_NAME="${2:-}"
      shift 2
      ;;
    --output-file)
      OUTPUT_FILE="${2:-}"
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

if [[ ! -f "$CONNECTOR_FILE" ]]; then
  echo "error: connector file not found: $CONNECTOR_FILE" >&2
  exit 2
fi

parsed_payload=$(python3 - "$CONNECTOR_FILE" "$CONNECTOR_NAME" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
name_override = sys.argv[2]
raw = json.loads(path.read_text(encoding="utf-8"))
if isinstance(raw, dict) and "config" in raw:
    config = raw["config"]
    name = name_override or raw.get("name", "")
else:
    config = raw
    name = name_override
if not isinstance(config, dict):
    raise SystemExit("connector config must be a JSON object")
if not name:
    raise SystemExit("connector name is required (set --connector-name or provide JSON.name)")
print(name)
print(json.dumps(config, ensure_ascii=True, separators=(",", ":")))
PY
)

connector_name=$(echo "$parsed_payload" | head -n 1)
config_json=$(echo "$parsed_payload" | tail -n +2)
register_command="curl -fsS -X PUT ${CONNECT_URL}/connectors/${connector_name}/config -H 'Content-Type: application/json' --data '<config-json>'"

status="simulated_ok"
failed_step=""
started_ns=$(date +%s%N)
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] $register_command"
else
  set +e
  curl -fsS -X PUT "${CONNECT_URL}/connectors/${connector_name}/config" \
    -H "Content-Type: application/json" \
    --data "$config_json" >/dev/null
  rc=$?
  set -e
  if [[ $rc -eq 0 ]]; then
    status="ok"
  else
    status="failed"
    failed_step="register_connector"
  fi
fi
finished_ns=$(date +%s%N)
elapsed_ms=$(( (finished_ns - started_ns) / 1000000 ))
if [[ $elapsed_ms -lt 0 ]]; then
  elapsed_ms=0
fi

success="true"
if [[ "$status" == "failed" ]]; then
  success="false"
fi

mkdir -p "$(dirname "$OUTPUT_FILE")"
{
  echo "DEBEZIUM_REGISTER_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "DEBEZIUM_REGISTER_SUCCESS=$success"
  echo "DEBEZIUM_REGISTER_FAILED_STEP=$failed_step"
  echo "DEBEZIUM_REGISTER_CONNECT_URL=$CONNECT_URL"
  echo "DEBEZIUM_REGISTER_CONNECTOR_FILE=$CONNECTOR_FILE"
  echo "DEBEZIUM_REGISTER_CONNECTOR_NAME=$connector_name"
  echo "STEP_1_NAME=register_connector"
  echo "STEP_1_STATUS=$status"
  echo "STEP_1_DURATION_MS=$elapsed_ms"
  echo "STEP_1_COMMAND=$register_command"
} > "$OUTPUT_FILE"

echo "$OUTPUT_FILE"
if [[ "$success" == "true" ]]; then
  exit 0
fi
exit 2
