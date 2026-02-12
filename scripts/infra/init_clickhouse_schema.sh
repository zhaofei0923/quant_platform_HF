#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="infra/docker-compose.single-host.m2.yaml"
PROJECT_NAME="quant-hft-single-host-m2"
ENV_FILE="infra/env/prodlike.env"
DOCKER_BIN="docker"
CLICKHOUSE_SERVICE="clickhouse"
CLICKHOUSE_DB="quant_hft"
CLICKHOUSE_USER="quant_hft"
CLICKHOUSE_PASSWORD="quant_hft"
SCHEMA_DIR="infra/clickhouse/init"
OUTPUT_FILE="docs/results/clickhouse_schema_init_result.env"
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --compose-file <path>       Docker compose file path
  --project-name <name>       Compose project name
  --env-file <path>           Env file passed to compose
  --docker-bin <path>         Docker binary (default: docker)
  --clickhouse-service <name> ClickHouse service name (default: clickhouse)
  --clickhouse-db <name>      ClickHouse database name (default: quant_hft)
  --clickhouse-user <name>    ClickHouse user name (default: quant_hft)
  --clickhouse-password <pwd> ClickHouse password (default: quant_hft)
  --schema-dir <path>         SQL schema directory (default: infra/clickhouse/init)
  --output-file <path>        Evidence env output path
  --dry-run                   Print/record commands without executing (default)
  --execute                   Execute commands
  -h, --help                  Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compose-file)
      COMPOSE_FILE="${2:-}"
      shift 2
      ;;
    --project-name)
      PROJECT_NAME="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --docker-bin)
      DOCKER_BIN="${2:-}"
      shift 2
      ;;
    --clickhouse-service)
      CLICKHOUSE_SERVICE="${2:-}"
      shift 2
      ;;
    --clickhouse-db)
      CLICKHOUSE_DB="${2:-}"
      shift 2
      ;;
    --clickhouse-user)
      CLICKHOUSE_USER="${2:-}"
      shift 2
      ;;
    --clickhouse-password)
      CLICKHOUSE_PASSWORD="${2:-}"
      shift 2
      ;;
    --schema-dir)
      SCHEMA_DIR="${2:-}"
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

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "error: compose file not found: $COMPOSE_FILE" >&2
  exit 2
fi
if [[ ! -d "$SCHEMA_DIR" ]]; then
  echo "error: schema directory not found: $SCHEMA_DIR" >&2
  exit 2
fi

mapfile -t schema_files < <(find "$SCHEMA_DIR" -maxdepth 1 -type f -name '*.sql' | sort)
if [[ "${#schema_files[@]}" -eq 0 ]]; then
  echo "error: no SQL files found under: $SCHEMA_DIR" >&2
  exit 2
fi

compose_base=("$DOCKER_BIN" "compose" "-f" "$COMPOSE_FILE" "--project-name" "$PROJECT_NAME")
if [[ -f "$ENV_FILE" ]]; then
  compose_base+=("--env-file" "$ENV_FILE")
fi

steps_name=()
steps_cmd=()
steps_status=()
steps_duration_ms=()
failed_step=""

run_apply_schema() {
  local schema_file="$1"
  "${compose_base[@]}" exec -T "$CLICKHOUSE_SERVICE" clickhouse-client \
    --multiquery \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" < "$schema_file"
}

for schema_file in "${schema_files[@]}"; do
  schema_name="$(basename "$schema_file")"
  steps_name+=("apply_${schema_name}")
  steps_cmd+=("${compose_base[*]} exec -T $CLICKHOUSE_SERVICE clickhouse-client --multiquery --user $CLICKHOUSE_USER --password **** --database $CLICKHOUSE_DB < $schema_file")
done

for idx in "${!steps_name[@]}"; do
  name="${steps_name[$idx]}"
  command_text="${steps_cmd[$idx]}"
  started_ns=$(date +%s%N)

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $command_text"
    status="simulated_ok"
  else
    set +e
    run_apply_schema "${schema_files[$idx]}"
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
  echo "CLICKHOUSE_SCHEMA_INIT_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "CLICKHOUSE_SCHEMA_INIT_SUCCESS=$success"
  echo "CLICKHOUSE_SCHEMA_INIT_FAILED_STEP=$failed_step"
  echo "CLICKHOUSE_SCHEMA_INIT_COMPOSE_FILE=$COMPOSE_FILE"
  echo "CLICKHOUSE_SCHEMA_INIT_PROJECT_NAME=$PROJECT_NAME"
  echo "CLICKHOUSE_SCHEMA_INIT_ENV_FILE=$ENV_FILE"
  echo "CLICKHOUSE_SCHEMA_INIT_SCHEMA_DIR=$SCHEMA_DIR"
  echo "CLICKHOUSE_SCHEMA_INIT_SERVICE=$CLICKHOUSE_SERVICE"
  echo "CLICKHOUSE_SCHEMA_INIT_DB=$CLICKHOUSE_DB"
  echo "CLICKHOUSE_SCHEMA_INIT_TOTAL_STEPS=${#steps_status[@]}"
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
