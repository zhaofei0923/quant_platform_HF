#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="infra/docker-compose.single-host.yaml"
PROJECT_NAME="quant-hft-single-host"
ENV_FILE="infra/env/prodlike.env"
SCHEMA_DIR="infra/timescale/init"
SCHEMA_FILE=""
TIMESCALE_SERVICE="timescale-primary"
TIMESCALE_DB="quant_hft"
TIMESCALE_USER="quant_hft"
OUTPUT_FILE="docs/results/timescale_schema_init_result.env"
DOCKER_BIN="docker"
READY_TIMEOUT_SEC=120
READY_POLL_INTERVAL_SEC=2
APPLY_MAX_ATTEMPTS=20
APPLY_RETRY_INTERVAL_SEC=2
EXPECTED_TABLE_COUNT=25
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --compose-file <path>        Docker compose file path
  --project-name <name>        Compose project name
  --env-file <path>            Env file passed to compose
  --schema-dir <path>          SQL schema directory path (all *.sql sorted)
  --schema-file <path>         Single SQL schema file path (overrides --schema-dir)
  --timescale-service <name>   Timescale service name (default: timescale-primary)
  --timescale-db <name>        Database name (default: quant_hft)
  --timescale-user <name>      Database user (default: quant_hft)
  --ready-timeout-sec <sec>    Wait timeout for pg_isready (default: 120)
  --ready-poll-interval-sec <sec> Poll interval for pg_isready (default: 2)
  --apply-max-attempts <n>     Retry attempts for schema apply/verify (default: 20)
  --apply-retry-interval-sec <sec> Sleep between schema retries (default: 2)
  --expected-table-count <n>   Verify target table count (default: 20)
  --output-file <path>         Evidence env output path
  --docker-bin <path>          Docker binary (default: docker)
  --dry-run                    Print/record commands without executing (default)
  --execute                    Execute commands
  -h, --help                   Show this help
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
    --schema-file)
      SCHEMA_FILE="${2:-}"
      shift 2
      ;;
    --schema-dir)
      SCHEMA_DIR="${2:-}"
      shift 2
      ;;
    --timescale-service)
      TIMESCALE_SERVICE="${2:-}"
      shift 2
      ;;
    --timescale-db)
      TIMESCALE_DB="${2:-}"
      shift 2
      ;;
    --timescale-user)
      TIMESCALE_USER="${2:-}"
      shift 2
      ;;
    --ready-timeout-sec)
      READY_TIMEOUT_SEC="${2:-}"
      shift 2
      ;;
    --ready-poll-interval-sec)
      READY_POLL_INTERVAL_SEC="${2:-}"
      shift 2
      ;;
    --apply-max-attempts)
      APPLY_MAX_ATTEMPTS="${2:-}"
      shift 2
      ;;
    --apply-retry-interval-sec)
      APPLY_RETRY_INTERVAL_SEC="${2:-}"
      shift 2
      ;;
    --expected-table-count)
      EXPECTED_TABLE_COUNT="${2:-}"
      shift 2
      ;;
    --output-file)
      OUTPUT_FILE="${2:-}"
      shift 2
      ;;
    --docker-bin)
      DOCKER_BIN="${2:-}"
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
declare -a schema_files=()
if [[ -n "$SCHEMA_FILE" ]]; then
  if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "error: schema file not found: $SCHEMA_FILE" >&2
    exit 2
  fi
  schema_files=("$SCHEMA_FILE")
else
  if [[ ! -d "$SCHEMA_DIR" ]]; then
    echo "error: schema dir not found: $SCHEMA_DIR" >&2
    exit 2
  fi
  mapfile -t schema_files < <(find "$SCHEMA_DIR" -maxdepth 1 -type f -name '*.sql' | sort)
  if [[ ${#schema_files[@]} -eq 0 ]]; then
    echo "error: no schema files found under: $SCHEMA_DIR" >&2
    exit 2
  fi
fi

compose_base=("$DOCKER_BIN" "compose" "-f" "$COMPOSE_FILE" "--project-name" "$PROJECT_NAME")
if [[ -f "$ENV_FILE" ]]; then
  compose_base+=("--env-file" "$ENV_FILE")
fi

apply_command="${compose_base[*]} exec -T $TIMESCALE_SERVICE psql -v ON_ERROR_STOP=1 -U $TIMESCALE_USER -d $TIMESCALE_DB < <schema-files> (files=${#schema_files[@]} retry=${APPLY_MAX_ATTEMPTS}x interval=${APPLY_RETRY_INTERVAL_SEC}s)"
verify_sql="$(cat <<'SQL'
WITH required(schema_name, table_name) AS (
    VALUES
        ('analytics_ts', 'market_snapshots'),
        ('analytics_ts', 'order_events'),
        ('analytics_ts', 'risk_decisions'),
        ('trading_core', 'account_funds'),
        ('trading_core', 'position_detail'),
        ('trading_core', 'position_summary'),
        ('trading_core', 'orders'),
        ('trading_core', 'trades'),
        ('trading_core', 'instruments'),
        ('trading_core', 'trading_calendar'),
        ('trading_core', 'strategies'),
        ('trading_core', 'strategy_params'),
        ('trading_core', 'risk_events'),
        ('trading_core', 'fund_transfer'),
        ('trading_core', 'fee_margin_template'),
        ('trading_core', 'settlement_summary'),
        ('trading_core', 'settlement_detail'),
        ('trading_core', 'settlement_prices'),
        ('ops', 'system_config'),
        ('ops', 'system_logs'),
        ('ops', 'archive_manifest'),
        ('ops', 'sim_account_mapping'),
        ('ops', 'ctp_connection'),
        ('ops', 'settlement_runs'),
        ('ops', 'settlement_reconcile_diff')
)
SELECT COUNT(*)
FROM required r
JOIN information_schema.tables t
  ON t.table_schema = r.schema_name
 AND t.table_name = r.table_name;
SQL
)"
wait_ready_command="${compose_base[*]} exec -T $TIMESCALE_SERVICE pg_isready -U $TIMESCALE_USER -d $TIMESCALE_DB (timeout=${READY_TIMEOUT_SEC}s interval=${READY_POLL_INTERVAL_SEC}s)"
verify_command="${compose_base[*]} exec -T $TIMESCALE_SERVICE psql -t -A -U $TIMESCALE_USER -d $TIMESCALE_DB -c \"$verify_sql\" (retry=${APPLY_MAX_ATTEMPTS}x interval=${APPLY_RETRY_INTERVAL_SEC}s)"

steps_name=("wait_ready" "apply_schema" "verify_schema")
steps_cmd=("$wait_ready_command" "$apply_command" "$verify_command")
steps_status=()
steps_duration_ms=()
failed_step=""

run_step_wait_ready() {
  local elapsed=0
  while (( elapsed <= READY_TIMEOUT_SEC )); do
    if "${compose_base[@]}" exec -T "$TIMESCALE_SERVICE" \
      pg_isready -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$READY_POLL_INTERVAL_SEC"
    elapsed=$((elapsed + READY_POLL_INTERVAL_SEC))
  done
  return 1
}

run_step_apply() {
  local attempt
  for ((attempt = 1; attempt <= APPLY_MAX_ATTEMPTS; ++attempt)); do
    local applied=1
    local schema
    for schema in "${schema_files[@]}"; do
      if ! "${compose_base[@]}" exec -T "$TIMESCALE_SERVICE" \
        psql -v ON_ERROR_STOP=1 -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" < "$schema"; then
        applied=0
        break
      fi
    done
    if (( applied == 1 )); then
      return 0
    fi
    if (( attempt < APPLY_MAX_ATTEMPTS )); then
      sleep "$APPLY_RETRY_INTERVAL_SEC"
    fi
  done
  return 1
}

run_step_verify() {
  local count
  local attempt
  for ((attempt = 1; attempt <= APPLY_MAX_ATTEMPTS; ++attempt)); do
    if count=$("${compose_base[@]}" exec -T "$TIMESCALE_SERVICE" \
      psql -t -A -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" -c "$verify_sql" 2>/dev/null); then
      count="$(echo "$count" | tr -d '[:space:]')"
      if [[ "$count" == "$EXPECTED_TABLE_COUNT" ]]; then
        return 0
      fi
    fi
    if (( attempt < APPLY_MAX_ATTEMPTS )); then
      sleep "$APPLY_RETRY_INTERVAL_SEC"
    fi
  done
  return 1
}

for idx in "${!steps_name[@]}"; do
  name="${steps_name[$idx]}"
  command_text="${steps_cmd[$idx]}"
  started_ns=$(date +%s%N)

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $command_text"
    status="simulated_ok"
  else
    set +e
    if [[ "$name" == "wait_ready" ]]; then
      run_step_wait_ready
    elif [[ "$name" == "apply_schema" ]]; then
      run_step_apply
    else
      run_step_verify
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
  echo "TIMESCALE_SCHEMA_INIT_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "TIMESCALE_SCHEMA_INIT_SUCCESS=$success"
  echo "TIMESCALE_SCHEMA_INIT_FAILED_STEP=$failed_step"
  echo "TIMESCALE_SCHEMA_INIT_COMPOSE_FILE=$COMPOSE_FILE"
  echo "TIMESCALE_SCHEMA_INIT_PROJECT_NAME=$PROJECT_NAME"
  echo "TIMESCALE_SCHEMA_INIT_ENV_FILE=$ENV_FILE"
  echo "TIMESCALE_SCHEMA_INIT_SCHEMA_FILE=$SCHEMA_FILE"
  echo "TIMESCALE_SCHEMA_INIT_SCHEMA_DIR=$SCHEMA_DIR"
  echo "TIMESCALE_SCHEMA_INIT_SCHEMA_FILE_COUNT=${#schema_files[@]}"
  echo "TIMESCALE_SCHEMA_INIT_EXPECTED_TABLE_COUNT=$EXPECTED_TABLE_COUNT"
  echo "TIMESCALE_SCHEMA_INIT_SERVICE=$TIMESCALE_SERVICE"
  echo "TIMESCALE_SCHEMA_INIT_DB=$TIMESCALE_DB"
  echo "TIMESCALE_SCHEMA_INIT_USER=$TIMESCALE_USER"
  echo "TIMESCALE_SCHEMA_INIT_READY_TIMEOUT_SEC=$READY_TIMEOUT_SEC"
  echo "TIMESCALE_SCHEMA_INIT_READY_POLL_INTERVAL_SEC=$READY_POLL_INTERVAL_SEC"
  echo "TIMESCALE_SCHEMA_INIT_APPLY_MAX_ATTEMPTS=$APPLY_MAX_ATTEMPTS"
  echo "TIMESCALE_SCHEMA_INIT_APPLY_RETRY_INTERVAL_SEC=$APPLY_RETRY_INTERVAL_SEC"
  echo "TIMESCALE_SCHEMA_INIT_TOTAL_STEPS=${#steps_status[@]}"
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
