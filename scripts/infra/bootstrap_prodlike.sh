#!/usr/bin/env bash
set -euo pipefail

ACTION="up"
PROFILE="single-host"
COMPOSE_FILE=""
PROJECT_NAME=""
ENV_FILE="infra/env/prodlike.env"
OUTPUT_FILE="docs/results/prodlike_bootstrap_result.env"
HEALTH_REPORT="docs/results/prodlike_health_report.json"
DOCKER_BIN="docker"
HEALTH_CHECK_SCRIPT="scripts/infra/check_prodlike_health.py"
SCHEMA_INIT_SCRIPT="scripts/infra/init_timescale_schema.sh"
SCHEMA_FILE="infra/timescale/init/001_quant_hft_schema.sql"
SCHEMA_EVIDENCE="docs/results/timescale_schema_init_result.env"
CLICKHOUSE_SCHEMA_INIT_SCRIPT="scripts/infra/init_clickhouse_schema.sh"
CLICKHOUSE_SCHEMA_EVIDENCE="docs/results/clickhouse_schema_init_result.env"
KAFKA_TOPICS_INIT_SCRIPT="scripts/infra/init_kafka_topics.sh"
KAFKA_TOPICS_EVIDENCE="docs/results/kafka_topics_init_result.env"
TIMESCALE_SERVICE="timescale-primary"
TIMESCALE_DB="quant_hft"
TIMESCALE_USER="quant_hft"
HEALTH_WAIT_TIMEOUT_SEC=120
HEALTH_WAIT_POLL_INTERVAL_SEC=3
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --profile <single-host|single-host-m2|prodlike>  Deployment profile (default: single-host)
  --action <up|down|ps|health>   Action to perform (default: up)
  --compose-file <path>          Docker compose file path (default by profile)
  --project-name <name>          Compose project name (default by profile)
  --env-file <path>              Environment file passed to compose
  --output-file <path>           Evidence env output path
  --health-report <path>         Health report JSON output path
  --docker-bin <path>            Docker binary (default: docker)
  --health-check-script <path>   Health checker script path
  --schema-init-script <path>    Timescale schema initializer script path
  --schema-file <path>           Timescale SQL schema file path
  --schema-evidence <path>       Timescale schema initializer evidence output path
  --clickhouse-schema-init-script <path> ClickHouse schema initializer script path
  --clickhouse-schema-evidence <path>    ClickHouse schema initializer evidence output path
  --kafka-topics-init-script <path>      Kafka topics initializer script path
  --kafka-topics-evidence <path>         Kafka topics initializer evidence output path
  --timescale-service <name>     Timescale service name (default: timescale-primary)
  --timescale-db <name>          Timescale database name (default: quant_hft)
  --timescale-user <name>        Timescale database user (default: quant_hft)
  --health-wait-timeout-sec <sec> Wait timeout for health check (default: 120)
  --health-wait-poll-interval-sec <sec> Poll interval for health check (default: 3)
  --dry-run                      Print/record commands without executing (default)
  --execute                      Execute commands
  -h, --help                     Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --action)
      ACTION="${2:-}"
      shift 2
      ;;
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
    --output-file)
      OUTPUT_FILE="${2:-}"
      shift 2
      ;;
    --health-report)
      HEALTH_REPORT="${2:-}"
      shift 2
      ;;
    --docker-bin)
      DOCKER_BIN="${2:-}"
      shift 2
      ;;
    --health-check-script)
      HEALTH_CHECK_SCRIPT="${2:-}"
      shift 2
      ;;
    --schema-init-script)
      SCHEMA_INIT_SCRIPT="${2:-}"
      shift 2
      ;;
    --schema-file)
      SCHEMA_FILE="${2:-}"
      shift 2
      ;;
    --schema-evidence)
      SCHEMA_EVIDENCE="${2:-}"
      shift 2
      ;;
    --clickhouse-schema-init-script)
      CLICKHOUSE_SCHEMA_INIT_SCRIPT="${2:-}"
      shift 2
      ;;
    --clickhouse-schema-evidence)
      CLICKHOUSE_SCHEMA_EVIDENCE="${2:-}"
      shift 2
      ;;
    --kafka-topics-init-script)
      KAFKA_TOPICS_INIT_SCRIPT="${2:-}"
      shift 2
      ;;
    --kafka-topics-evidence)
      KAFKA_TOPICS_EVIDENCE="${2:-}"
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
    --health-wait-timeout-sec)
      HEALTH_WAIT_TIMEOUT_SEC="${2:-}"
      shift 2
      ;;
    --health-wait-poll-interval-sec)
      HEALTH_WAIT_POLL_INTERVAL_SEC="${2:-}"
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

case "$ACTION" in
  up|down|ps|health) ;;
  *)
    echo "error: --action must be one of up|down|ps|health" >&2
    exit 2
    ;;
esac

case "$PROFILE" in
  single-host|single-host-m2|prodlike) ;;
  *)
    echo "error: --profile must be one of single-host|single-host-m2|prodlike" >&2
    exit 2
    ;;
esac

if [[ -z "$COMPOSE_FILE" ]]; then
  case "$PROFILE" in
    single-host)
      COMPOSE_FILE="infra/docker-compose.single-host.yaml"
      ;;
    single-host-m2)
      COMPOSE_FILE="infra/docker-compose.single-host.m2.yaml"
      ;;
    prodlike)
      COMPOSE_FILE="infra/docker-compose.prodlike.yaml"
      ;;
  esac
fi

if [[ -z "$PROJECT_NAME" ]]; then
  case "$PROFILE" in
    single-host)
      PROJECT_NAME="quant-hft-single-host"
      ;;
    single-host-m2)
      PROJECT_NAME="quant-hft-single-host-m2"
      ;;
    prodlike)
      PROJECT_NAME="quant-hft-prodlike"
      ;;
  esac
fi

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "error: compose file not found: $COMPOSE_FILE" >&2
  exit 2
fi
if [[ "$ACTION" == "up" ]]; then
  if [[ ! -f "$SCHEMA_INIT_SCRIPT" ]]; then
    echo "error: schema init script not found: $SCHEMA_INIT_SCRIPT" >&2
    exit 2
  fi
  if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "error: schema file not found: $SCHEMA_FILE" >&2
    exit 2
  fi
  if [[ "$PROFILE" == "single-host-m2" ]]; then
    if [[ ! -f "$CLICKHOUSE_SCHEMA_INIT_SCRIPT" ]]; then
      echo "error: clickhouse schema init script not found: $CLICKHOUSE_SCHEMA_INIT_SCRIPT" >&2
      exit 2
    fi
    if [[ ! -f "$KAFKA_TOPICS_INIT_SCRIPT" ]]; then
      echo "error: kafka topics init script not found: $KAFKA_TOPICS_INIT_SCRIPT" >&2
      exit 2
    fi
  fi
fi

steps_name=()
steps_cmd=()
steps_status=()
steps_duration_ms=()

compose_base=("$DOCKER_BIN" "compose" "-f" "$COMPOSE_FILE" "--project-name" "$PROJECT_NAME")
if [[ -f "$ENV_FILE" ]]; then
  compose_base+=("--env-file" "$ENV_FILE")
fi

if [[ "$ACTION" == "up" ]]; then
  steps_name+=("compose_up")
  steps_cmd+=("${compose_base[*]} up -d")
  steps_name+=("timescale_schema_init")
  if [[ "$DRY_RUN" -eq 1 ]]; then
    steps_cmd+=("bash $SCHEMA_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $ENV_FILE --schema-file $SCHEMA_FILE --timescale-service $TIMESCALE_SERVICE --timescale-db $TIMESCALE_DB --timescale-user $TIMESCALE_USER --docker-bin $DOCKER_BIN --output-file $SCHEMA_EVIDENCE --dry-run")
  else
    steps_cmd+=("bash $SCHEMA_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $ENV_FILE --schema-file $SCHEMA_FILE --timescale-service $TIMESCALE_SERVICE --timescale-db $TIMESCALE_DB --timescale-user $TIMESCALE_USER --docker-bin $DOCKER_BIN --output-file $SCHEMA_EVIDENCE --execute")
  fi
  if [[ "$PROFILE" == "single-host-m2" ]]; then
    steps_name+=("clickhouse_schema_init")
    if [[ "$DRY_RUN" -eq 1 ]]; then
      steps_cmd+=("bash $CLICKHOUSE_SCHEMA_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $ENV_FILE --docker-bin $DOCKER_BIN --output-file $CLICKHOUSE_SCHEMA_EVIDENCE --dry-run")
    else
      steps_cmd+=("bash $CLICKHOUSE_SCHEMA_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $ENV_FILE --docker-bin $DOCKER_BIN --output-file $CLICKHOUSE_SCHEMA_EVIDENCE --execute")
    fi
    steps_name+=("kafka_topics_init")
    if [[ "$DRY_RUN" -eq 1 ]]; then
      steps_cmd+=("bash $KAFKA_TOPICS_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $ENV_FILE --docker-bin $DOCKER_BIN --output-file $KAFKA_TOPICS_EVIDENCE --dry-run")
    else
      steps_cmd+=("bash $KAFKA_TOPICS_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $ENV_FILE --docker-bin $DOCKER_BIN --output-file $KAFKA_TOPICS_EVIDENCE --execute")
    fi
  fi
  steps_name+=("health_check")
  steps_cmd+=("python3 $HEALTH_CHECK_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --docker-bin $DOCKER_BIN --report-json $HEALTH_REPORT (timeout=${HEALTH_WAIT_TIMEOUT_SEC}s interval=${HEALTH_WAIT_POLL_INTERVAL_SEC}s)")
elif [[ "$ACTION" == "down" ]]; then
  steps_name+=("compose_down")
  steps_cmd+=("${compose_base[*]} down --remove-orphans")
elif [[ "$ACTION" == "ps" ]]; then
  steps_name+=("compose_ps")
  steps_cmd+=("${compose_base[*]} ps")
else
  steps_name+=("health_check")
  steps_cmd+=("python3 $HEALTH_CHECK_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --docker-bin $DOCKER_BIN --report-json $HEALTH_REPORT (timeout=${HEALTH_WAIT_TIMEOUT_SEC}s interval=${HEALTH_WAIT_POLL_INTERVAL_SEC}s)")
fi

run_step_health_check() {
  local elapsed=0
  while (( elapsed <= HEALTH_WAIT_TIMEOUT_SEC )); do
    if python3 "$HEALTH_CHECK_SCRIPT" \
      --compose-file "$COMPOSE_FILE" \
      --project-name "$PROJECT_NAME" \
      --docker-bin "$DOCKER_BIN" \
      --report-json "$HEALTH_REPORT"; then
      return 0
    fi
    sleep "$HEALTH_WAIT_POLL_INTERVAL_SEC"
    elapsed=$((elapsed + HEALTH_WAIT_POLL_INTERVAL_SEC))
  done
  return 1
}

failed_step=""
for idx in "${!steps_name[@]}"; do
  name="${steps_name[$idx]}"
  command="${steps_cmd[$idx]}"
  started_ns=$(date +%s%N)

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $command"
    status="simulated_ok"
  else
    set +e
    if [[ "$name" == "health_check" ]]; then
      run_step_health_check
      rc=$?
    else
      bash -lc "$command"
      rc=$?
    fi
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
  echo "PRODLIKE_PROFILE=$PROFILE"
  echo "PRODLIKE_ACTION=$ACTION"
  echo "PRODLIKE_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "PRODLIKE_SUCCESS=$success"
  echo "PRODLIKE_TOTAL_STEPS=${#steps_status[@]}"
  echo "PRODLIKE_FAILED_STEP=$failed_step"
  echo "PRODLIKE_COMPOSE_FILE=$COMPOSE_FILE"
  echo "PRODLIKE_PROJECT_NAME=$PROJECT_NAME"
  echo "PRODLIKE_ENV_FILE=$ENV_FILE"
  echo "PRODLIKE_HEALTH_REPORT=$HEALTH_REPORT"
  echo "PRODLIKE_TIMESCALE_SCHEMA_EVIDENCE=$SCHEMA_EVIDENCE"
  echo "PRODLIKE_CLICKHOUSE_SCHEMA_EVIDENCE=$CLICKHOUSE_SCHEMA_EVIDENCE"
  echo "PRODLIKE_KAFKA_TOPICS_EVIDENCE=$KAFKA_TOPICS_EVIDENCE"
  echo "PRODLIKE_HEALTH_WAIT_TIMEOUT_SEC=$HEALTH_WAIT_TIMEOUT_SEC"
  echo "PRODLIKE_HEALTH_WAIT_POLL_INTERVAL_SEC=$HEALTH_WAIT_POLL_INTERVAL_SEC"
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
