#!/usr/bin/env bash
set -euo pipefail

ACTION="up"
COMPOSE_FILE="infra/docker-compose.prodlike.multi-host.yaml"
PROJECT_NAME="quant-hft-prodlike-multi-host"
PRIMARY_ENV_FILE="infra/env/prodlike-primary.env"
STANDBY_ENV_FILE="infra/env/prodlike-standby.env"
OUTPUT_FILE="docs/results/prodlike_multi_host_bootstrap_result.env"
HEALTH_REPORT="docs/results/prodlike_multi_host_health_report.json"
DOCKER_BIN="docker"
HEALTH_CHECK_SCRIPT="scripts/infra/check_prodlike_multi_host_health.py"
KAFKA_INIT_SCRIPT="scripts/infra/init_kafka_topics.sh"
DEBEZIUM_INIT_SCRIPT="scripts/infra/init_debezium_connectors.sh"
CLICKHOUSE_INIT_SCRIPT="scripts/infra/init_clickhouse_schema.sh"
KAFKA_EVIDENCE="docs/results/kafka_topic_init_result.env"
DEBEZIUM_EVIDENCE="docs/results/debezium_connector_init_result.env"
CLICKHOUSE_EVIDENCE="docs/results/clickhouse_schema_init_result.env"
KAFKA_SERVICE="kafka"
KAFKA_BOOTSTRAP_SERVER="kafka:9092"
KAFKA_TOPICS="market.ticks.v1,quant_hft_pg.trading_core.orders,quant_hft_pg.trading_core.trades,quant_hft_pg.trading_core.position_detail,quant_hft_pg.trading_core.account_funds,quant_hft_pg.trading_core.risk_events"
KAFKA_PARTITIONS=6
KAFKA_REPLICATION_FACTOR=1
DEBEZIUM_CONNECT_URL="http://kafka-connect:8083"
CLICKHOUSE_SERVICE="clickhouse"
CLICKHOUSE_DB="quant_hft"
CLICKHOUSE_SCHEMA_DIR="infra/clickhouse/init"
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --action <up|down|status|health>  Action to perform (default: up)
  --compose-file <path>             Docker compose file path
  --project-name <name>             Compose project name
  --primary-env-file <path>         Primary env file
  --standby-env-file <path>         Standby env file
  --output-file <path>              Evidence env output path
  --health-report <path>            Health report JSON output path
  --docker-bin <path>               Docker binary (default: docker)
  --health-check-script <path>      Health checker script path
  --kafka-init-script <path>        Kafka topic initializer script path
  --debezium-init-script <path>     Debezium connector initializer script path
  --clickhouse-init-script <path>   ClickHouse schema initializer script path
  --kafka-evidence <path>           Kafka topic initializer evidence output path
  --debezium-evidence <path>        Debezium initializer evidence output path
  --clickhouse-evidence <path>      ClickHouse schema initializer evidence output path
  --kafka-service <name>            Kafka service name (default: kafka)
  --kafka-bootstrap-server <addr>   Kafka bootstrap server (default: kafka:9092)
  --kafka-topics <csv>              Kafka topic csv list
  --kafka-topic <name>              Kafka topic (compat option, overrides --kafka-topics)
  --kafka-partitions <n>            Kafka partitions (default: 6)
  --kafka-replication-factor <n>    Kafka replication factor (default: 1)
  --debezium-connect-url <url>      Debezium Kafka Connect URL (default: http://kafka-connect:8083)
  --clickhouse-service <name>       ClickHouse service name (default: clickhouse)
  --clickhouse-db <name>            ClickHouse database name (default: quant_hft)
  --clickhouse-schema-dir <path>    ClickHouse schema directory path
  --dry-run                         Print/record commands without executing (default)
  --execute                         Execute commands
  -h, --help                        Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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
    --primary-env-file)
      PRIMARY_ENV_FILE="${2:-}"
      shift 2
      ;;
    --standby-env-file)
      STANDBY_ENV_FILE="${2:-}"
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
    --kafka-init-script)
      KAFKA_INIT_SCRIPT="${2:-}"
      shift 2
      ;;
    --debezium-init-script)
      DEBEZIUM_INIT_SCRIPT="${2:-}"
      shift 2
      ;;
    --clickhouse-init-script)
      CLICKHOUSE_INIT_SCRIPT="${2:-}"
      shift 2
      ;;
    --kafka-evidence)
      KAFKA_EVIDENCE="${2:-}"
      shift 2
      ;;
    --debezium-evidence)
      DEBEZIUM_EVIDENCE="${2:-}"
      shift 2
      ;;
    --clickhouse-evidence)
      CLICKHOUSE_EVIDENCE="${2:-}"
      shift 2
      ;;
    --kafka-service)
      KAFKA_SERVICE="${2:-}"
      shift 2
      ;;
    --kafka-bootstrap-server)
      KAFKA_BOOTSTRAP_SERVER="${2:-}"
      shift 2
      ;;
    --kafka-topic)
      KAFKA_TOPICS="${2:-}"
      shift 2
      ;;
    --kafka-topics)
      KAFKA_TOPICS="${2:-}"
      shift 2
      ;;
    --kafka-partitions)
      KAFKA_PARTITIONS="${2:-}"
      shift 2
      ;;
    --kafka-replication-factor)
      KAFKA_REPLICATION_FACTOR="${2:-}"
      shift 2
      ;;
    --debezium-connect-url)
      DEBEZIUM_CONNECT_URL="${2:-}"
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
    --clickhouse-schema-dir)
      CLICKHOUSE_SCHEMA_DIR="${2:-}"
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
  up|down|status|health) ;;
  *)
    echo "error: --action must be one of up|down|status|health" >&2
    exit 2
    ;;
esac

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "error: compose file not found: $COMPOSE_FILE" >&2
  exit 2
fi
if [[ ! -f "$PRIMARY_ENV_FILE" ]]; then
  echo "error: primary env file not found: $PRIMARY_ENV_FILE" >&2
  exit 2
fi
if [[ ! -f "$STANDBY_ENV_FILE" ]]; then
  echo "error: standby env file not found: $STANDBY_ENV_FILE" >&2
  exit 2
fi
if [[ "$ACTION" == "up" ]]; then
  if [[ ! -f "$KAFKA_INIT_SCRIPT" ]]; then
    echo "error: kafka init script not found: $KAFKA_INIT_SCRIPT" >&2
    exit 2
  fi
  if [[ ! -f "$DEBEZIUM_INIT_SCRIPT" ]]; then
    echo "error: debezium init script not found: $DEBEZIUM_INIT_SCRIPT" >&2
    exit 2
  fi
  if [[ ! -f "$CLICKHOUSE_INIT_SCRIPT" ]]; then
    echo "error: clickhouse init script not found: $CLICKHOUSE_INIT_SCRIPT" >&2
    exit 2
  fi
  if [[ ! -d "$CLICKHOUSE_SCHEMA_DIR" ]]; then
    echo "error: clickhouse schema dir not found: $CLICKHOUSE_SCHEMA_DIR" >&2
    exit 2
  fi
fi

steps_name=()
steps_cmd=()
steps_status=()
steps_duration_ms=()

compose_base=("$DOCKER_BIN" "compose" "-f" "$COMPOSE_FILE" "--project-name" "$PROJECT_NAME")

if [[ "$ACTION" == "up" ]]; then
  steps_name+=("compose_up_primary")
  steps_cmd+=("${compose_base[*]} --env-file $PRIMARY_ENV_FILE up -d")
  steps_name+=("compose_up_standby")
  steps_cmd+=("${compose_base[*]} --env-file $STANDBY_ENV_FILE up -d")
  steps_name+=("kafka_topic_init")
  if [[ "$DRY_RUN" -eq 1 ]]; then
    steps_cmd+=("bash $KAFKA_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $PRIMARY_ENV_FILE --kafka-service $KAFKA_SERVICE --kafka-bootstrap-server $KAFKA_BOOTSTRAP_SERVER --kafka-topics $KAFKA_TOPICS --kafka-partitions $KAFKA_PARTITIONS --kafka-replication-factor $KAFKA_REPLICATION_FACTOR --docker-bin $DOCKER_BIN --output-file $KAFKA_EVIDENCE --dry-run")
  else
    steps_cmd+=("bash $KAFKA_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $PRIMARY_ENV_FILE --kafka-service $KAFKA_SERVICE --kafka-bootstrap-server $KAFKA_BOOTSTRAP_SERVER --kafka-topics $KAFKA_TOPICS --kafka-partitions $KAFKA_PARTITIONS --kafka-replication-factor $KAFKA_REPLICATION_FACTOR --docker-bin $DOCKER_BIN --output-file $KAFKA_EVIDENCE --execute")
  fi
  steps_name+=("debezium_connector_init")
  if [[ "$DRY_RUN" -eq 1 ]]; then
    steps_cmd+=("bash $DEBEZIUM_INIT_SCRIPT --connect-url $DEBEZIUM_CONNECT_URL --output-file $DEBEZIUM_EVIDENCE --dry-run")
  else
    steps_cmd+=("bash $DEBEZIUM_INIT_SCRIPT --connect-url $DEBEZIUM_CONNECT_URL --output-file $DEBEZIUM_EVIDENCE --execute")
  fi
  steps_name+=("clickhouse_schema_init")
  if [[ "$DRY_RUN" -eq 1 ]]; then
    steps_cmd+=("bash $CLICKHOUSE_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $PRIMARY_ENV_FILE --schema-dir $CLICKHOUSE_SCHEMA_DIR --clickhouse-service $CLICKHOUSE_SERVICE --clickhouse-db $CLICKHOUSE_DB --docker-bin $DOCKER_BIN --output-file $CLICKHOUSE_EVIDENCE --dry-run")
  else
    steps_cmd+=("bash $CLICKHOUSE_INIT_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --env-file $PRIMARY_ENV_FILE --schema-dir $CLICKHOUSE_SCHEMA_DIR --clickhouse-service $CLICKHOUSE_SERVICE --clickhouse-db $CLICKHOUSE_DB --docker-bin $DOCKER_BIN --output-file $CLICKHOUSE_EVIDENCE --execute")
  fi
  steps_name+=("health_check")
  steps_cmd+=("python3 $HEALTH_CHECK_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --docker-bin $DOCKER_BIN --report-json $HEALTH_REPORT")
elif [[ "$ACTION" == "down" ]]; then
  steps_name+=("compose_down")
  steps_cmd+=("${compose_base[*]} down --remove-orphans")
elif [[ "$ACTION" == "status" ]]; then
  steps_name+=("compose_status")
  steps_cmd+=("${compose_base[*]} ps --format json")
else
  steps_name+=("health_check")
  steps_cmd+=("python3 $HEALTH_CHECK_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --docker-bin $DOCKER_BIN --report-json $HEALTH_REPORT")
fi

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
    bash -lc "$command"
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
  echo "MULTI_HOST_ACTION=$ACTION"
  echo "MULTI_HOST_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "MULTI_HOST_SUCCESS=$success"
  echo "MULTI_HOST_TOTAL_STEPS=${#steps_status[@]}"
  echo "MULTI_HOST_FAILED_STEP=$failed_step"
  echo "MULTI_HOST_COMPOSE_FILE=$COMPOSE_FILE"
  echo "MULTI_HOST_PROJECT_NAME=$PROJECT_NAME"
  echo "MULTI_HOST_PRIMARY_ENV_FILE=$PRIMARY_ENV_FILE"
  echo "MULTI_HOST_STANDBY_ENV_FILE=$STANDBY_ENV_FILE"
  echo "MULTI_HOST_HEALTH_REPORT=$HEALTH_REPORT"
  echo "MULTI_HOST_KAFKA_TOPIC_EVIDENCE=$KAFKA_EVIDENCE"
  echo "MULTI_HOST_DEBEZIUM_EVIDENCE=$DEBEZIUM_EVIDENCE"
  echo "MULTI_HOST_CLICKHOUSE_SCHEMA_EVIDENCE=$CLICKHOUSE_EVIDENCE"
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
