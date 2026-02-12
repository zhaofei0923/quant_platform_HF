#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="infra/docker-compose.single-host.yaml"
PROJECT_NAME="quant-hft-single-host"
ENV_FILE="infra/env/prodlike.env"
KAFKA_SERVICE="kafka"
KAFKA_BOOTSTRAP_SERVER="kafka:9092"
KAFKA_TOPICS="market.ticks.v1,quant_hft_pg.trading_core.orders,quant_hft_pg.trading_core.trades,quant_hft_pg.trading_core.position_detail,quant_hft_pg.trading_core.account_funds,quant_hft_pg.trading_core.risk_events"
KAFKA_TOPIC=""
KAFKA_PARTITIONS=6
KAFKA_REPLICATION_FACTOR=1
OUTPUT_FILE="docs/results/kafka_topic_init_result.env"
DOCKER_BIN="docker"
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --compose-file <path>            Docker compose file path
  --project-name <name>            Compose project name
  --env-file <path>                Env file passed to compose
  --kafka-service <name>           Kafka service name (default: kafka)
  --kafka-bootstrap-server <addr>  Kafka bootstrap server (default: kafka:9092)
  --kafka-topics <csv>             Kafka topic csv list (default: market + CDC topics)
  --kafka-topic <name>             Single kafka topic (compat option, overrides --kafka-topics)
  --kafka-partitions <n>           Kafka partitions (default: 6)
  --kafka-replication-factor <n>   Kafka replication factor (default: 1)
  --output-file <path>             Evidence env output path
  --docker-bin <path>              Docker binary (default: docker)
  --dry-run                        Print/record commands without executing (default)
  --execute                        Execute commands
  -h, --help                       Show this help
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
    --kafka-service)
      KAFKA_SERVICE="${2:-}"
      shift 2
      ;;
    --kafka-bootstrap-server)
      KAFKA_BOOTSTRAP_SERVER="${2:-}"
      shift 2
      ;;
    --kafka-topic)
      KAFKA_TOPIC="${2:-}"
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
if [[ -n "$KAFKA_TOPIC" ]]; then
  KAFKA_TOPICS="$KAFKA_TOPIC"
fi
declare -a topics=()
IFS=',' read -r -a raw_topics <<< "$KAFKA_TOPICS"
for raw in "${raw_topics[@]}"; do
  topic="$(echo "$raw" | xargs)"
  if [[ -n "$topic" ]]; then
    topics+=("$topic")
  fi
done
if [[ ${#topics[@]} -eq 0 ]]; then
  echo "error: kafka topics must not be empty" >&2
  exit 2
fi

compose_base=("$DOCKER_BIN" "compose" "-f" "$COMPOSE_FILE" "--project-name" "$PROJECT_NAME")
if [[ -f "$ENV_FILE" ]]; then
  compose_base+=("--env-file" "$ENV_FILE")
fi

steps_name=()
steps_cmd=()
for topic in "${topics[@]}"; do
  exists_check_cmd="${compose_base[*]} exec -T $KAFKA_SERVICE rpk topic list --brokers $KAFKA_BOOTSTRAP_SERVER | awk 'NR>1 {print \$1}' | grep -Fx '$topic' >/dev/null"
  create_cmd="${compose_base[*]} exec -T $KAFKA_SERVICE rpk topic create $topic --brokers $KAFKA_BOOTSTRAP_SERVER --partitions $KAFKA_PARTITIONS --replicas $KAFKA_REPLICATION_FACTOR"
  steps_name+=("create_${topic}")
  steps_cmd+=("if $exists_check_cmd; then echo \"topic already exists: $topic\"; else $create_cmd; fi")
  steps_name+=("verify_${topic}")
  steps_cmd+=("${compose_base[*]} exec -T $KAFKA_SERVICE rpk topic describe $topic --brokers $KAFKA_BOOTSTRAP_SERVER")
done
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
    bash -lc "$command_text"
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
  echo "KAFKA_TOPIC_INIT_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "KAFKA_TOPIC_INIT_SUCCESS=$success"
  echo "KAFKA_TOPIC_INIT_FAILED_STEP=$failed_step"
  echo "KAFKA_TOPIC_INIT_COMPOSE_FILE=$COMPOSE_FILE"
  echo "KAFKA_TOPIC_INIT_PROJECT_NAME=$PROJECT_NAME"
  echo "KAFKA_TOPIC_INIT_ENV_FILE=$ENV_FILE"
  echo "KAFKA_TOPIC_INIT_SERVICE=$KAFKA_SERVICE"
  echo "KAFKA_TOPIC_INIT_BOOTSTRAP_SERVER=$KAFKA_BOOTSTRAP_SERVER"
  echo "KAFKA_TOPIC_INIT_TOPIC=$KAFKA_TOPIC"
  echo "KAFKA_TOPIC_INIT_TOPICS=$KAFKA_TOPICS"
  echo "KAFKA_TOPIC_INIT_TOPIC_COUNT=${#topics[@]}"
  echo "KAFKA_TOPIC_INIT_PARTITIONS=$KAFKA_PARTITIONS"
  echo "KAFKA_TOPIC_INIT_REPLICATION_FACTOR=$KAFKA_REPLICATION_FACTOR"
  echo "KAFKA_TOPIC_INIT_TOTAL_STEPS=${#steps_status[@]}"
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
