#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="infra/docker-compose.single-host.m2.yaml"
PROJECT_NAME="quant-hft-single-host-m2"
ENV_FILE="infra/env/prodlike.env"
DOCKER_BIN="docker"
KAFKA_SERVICE="kafka"
KAFKA_BOOTSTRAP_SERVER="localhost:9092"
KAFKA_TOPICS_BIN="/opt/bitnami/kafka/bin/kafka-topics.sh"
MARKET_TOPIC="quant_hft.market.snapshots.v1"
CDC_TOPIC_PREFIX="quant_hft.trading_core"
OUTPUT_FILE="docs/results/kafka_topics_init_result.env"
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --compose-file <path>           Docker compose file path
  --project-name <name>           Compose project name
  --env-file <path>               Env file passed to compose
  --docker-bin <path>             Docker binary (default: docker)
  --kafka-service <name>          Kafka service name (default: kafka)
  --kafka-bootstrap-server <addr> Kafka bootstrap address inside container
  --kafka-topics-bin <path>       kafka-topics.sh path in container
  --market-topic <topic>          Market snapshots topic name
  --cdc-topic-prefix <prefix>     CDC topic prefix (default: quant_hft.trading_core)
  --output-file <path>            Evidence env output path
  --dry-run                       Print/record commands without executing (default)
  --execute                       Execute commands
  -h, --help                      Show this help
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
    --kafka-service)
      KAFKA_SERVICE="${2:-}"
      shift 2
      ;;
    --kafka-bootstrap-server)
      KAFKA_BOOTSTRAP_SERVER="${2:-}"
      shift 2
      ;;
    --kafka-topics-bin)
      KAFKA_TOPICS_BIN="${2:-}"
      shift 2
      ;;
    --market-topic)
      MARKET_TOPIC="${2:-}"
      shift 2
      ;;
    --cdc-topic-prefix)
      CDC_TOPIC_PREFIX="${2:-}"
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

compose_base=("$DOCKER_BIN" "compose" "-f" "$COMPOSE_FILE" "--project-name" "$PROJECT_NAME")
if [[ -f "$ENV_FILE" ]]; then
  compose_base+=("--env-file" "$ENV_FILE")
fi

topic_specs=(
  "${MARKET_TOPIC}|6|1|cleanup.policy=delete,retention.ms=259200000,compression.type=lz4"
  "${CDC_TOPIC_PREFIX}.order_events|3|1|cleanup.policy=delete,retention.ms=604800000,compression.type=lz4"
  "${CDC_TOPIC_PREFIX}.trade_events|3|1|cleanup.policy=delete,retention.ms=604800000,compression.type=lz4"
  "${CDC_TOPIC_PREFIX}.account_snapshots|3|1|cleanup.policy=delete,retention.ms=604800000,compression.type=lz4"
  "${CDC_TOPIC_PREFIX}.position_snapshots|3|1|cleanup.policy=delete,retention.ms=604800000,compression.type=lz4"
  "quant_hft.connect.configs|1|1|cleanup.policy=compact"
  "quant_hft.connect.offsets|1|1|cleanup.policy=compact"
  "quant_hft.connect.status|1|1|cleanup.policy=compact"
)

steps_name=()
steps_cmd=()
steps_status=()
steps_duration_ms=()
failed_step=""

run_create_topic() {
  local topic="$1"
  local partitions="$2"
  local replication="$3"
  local configs_csv="$4"
  local command=(
    "${compose_base[@]}"
    exec
    -T
    "$KAFKA_SERVICE"
    "$KAFKA_TOPICS_BIN"
    --bootstrap-server
    "$KAFKA_BOOTSTRAP_SERVER"
    --create
    --if-not-exists
    --topic
    "$topic"
    --partitions
    "$partitions"
    --replication-factor
    "$replication"
  )

  IFS=',' read -r -a cfg_items <<<"$configs_csv"
  for cfg in "${cfg_items[@]}"; do
    command+=(--config "$cfg")
  done
  "${command[@]}"
}

for spec in "${topic_specs[@]}"; do
  IFS='|' read -r topic partitions replication configs_csv <<<"$spec"
  step_name="create_topic_${topic}"
  step_cmd="${compose_base[*]} exec -T $KAFKA_SERVICE $KAFKA_TOPICS_BIN --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic $topic --partitions $partitions --replication-factor $replication --config ${configs_csv//,/ --config }"
  steps_name+=("$step_name")
  steps_cmd+=("$step_cmd")
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
    IFS='|' read -r topic partitions replication configs_csv <<<"${topic_specs[$idx]}"
    run_create_topic "$topic" "$partitions" "$replication" "$configs_csv"
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
  echo "KAFKA_TOPICS_INIT_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "KAFKA_TOPICS_INIT_SUCCESS=$success"
  echo "KAFKA_TOPICS_INIT_FAILED_STEP=$failed_step"
  echo "KAFKA_TOPICS_INIT_COMPOSE_FILE=$COMPOSE_FILE"
  echo "KAFKA_TOPICS_INIT_PROJECT_NAME=$PROJECT_NAME"
  echo "KAFKA_TOPICS_INIT_ENV_FILE=$ENV_FILE"
  echo "KAFKA_TOPICS_INIT_KAFKA_SERVICE=$KAFKA_SERVICE"
  echo "KAFKA_TOPICS_INIT_MARKET_TOPIC=$MARKET_TOPIC"
  echo "KAFKA_TOPICS_INIT_CDC_TOPIC_PREFIX=$CDC_TOPIC_PREFIX"
  echo "KAFKA_TOPICS_INIT_TOTAL_STEPS=${#steps_status[@]}"
  for idx in "${!steps_status[@]}"; do
    n=$((idx + 1))
    echo "STEP_${n}_NAME=${steps_name[$idx]}"
    echo "STEP_${n}_STATUS=${steps_status[$idx]}"
    echo "STEP_${n}_DURATION_MS=${steps_duration_ms[$idx]}"
    echo "STEP_${n}_COMMAND=${steps_cmd[$idx]}"
  done
} >"$OUTPUT_FILE"

echo "$OUTPUT_FILE"
if [[ "$success" == "true" ]]; then
  exit 0
fi
exit 2
