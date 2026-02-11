#!/usr/bin/env bash
set -euo pipefail

ACTION="up"
COMPOSE_FILE="infra/docker-compose.prodlike.yaml"
PROJECT_NAME="quant-hft-prodlike"
ENV_FILE="infra/env/prodlike.env"
OUTPUT_FILE="docs/results/prodlike_bootstrap_result.env"
HEALTH_REPORT="docs/results/prodlike_health_report.json"
DOCKER_BIN="docker"
HEALTH_CHECK_SCRIPT="scripts/infra/check_prodlike_health.py"
DRY_RUN=1

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --action <up|down|ps|health>   Action to perform (default: up)
  --compose-file <path>          Docker compose file path
  --project-name <name>          Compose project name
  --env-file <path>              Environment file passed to compose
  --output-file <path>           Evidence env output path
  --health-report <path>         Health report JSON output path
  --docker-bin <path>            Docker binary (default: docker)
  --health-check-script <path>   Health checker script path
  --dry-run                      Print/record commands without executing (default)
  --execute                      Execute commands
  -h, --help                     Show this help
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

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "error: compose file not found: $COMPOSE_FILE" >&2
  exit 2
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
  steps_name+=("health_check")
  steps_cmd+=("python3 $HEALTH_CHECK_SCRIPT --compose-file $COMPOSE_FILE --project-name $PROJECT_NAME --docker-bin $DOCKER_BIN --report-json $HEALTH_REPORT")
elif [[ "$ACTION" == "down" ]]; then
  steps_name+=("compose_down")
  steps_cmd+=("${compose_base[*]} down --remove-orphans")
elif [[ "$ACTION" == "ps" ]]; then
  steps_name+=("compose_ps")
  steps_cmd+=("${compose_base[*]} ps")
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
  echo "PRODLIKE_ACTION=$ACTION"
  echo "PRODLIKE_DRY_RUN=$([[ "$DRY_RUN" -eq 1 ]] && echo 1 || echo 0)"
  echo "PRODLIKE_SUCCESS=$success"
  echo "PRODLIKE_TOTAL_STEPS=${#steps_status[@]}"
  echo "PRODLIKE_FAILED_STEP=$failed_step"
  echo "PRODLIKE_COMPOSE_FILE=$COMPOSE_FILE"
  echo "PRODLIKE_PROJECT_NAME=$PROJECT_NAME"
  echo "PRODLIKE_ENV_FILE=$ENV_FILE"
  echo "PRODLIKE_HEALTH_REPORT=$HEALTH_REPORT"
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
