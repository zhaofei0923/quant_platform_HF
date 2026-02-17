#!/usr/bin/env bash
set -euo pipefail

build_dir="build"
results_dir="docs/results"
env_name="preprod-local"
max_rollback_seconds="180"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --build-dir PATH             Build directory containing C++ CLIs (default: build)
  --results-dir PATH           Directory for generated rehearsal evidence (default: docs/results)
  --env-name NAME              Logical environment name in env evidence (default: preprod-local)
  --max-rollback-seconds N     Rollback SLO seconds in template (default: 180)
  -h, --help                   Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      build_dir="$2"
      shift 2
      ;;
    --results-dir)
      results_dir="$2"
      shift 2
      ;;
    --env-name)
      env_name="$2"
      shift 2
      ;;
    --max-rollback-seconds)
      max_rollback_seconds="$2"
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

if [[ ! -x "${build_dir}/reconnect_evidence_cli" ]]; then
  echo "error: missing executable: ${build_dir}/reconnect_evidence_cli" >&2
  exit 2
fi
if [[ ! -x "${build_dir}/ctp_cutover_orchestrator_cli" ]]; then
  echo "error: missing executable: ${build_dir}/ctp_cutover_orchestrator_cli" >&2
  exit 2
fi

mkdir -p "${results_dir}"
mkdir -p runtime

reconnect_report_md="${results_dir}/preprod_reconnect_fault_result.md"
ops_health_json="${results_dir}/preprod_ops_health_report.json"
ops_health_md="${results_dir}/preprod_ops_health_report.md"
ops_alert_json="${results_dir}/preprod_ops_alert_report.json"
ops_alert_md="${results_dir}/preprod_ops_alert_report.md"

"${build_dir}/reconnect_evidence_cli" \
  --report_file "${reconnect_report_md}" \
  --health_json_file "${ops_health_json}" \
  --health_markdown_file "${ops_health_md}" \
  --alert_json_file "${ops_alert_json}" \
  --alert_markdown_file "${ops_alert_md}" \
  --strategy-engine-chain-status complete \
  --strategy-engine-chain-source in_process \
  --strategy-engine-state-key-count 2 \
  --strategy-engine-intent-count 1 \
  --strategy-engine-order-key-count 1 \
  --storage-redis-health healthy \
  --storage-timescale-health healthy >/dev/null

tmp_dir="$(mktemp -d /tmp/quant_hft_preprod_rehearsal_gate.XXXXXX)"
trap 'rm -rf "${tmp_dir}"' EXIT

cutover_template="${tmp_dir}/cutover.env"
rollback_template="${tmp_dir}/rollback.env"
cutover_env="${results_dir}/preprod_cutover_result.env"
rollback_env="${results_dir}/preprod_rollback_result.env"

cat >"${cutover_template}" <<EOF
CUTOVER_ENV_NAME=${env_name}
CUTOVER_WINDOW_LOCAL=2026-02-17T09:00:00+08:00
CTP_CONFIG_PATH=configs/prod/ctp.yaml
OLD_CORE_ENGINE_STOP_CMD=bash -lc 'echo preprod-rehearsal: stop old core engine'
PRECHECK_CMD=bash -lc 'echo preprod-rehearsal: precheck pass'
BOOTSTRAP_INFRA_CMD=bash -lc 'echo preprod-rehearsal: bootstrap infra'
INIT_KAFKA_TOPIC_CMD=bash -lc 'echo preprod-rehearsal: init kafka topic'
INIT_CLICKHOUSE_SCHEMA_CMD=bash -lc 'echo preprod-rehearsal: init clickhouse schema'
INIT_DEBEZIUM_CONNECTOR_CMD=bash -lc 'echo preprod-rehearsal: init debezium connector'
NEW_CORE_ENGINE_START_CMD=bash -lc 'echo preprod-rehearsal: start new core engine'
WARMUP_QUERY_CMD=bash -lc 'echo preprod-rehearsal: warmup query pass'
POST_SWITCH_MONITOR_MINUTES=30
MONITOR_KEYS=order_latency_p99_ms,reconnect_p99_s,quant_hft_strategy_engine_chain_integrity
CUTOVER_EVIDENCE_OUTPUT=${cutover_env}
EOF

cat >"${rollback_template}" <<EOF
ROLLBACK_ENV_NAME=${env_name}
ROLLBACK_TRIGGER_CONDITION=forced_rehearsal_validation
NEW_CORE_ENGINE_STOP_CMD=bash -lc 'echo preprod-rehearsal: stop new core engine'
RESTORE_PREVIOUS_BINARIES_CMD=bash -lc 'echo preprod-rehearsal: restore previous binaries'
RESTORE_STRATEGY_ENGINE_COMPAT_CMD=bash -lc 'echo preprod-rehearsal: restore strategy engine compat'
PREVIOUS_CORE_ENGINE_START_CMD=bash -lc 'echo preprod-rehearsal: start previous core engine'
POST_ROLLBACK_VALIDATE_CMD=bash -lc 'echo preprod-rehearsal: rollback validate pass'
MAX_ROLLBACK_SECONDS=${max_rollback_seconds}
ROLLBACK_EVIDENCE_OUTPUT=${rollback_env}
EOF

"${build_dir}/ctp_cutover_orchestrator_cli" \
  --cutover-template "${cutover_template}" \
  --rollback-template "${rollback_template}" \
  --cutover-output "${cutover_env}" \
  --rollback-output "${rollback_env}" \
  --execute \
  --force-rollback >/dev/null

env_value() {
  local key="$1"
  local file="$2"
  awk -F= -v target="${key}" '$1 == target {sub(/^[^=]*=/, ""); print; exit}' "${file}"
}

rollback_drill="pass"
rollback_reason="ok"
if [[ "$(env_value CUTOVER_SUCCESS "${cutover_env}")" != "true" ]]; then
  rollback_drill="fail"
  rollback_reason="cutover_execute_failed"
elif [[ "$(env_value CUTOVER_TRIGGERED_ROLLBACK "${cutover_env}")" != "true" ]]; then
  rollback_drill="fail"
  rollback_reason="rollback_not_triggered_from_cutover"
elif [[ "$(env_value ROLLBACK_TRIGGERED "${rollback_env}")" != "true" ]]; then
  rollback_drill="fail"
  rollback_reason="rollback_not_triggered"
elif [[ "$(env_value ROLLBACK_SUCCESS "${rollback_env}")" != "true" ]]; then
  rollback_drill="fail"
  rollback_reason="rollback_failed"
elif [[ -z "$(env_value ROLLBACK_STARTED_UTC "${rollback_env}")" ]]; then
  rollback_drill="fail"
  rollback_reason="rollback_start_timestamp_missing"
elif [[ -z "$(env_value ROLLBACK_COMPLETED_UTC "${rollback_env}")" ]]; then
  rollback_drill="fail"
  rollback_reason="rollback_complete_timestamp_missing"
fi

ops_evidence="pass"
if [[ ! -f "${ops_health_json}" || ! -f "${ops_alert_json}" || ! -f "${reconnect_report_md}" ]]; then
  ops_evidence="fail"
fi

status="pass"
if [[ "${rollback_drill}" != "pass" || "${ops_evidence}" != "pass" ]]; then
  status="fail"
fi

generated_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
report_json="${results_dir}/preprod_rehearsal_report.json"
report_md="${results_dir}/preprod_rehearsal_report.md"

cat >"${report_json}" <<EOF
{
  "status": "${status}",
  "generated_utc": "${generated_utc}",
  "env_name": "${env_name}",
  "ops_evidence": "${ops_evidence}",
  "rollback_drill": "${rollback_drill}",
  "rollback_reason": "${rollback_reason}",
  "cutover_env": "${cutover_env}",
  "rollback_env": "${rollback_env}",
  "ops_health_json": "${ops_health_json}",
  "ops_alert_json": "${ops_alert_json}",
  "reconnect_report_md": "${reconnect_report_md}"
}
EOF

cat >"${report_md}" <<EOF
# Preprod Full Rehearsal Report

- generated_utc: ${generated_utc}
- status: ${status}
- env_name: ${env_name}
- ops_evidence: ${ops_evidence}
- rollback_drill: ${rollback_drill}
- rollback_reason: ${rollback_reason}
- cutover_env: ${cutover_env}
- rollback_env: ${rollback_env}
- ops_health_json: ${ops_health_json}
- ops_alert_json: ${ops_alert_json}
- reconnect_report_md: ${reconnect_report_md}
EOF

if [[ "${status}" != "pass" ]]; then
  echo "preprod rehearsal gate failed: ${report_json}" >&2
  exit 1
fi

echo "preprod rehearsal gate passed: ${report_json}"
