from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _write_cutover_template(path: Path, *, precheck_cmd: str = "echo precheck") -> None:
    path.write_text(
        "\n".join(
            [
                "CUTOVER_ENV_NAME=prodlike",
                "CUTOVER_WINDOW_LOCAL=2026-02-13T09:00:00+08:00",
                "CTP_CONFIG_PATH=configs/prod/ctp.yaml",
                "OLD_CORE_ENGINE_STOP_CMD=echo stop-old-core",
                "OLD_STRATEGY_RUNNER_STOP_CMD=echo stop-old-runner",
                "BOOTSTRAP_INFRA_CMD=echo bootstrap",
                "INIT_KAFKA_TOPIC_CMD=echo init-kafka",
                "INIT_CLICKHOUSE_SCHEMA_CMD=echo init-clickhouse",
                "INIT_DEBEZIUM_CONNECTOR_CMD=echo init-debezium",
                "NEW_CORE_ENGINE_START_CMD=echo start-new-core",
                "NEW_STRATEGY_RUNNER_START_CMD=echo start-new-runner",
                f"PRECHECK_CMD={precheck_cmd}",
                "WARMUP_QUERY_CMD=echo warmup",
                "POST_SWITCH_MONITOR_MINUTES=30",
                "MONITOR_KEYS=order_latency_p99_ms,breaker_state",
                "CUTOVER_EVIDENCE_OUTPUT=docs/results/ctp_cutover_result.env",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_rollback_template(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "ROLLBACK_ENV_NAME=prodlike",
                "ROLLBACK_TRIGGER_CONDITION=order_latency_gt_5ms",
                "NEW_CORE_ENGINE_STOP_CMD=echo stop-new-core",
                "NEW_STRATEGY_RUNNER_STOP_CMD=echo stop-new-runner",
                "RESTORE_PREVIOUS_BINARIES_CMD=echo restore-binaries",
                "RESTORE_REDIS_BRIDGE_COMPAT_CMD=echo restore-redis",
                "PREVIOUS_CORE_ENGINE_START_CMD=echo start-prev-core",
                "PREVIOUS_STRATEGY_RUNNER_START_CMD=echo start-prev-runner",
                "POST_ROLLBACK_VALIDATE_CMD=echo validate-rollback",
                "MAX_ROLLBACK_SECONDS=180",
                "ROLLBACK_EVIDENCE_OUTPUT=docs/results/ctp_rollback_result.env",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_ctp_cutover_orchestrator_dry_run_outputs_cutover_and_rollback_evidence(
    tmp_path: Path,
) -> None:
    cutover_template = tmp_path / "cutover.env"
    rollback_template = tmp_path / "rollback.env"
    cutover_evidence = tmp_path / "cutover_result.env"
    rollback_evidence = tmp_path / "rollback_result.env"
    _write_cutover_template(cutover_template)
    _write_rollback_template(rollback_template)

    command = [
        sys.executable,
        "scripts/ops/ctp_cutover_orchestrator.py",
        "--cutover-template",
        str(cutover_template),
        "--rollback-template",
        str(rollback_template),
        "--cutover-output",
        str(cutover_evidence),
        "--rollback-output",
        str(rollback_evidence),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    cutover_payload = cutover_evidence.read_text(encoding="utf-8")
    rollback_payload = rollback_evidence.read_text(encoding="utf-8")
    assert "CUTOVER_DRY_RUN=1" in cutover_payload
    assert "CUTOVER_SUCCESS=true" in cutover_payload
    assert "CUTOVER_TRIGGERED_ROLLBACK=false" in cutover_payload
    assert "ROLLBACK_TRIGGERED=false" in rollback_payload
    assert "ROLLBACK_TOTAL_STEPS=0" in rollback_payload


def test_ctp_cutover_orchestrator_execute_failure_triggers_rollback(tmp_path: Path) -> None:
    cutover_template = tmp_path / "cutover.env"
    rollback_template = tmp_path / "rollback.env"
    cutover_evidence = tmp_path / "cutover_result.env"
    rollback_evidence = tmp_path / "rollback_result.env"
    _write_cutover_template(cutover_template, precheck_cmd="false")
    _write_rollback_template(rollback_template)

    command = [
        sys.executable,
        "scripts/ops/ctp_cutover_orchestrator.py",
        "--cutover-template",
        str(cutover_template),
        "--rollback-template",
        str(rollback_template),
        "--cutover-output",
        str(cutover_evidence),
        "--rollback-output",
        str(rollback_evidence),
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0

    cutover_payload = cutover_evidence.read_text(encoding="utf-8")
    rollback_payload = rollback_evidence.read_text(encoding="utf-8")
    assert "CUTOVER_DRY_RUN=0" in cutover_payload
    assert "CUTOVER_SUCCESS=false" in cutover_payload
    assert "CUTOVER_FAILED_STEP=precheck" in cutover_payload
    assert "CUTOVER_TRIGGERED_ROLLBACK=true" in cutover_payload
    assert "ROLLBACK_TRIGGERED=true" in rollback_payload
    assert "ROLLBACK_SUCCESS=true" in rollback_payload
