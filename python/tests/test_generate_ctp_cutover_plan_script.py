from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _write_cutover(path: Path) -> None:
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
                "PRECHECK_CMD=echo precheck",
                "WARMUP_QUERY_CMD=echo warmup",
                "POST_SWITCH_MONITOR_MINUTES=30",
                "MONITOR_KEYS=order_latency,breaker_state",
                "CUTOVER_EVIDENCE_OUTPUT=docs/results/ctp_cutover_result.env",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_rollback(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "ROLLBACK_ENV_NAME=prodlike",
                "ROLLBACK_TRIGGER_CONDITION=order_latency_gt_5ms",
                "NEW_CORE_ENGINE_STOP_CMD=echo stop-new-core",
                "NEW_STRATEGY_RUNNER_STOP_CMD=echo stop-new-runner",
                "RESTORE_PREVIOUS_BINARIES_CMD=echo restore-binaries",
                "RESTORE_REDIS_BRIDGE_COMPAT_CMD=echo restore-redis-bridge",
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


def test_generate_ctp_cutover_plan_script_outputs_markdown(tmp_path: Path) -> None:
    cutover = tmp_path / "cutover.env"
    rollback = tmp_path / "rollback.env"
    output_md = tmp_path / "checklist.md"
    output_json = tmp_path / "checklist.json"
    _write_cutover(cutover)
    _write_rollback(rollback)

    command = [
        sys.executable,
        "scripts/ops/generate_ctp_cutover_plan.py",
        "--cutover-template",
        str(cutover),
        "--rollback-template",
        str(rollback),
        "--output-md",
        str(output_md),
        "--output-json",
        str(output_json),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    content = output_md.read_text(encoding="utf-8")
    assert "# CTP One-Shot Cutover Checklist" in content
    assert "## 1) Pre-Cutover (T-30min)" in content
    assert "echo init-kafka" in content
    assert "echo validate-rollback" in content
    assert output_json.exists()


def test_generate_ctp_cutover_plan_script_fails_when_missing_required_key(tmp_path: Path) -> None:
    cutover = tmp_path / "cutover.env"
    rollback = tmp_path / "rollback.env"
    output_md = tmp_path / "checklist.md"
    _write_cutover(cutover)
    _write_rollback(rollback)

    cutover.write_text(
        cutover.read_text(encoding="utf-8").replace("PRECHECK_CMD=echo precheck\n", "")
    )

    command = [
        sys.executable,
        "scripts/ops/generate_ctp_cutover_plan.py",
        "--cutover-template",
        str(cutover),
        "--rollback-template",
        str(rollback),
        "--output-md",
        str(output_md),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "missing required keys" in (completed.stdout + completed.stderr).lower()
