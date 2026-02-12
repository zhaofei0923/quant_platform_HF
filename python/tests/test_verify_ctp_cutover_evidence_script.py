from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _write_cutover_evidence(
    path: Path,
    *,
    success: str = "true",
    failed_step: str = "",
    triggered_rollback: str = "false",
) -> None:
    path.write_text(
        "\n".join(
            [
                "CUTOVER_ENV=prodlike",
                "CUTOVER_WINDOW_LOCAL=2026-02-13T09:00:00+08:00",
                "CUTOVER_CTP_CONFIG_PATH=configs/prod/ctp.yaml",
                "CUTOVER_DRY_RUN=0",
                f"CUTOVER_SUCCESS={success}",
                "CUTOVER_TOTAL_STEPS=3",
                f"CUTOVER_FAILED_STEP={failed_step}",
                "CUTOVER_MONITOR_MINUTES=30",
                "CUTOVER_MONITOR_KEYS=order_latency_p99_ms,breaker_state",
                f"CUTOVER_TRIGGERED_ROLLBACK={triggered_rollback}",
                "CUTOVER_STARTED_UTC=2026-02-12T04:00:00Z",
                "CUTOVER_COMPLETED_UTC=2026-02-12T04:00:02Z",
                "CUTOVER_DURATION_SECONDS=2.000",
                "STEP_1_NAME=precheck",
                "STEP_1_STATUS=ok",
                "STEP_1_DURATION_MS=10",
                "STEP_1_EXIT_CODE=0",
                "STEP_2_NAME=start_new_core_engine",
                "STEP_2_STATUS=ok",
                "STEP_2_DURATION_MS=10",
                "STEP_2_EXIT_CODE=0",
                "STEP_3_NAME=warmup_query",
                "STEP_3_STATUS=ok",
                "STEP_3_DURATION_MS=10",
                "STEP_3_EXIT_CODE=0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_rollback_evidence(
    path: Path,
    *,
    triggered: str = "false",
    success: str = "true",
    duration_seconds: str = "0.000",
) -> None:
    lines = [
        "ROLLBACK_ENV=prodlike",
        "ROLLBACK_TRIGGER_CONDITION=order_latency_p99_ms_gt_5ms",
        "ROLLBACK_DRY_RUN=0",
        f"ROLLBACK_TRIGGERED={triggered}",
        f"ROLLBACK_SUCCESS={success}",
        "ROLLBACK_TOTAL_STEPS=0",
        "ROLLBACK_FAILED_STEP=",
        "ROLLBACK_MAX_SECONDS=180.000",
        f"ROLLBACK_DURATION_SECONDS={duration_seconds}",
        "ROLLBACK_SLO_MET=true",
        "ROLLBACK_STARTED_UTC=",
        "ROLLBACK_COMPLETED_UTC=",
    ]
    if triggered.lower() == "true":
        lines.extend(
            [
                "ROLLBACK_TOTAL_STEPS=2",
                "STEP_1_NAME=stop_new_core_engine",
                "STEP_1_STATUS=ok",
                "STEP_1_DURATION_MS=10",
                "STEP_1_EXIT_CODE=0",
                "STEP_2_NAME=restore_previous_binaries",
                "STEP_2_STATUS=ok",
                "STEP_2_DURATION_MS=10",
                "STEP_2_EXIT_CODE=0",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_verify_ctp_cutover_evidence_passes_for_successful_cutover(tmp_path: Path) -> None:
    cutover_file = tmp_path / "ctp_cutover_result.env"
    rollback_file = tmp_path / "ctp_rollback_result.env"
    _write_cutover_evidence(cutover_file)
    _write_rollback_evidence(rollback_file)

    command = [
        sys.executable,
        "scripts/ops/verify_ctp_cutover_evidence.py",
        "--cutover-evidence-file",
        str(cutover_file),
        "--rollback-evidence-file",
        str(rollback_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_ctp_cutover_evidence_fails_when_cutover_failed_without_allow(
    tmp_path: Path,
) -> None:
    cutover_file = tmp_path / "ctp_cutover_result.env"
    rollback_file = tmp_path / "ctp_rollback_result.env"
    _write_cutover_evidence(
        cutover_file,
        success="false",
        failed_step="precheck",
        triggered_rollback="true",
    )
    _write_rollback_evidence(
        rollback_file,
        triggered="true",
        success="true",
        duration_seconds="12.000",
    )

    command = [
        sys.executable,
        "scripts/ops/verify_ctp_cutover_evidence.py",
        "--cutover-evidence-file",
        str(cutover_file),
        "--rollback-evidence-file",
        str(rollback_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "cutover_success must be true" in (completed.stdout + completed.stderr).lower()


def test_verify_ctp_cutover_evidence_passes_with_allow_cutover_rollback(tmp_path: Path) -> None:
    cutover_file = tmp_path / "ctp_cutover_result.env"
    rollback_file = tmp_path / "ctp_rollback_result.env"
    _write_cutover_evidence(
        cutover_file,
        success="false",
        failed_step="precheck",
        triggered_rollback="true",
    )
    _write_rollback_evidence(
        rollback_file,
        triggered="true",
        success="true",
        duration_seconds="12.000",
    )

    command = [
        sys.executable,
        "scripts/ops/verify_ctp_cutover_evidence.py",
        "--cutover-evidence-file",
        str(cutover_file),
        "--rollback-evidence-file",
        str(rollback_file),
        "--allow-cutover-rollback",
        "--max-rollback-seconds",
        "180",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_ctp_cutover_evidence_fails_when_rollback_duration_exceeds_threshold(
    tmp_path: Path,
) -> None:
    cutover_file = tmp_path / "ctp_cutover_result.env"
    rollback_file = tmp_path / "ctp_rollback_result.env"
    _write_cutover_evidence(
        cutover_file,
        success="false",
        failed_step="precheck",
        triggered_rollback="true",
    )
    _write_rollback_evidence(
        rollback_file,
        triggered="true",
        success="true",
        duration_seconds="420.000",
    )

    command = [
        sys.executable,
        "scripts/ops/verify_ctp_cutover_evidence.py",
        "--cutover-evidence-file",
        str(cutover_file),
        "--rollback-evidence-file",
        str(rollback_file),
        "--allow-cutover-rollback",
        "--max-rollback-seconds",
        "180",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "rollback_duration_seconds" in (completed.stdout + completed.stderr).lower()
