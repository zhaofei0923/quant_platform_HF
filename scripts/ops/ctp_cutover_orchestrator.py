#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class StepResult:
    name: str
    status: str
    duration_ms: int
    command: str
    exit_code: int


def _parse_env_file(path: Path) -> dict[str, str]:
    payload: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        payload[key.strip()] = value.strip()
    return payload


def _required_keys(payload: dict[str, str], keys: tuple[str, ...], label: str) -> None:
    missing = [key for key in keys if not payload.get(key, "").strip()]
    if missing:
        raise ValueError(f"{label} missing required keys: {','.join(missing)}")


def _run_step(name: str, command: str, *, dry_run: bool) -> StepResult:
    started = time.monotonic_ns()
    status = "simulated_ok"
    exit_code = 0

    if command and not dry_run:
        completed = subprocess.run(command, shell=True, check=False, capture_output=True, text=True)
        exit_code = completed.returncode
        status = "ok" if exit_code == 0 else "failed"

    elapsed_ms = int((time.monotonic_ns() - started) / 1_000_000)
    return StepResult(
        name=name,
        status=status,
        duration_ms=max(0, elapsed_ms),
        command=command,
        exit_code=exit_code,
    )


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_duration_seconds(duration_ms: int) -> str:
    return f"{duration_ms / 1000.0:.3f}"


def _step_lines(results: list[StepResult]) -> list[str]:
    lines: list[str] = []
    for idx, item in enumerate(results, start=1):
        lines.append(f"STEP_{idx}_NAME={item.name}")
        lines.append(f"STEP_{idx}_STATUS={item.status}")
        lines.append(f"STEP_{idx}_DURATION_MS={item.duration_ms}")
        lines.append(f"STEP_{idx}_EXIT_CODE={item.exit_code}")
        lines.append(f"STEP_{idx}_COMMAND={item.command}")
    return lines


def _run_steps(
    definitions: list[tuple[str, str]],
    template: dict[str, str],
    *,
    dry_run: bool,
) -> tuple[list[StepResult], str]:
    results: list[StepResult] = []
    failed_step = ""
    for name, key in definitions:
        result = _run_step(name, template[key], dry_run=dry_run)
        results.append(result)
        if result.status == "failed":
            failed_step = name
            break
    return results, failed_step


def _parse_non_negative_float(raw_value: str, key: str) -> float:
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{key} must be numeric: {raw_value}") from exc
    if value < 0:
        raise ValueError(f"{key} must be >= 0: {raw_value}")
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute one-shot CTP cutover/rollback orchestration and emit " "evidence env files"
        )
    )
    parser.add_argument("--cutover-template", default="configs/ops/ctp_cutover.template.env")
    parser.add_argument(
        "--rollback-template", default="configs/ops/ctp_rollback_drill.template.env"
    )
    parser.add_argument("--cutover-output", default="")
    parser.add_argument("--rollback-output", default="")
    parser.add_argument("--force-rollback", action="store_true")
    parser.add_argument("--execute", action="store_true", help="Run real commands in templates")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    cutover_template_path = Path(args.cutover_template)
    rollback_template_path = Path(args.rollback_template)
    if not cutover_template_path.exists():
        print(f"error: cutover template not found: {cutover_template_path}")
        return 2
    if not rollback_template_path.exists():
        print(f"error: rollback template not found: {rollback_template_path}")
        return 2

    cutover = _parse_env_file(cutover_template_path)
    rollback = _parse_env_file(rollback_template_path)

    cutover_required = (
        "CUTOVER_ENV_NAME",
        "CUTOVER_WINDOW_LOCAL",
        "CTP_CONFIG_PATH",
        "OLD_CORE_ENGINE_STOP_CMD",
        "OLD_STRATEGY_RUNNER_STOP_CMD",
        "BOOTSTRAP_INFRA_CMD",
        "INIT_KAFKA_TOPIC_CMD",
        "INIT_CLICKHOUSE_SCHEMA_CMD",
        "INIT_DEBEZIUM_CONNECTOR_CMD",
        "NEW_CORE_ENGINE_START_CMD",
        "NEW_STRATEGY_RUNNER_START_CMD",
        "PRECHECK_CMD",
        "WARMUP_QUERY_CMD",
        "POST_SWITCH_MONITOR_MINUTES",
        "MONITOR_KEYS",
        "CUTOVER_EVIDENCE_OUTPUT",
    )
    rollback_required = (
        "ROLLBACK_ENV_NAME",
        "ROLLBACK_TRIGGER_CONDITION",
        "NEW_CORE_ENGINE_STOP_CMD",
        "NEW_STRATEGY_RUNNER_STOP_CMD",
        "RESTORE_PREVIOUS_BINARIES_CMD",
        "RESTORE_REDIS_BRIDGE_COMPAT_CMD",
        "PREVIOUS_CORE_ENGINE_START_CMD",
        "PREVIOUS_STRATEGY_RUNNER_START_CMD",
        "POST_ROLLBACK_VALIDATE_CMD",
        "MAX_ROLLBACK_SECONDS",
        "ROLLBACK_EVIDENCE_OUTPUT",
    )

    try:
        _required_keys(cutover, cutover_required, "cutover template")
        _required_keys(rollback, rollback_required, "rollback template")
        rollback_max_seconds = _parse_non_negative_float(
            rollback["MAX_ROLLBACK_SECONDS"], "MAX_ROLLBACK_SECONDS"
        )
    except ValueError as exc:
        print(f"error: {exc}")
        return 2

    dry_run = not args.execute
    cutover_output = Path(args.cutover_output or cutover["CUTOVER_EVIDENCE_OUTPUT"])
    rollback_output = Path(args.rollback_output or rollback["ROLLBACK_EVIDENCE_OUTPUT"])

    cutover_steps = [
        ("stop_old_core_engine", "OLD_CORE_ENGINE_STOP_CMD"),
        ("stop_old_strategy_runner", "OLD_STRATEGY_RUNNER_STOP_CMD"),
        ("precheck", "PRECHECK_CMD"),
        ("bootstrap_infra", "BOOTSTRAP_INFRA_CMD"),
        ("init_kafka_topic", "INIT_KAFKA_TOPIC_CMD"),
        ("init_clickhouse_schema", "INIT_CLICKHOUSE_SCHEMA_CMD"),
        ("init_debezium_connector", "INIT_DEBEZIUM_CONNECTOR_CMD"),
        ("start_new_core_engine", "NEW_CORE_ENGINE_START_CMD"),
        ("start_new_strategy_runner", "NEW_STRATEGY_RUNNER_START_CMD"),
        ("warmup_query", "WARMUP_QUERY_CMD"),
    ]
    rollback_steps = [
        ("stop_new_core_engine", "NEW_CORE_ENGINE_STOP_CMD"),
        ("stop_new_strategy_runner", "NEW_STRATEGY_RUNNER_STOP_CMD"),
        ("restore_previous_binaries", "RESTORE_PREVIOUS_BINARIES_CMD"),
        ("restore_redis_bridge_compat", "RESTORE_REDIS_BRIDGE_COMPAT_CMD"),
        ("start_previous_core_engine", "PREVIOUS_CORE_ENGINE_START_CMD"),
        ("start_previous_strategy_runner", "PREVIOUS_STRATEGY_RUNNER_START_CMD"),
        ("post_rollback_validate", "POST_ROLLBACK_VALIDATE_CMD"),
    ]

    cutover_started_utc = _utc_now()
    cutover_results, cutover_failed_step = _run_steps(cutover_steps, cutover, dry_run=dry_run)
    cutover_duration_ms = sum(item.duration_ms for item in cutover_results)
    cutover_completed_utc = _utc_now()
    cutover_success = cutover_failed_step == ""
    rollback_triggered = args.force_rollback or not cutover_success

    rollback_started_utc = ""
    rollback_completed_utc = ""
    rollback_results: list[StepResult] = []
    rollback_failed_step = ""
    rollback_duration_ms = 0
    rollback_slo_met = True
    rollback_success = True

    if rollback_triggered:
        rollback_started_utc = _utc_now()
        rollback_results, rollback_failed_step = _run_steps(
            rollback_steps, rollback, dry_run=dry_run
        )
        rollback_duration_ms = sum(item.duration_ms for item in rollback_results)
        rollback_completed_utc = _utc_now()

        rollback_success = rollback_failed_step == ""
        rollback_duration_seconds = rollback_duration_ms / 1000.0
        rollback_slo_met = rollback_duration_seconds <= rollback_max_seconds
        if rollback_success and not rollback_slo_met:
            rollback_success = False
            rollback_failed_step = "rollback_duration_exceeded"

    cutover_lines = [
        f"CUTOVER_ENV={cutover['CUTOVER_ENV_NAME']}",
        f"CUTOVER_WINDOW_LOCAL={cutover['CUTOVER_WINDOW_LOCAL']}",
        f"CUTOVER_CTP_CONFIG_PATH={cutover['CTP_CONFIG_PATH']}",
        f"CUTOVER_DRY_RUN={'1' if dry_run else '0'}",
        f"CUTOVER_SUCCESS={_bool_text(cutover_success)}",
        f"CUTOVER_TOTAL_STEPS={len(cutover_results)}",
        f"CUTOVER_FAILED_STEP={cutover_failed_step}",
        f"CUTOVER_MONITOR_MINUTES={cutover['POST_SWITCH_MONITOR_MINUTES']}",
        f"CUTOVER_MONITOR_KEYS={cutover['MONITOR_KEYS']}",
        f"CUTOVER_TRIGGERED_ROLLBACK={_bool_text(rollback_triggered)}",
        f"CUTOVER_STARTED_UTC={cutover_started_utc}",
        f"CUTOVER_COMPLETED_UTC={cutover_completed_utc}",
        f"CUTOVER_DURATION_SECONDS={_format_duration_seconds(cutover_duration_ms)}",
    ]
    cutover_lines.extend(_step_lines(cutover_results))

    rollback_lines = [
        f"ROLLBACK_ENV={rollback['ROLLBACK_ENV_NAME']}",
        f"ROLLBACK_TRIGGER_CONDITION={rollback['ROLLBACK_TRIGGER_CONDITION']}",
        f"ROLLBACK_DRY_RUN={'1' if dry_run else '0'}",
        f"ROLLBACK_TRIGGERED={_bool_text(rollback_triggered)}",
        f"ROLLBACK_SUCCESS={_bool_text(rollback_success)}",
        f"ROLLBACK_TOTAL_STEPS={len(rollback_results)}",
        f"ROLLBACK_FAILED_STEP={rollback_failed_step}",
        f"ROLLBACK_MAX_SECONDS={rollback_max_seconds:.3f}",
        f"ROLLBACK_DURATION_SECONDS={_format_duration_seconds(rollback_duration_ms)}",
        f"ROLLBACK_SLO_MET={_bool_text(rollback_slo_met)}",
        f"ROLLBACK_STARTED_UTC={rollback_started_utc}",
        f"ROLLBACK_COMPLETED_UTC={rollback_completed_utc}",
    ]
    rollback_lines.extend(_step_lines(rollback_results))

    cutover_output.parent.mkdir(parents=True, exist_ok=True)
    rollback_output.parent.mkdir(parents=True, exist_ok=True)
    cutover_output.write_text("\n".join(cutover_lines) + "\n", encoding="utf-8")
    rollback_output.write_text("\n".join(rollback_lines) + "\n", encoding="utf-8")

    print(str(cutover_output))
    print(str(rollback_output))
    return 0 if cutover_success and rollback_success else 2


if __name__ == "__main__":
    raise SystemExit(main())
