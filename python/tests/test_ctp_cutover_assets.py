from __future__ import annotations

from pathlib import Path

_REQUIRED_PATHS = (
    Path("docs/CTP_ONE_SHOT_CUTOVER_RUNBOOK.md"),
    Path("configs/ops/ctp_cutover.template.env"),
    Path("configs/ops/ctp_rollback_drill.template.env"),
    Path("scripts/ops/generate_ctp_cutover_plan.py"),
    Path("scripts/ops/ctp_cutover_orchestrator.py"),
    Path("scripts/ops/verify_ctp_cutover_evidence.py"),
)


_CUTOVER_KEYS = (
    "CUTOVER_ENV_NAME=",
    "CUTOVER_WINDOW_LOCAL=",
    "CTP_CONFIG_PATH=",
    "BOOTSTRAP_INFRA_CMD=",
    "INIT_KAFKA_TOPIC_CMD=",
    "INIT_CLICKHOUSE_SCHEMA_CMD=",
    "INIT_DEBEZIUM_CONNECTOR_CMD=",
    "NEW_CORE_ENGINE_START_CMD=",
    "NEW_STRATEGY_RUNNER_START_CMD=",
    "POST_SWITCH_MONITOR_MINUTES=",
    "CUTOVER_EVIDENCE_OUTPUT=",
)


_ROLLBACK_KEYS = (
    "ROLLBACK_ENV_NAME=",
    "ROLLBACK_TRIGGER_CONDITION=",
    "RESTORE_PREVIOUS_BINARIES_CMD=",
    "RESTORE_REDIS_BRIDGE_COMPAT_CMD=",
    "PREVIOUS_CORE_ENGINE_START_CMD=",
    "PREVIOUS_STRATEGY_RUNNER_START_CMD=",
    "MAX_ROLLBACK_SECONDS=",
    "ROLLBACK_EVIDENCE_OUTPUT=",
)


def test_ctp_cutover_assets_exist() -> None:
    for path in _REQUIRED_PATHS:
        assert path.exists(), f"missing asset: {path}"


def test_cutover_template_contains_required_keys() -> None:
    content = Path("configs/ops/ctp_cutover.template.env").read_text(encoding="utf-8")
    for key in _CUTOVER_KEYS:
        assert key in content


def test_rollback_template_contains_required_keys() -> None:
    content = Path("configs/ops/ctp_rollback_drill.template.env").read_text(encoding="utf-8")
    for key in _ROLLBACK_KEYS:
        assert key in content


def test_cutover_templates_default_to_single_host_ubuntu() -> None:
    cutover_content = Path("configs/ops/ctp_cutover.template.env").read_text(encoding="utf-8")
    rollback_content = Path("configs/ops/ctp_rollback_drill.template.env").read_text(
        encoding="utf-8"
    )
    assert "CUTOVER_ENV_NAME=single-host-ubuntu" in cutover_content
    assert "--profile single-host" in cutover_content
    assert "infra/docker-compose.single-host.yaml" in cutover_content
    assert "quant-hft-single-host" in cutover_content
    assert "single_host_bootstrap_result.env" in cutover_content
    assert "ROLLBACK_ENV_NAME=single-host-ubuntu" in rollback_content


def test_cutover_runbook_references_generation_script_and_templates() -> None:
    content = Path("docs/CTP_ONE_SHOT_CUTOVER_RUNBOOK.md").read_text(encoding="utf-8")
    assert "scripts/ops/generate_ctp_cutover_plan.py" in content
    assert "scripts/ops/ctp_cutover_orchestrator.py" in content
    assert "scripts/ops/verify_ctp_cutover_evidence.py" in content
    assert "configs/ops/ctp_cutover.template.env" in content
    assert "configs/ops/ctp_rollback_drill.template.env" in content
    assert "scripts/infra/init_debezium_connectors.sh" in content
    assert "infra/docker-compose.single-host.yaml" in content
    assert "docs/results/single_host_bootstrap_result.env" in content
