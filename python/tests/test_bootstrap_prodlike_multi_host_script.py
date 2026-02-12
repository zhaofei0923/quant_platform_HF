from __future__ import annotations

import subprocess
from pathlib import Path


def test_bootstrap_prodlike_multi_host_dry_run_writes_evidence(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  redis-primary:\n" "    image: redis:7\n",
        encoding="utf-8",
    )
    env_primary = tmp_path / "primary.env"
    env_primary.write_text("ROLE=primary\n", encoding="utf-8")
    env_standby = tmp_path / "standby.env"
    env_standby.write_text("ROLE=standby\n", encoding="utf-8")
    evidence_file = tmp_path / "prodlike_multi_host_bootstrap_result.env"

    command = [
        "bash",
        "scripts/infra/bootstrap_prodlike_multi_host.sh",
        "--compose-file",
        str(compose_file),
        "--primary-env-file",
        str(env_primary),
        "--standby-env-file",
        str(env_standby),
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "MULTI_HOST_ACTION=up" in payload
    assert "MULTI_HOST_DRY_RUN=1" in payload
    assert "MULTI_HOST_SUCCESS=true" in payload
    assert "MULTI_HOST_KAFKA_TOPIC_EVIDENCE=" in payload
    assert "MULTI_HOST_DEBEZIUM_EVIDENCE=" in payload
    assert "MULTI_HOST_CLICKHOUSE_SCHEMA_EVIDENCE=" in payload
    assert "STEP_3_NAME=kafka_topic_init" in payload
    assert "STEP_4_NAME=debezium_connector_init" in payload
    assert "STEP_5_NAME=clickhouse_schema_init" in payload
    assert "STEP_6_NAME=health_check" in payload


def test_bootstrap_prodlike_multi_host_execute_records_failure(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  redis-primary:\n" "    image: redis:7\n",
        encoding="utf-8",
    )
    env_primary = tmp_path / "primary.env"
    env_primary.write_text("ROLE=primary\n", encoding="utf-8")
    env_standby = tmp_path / "standby.env"
    env_standby.write_text("ROLE=standby\n", encoding="utf-8")
    evidence_file = tmp_path / "prodlike_multi_host_bootstrap_result.env"

    command = [
        "bash",
        "scripts/infra/bootstrap_prodlike_multi_host.sh",
        "--action",
        "status",
        "--compose-file",
        str(compose_file),
        "--primary-env-file",
        str(env_primary),
        "--standby-env-file",
        str(env_standby),
        "--output-file",
        str(evidence_file),
        "--docker-bin",
        "/bin/false",
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = evidence_file.read_text(encoding="utf-8")
    assert "MULTI_HOST_SUCCESS=false" in payload
    assert "MULTI_HOST_FAILED_STEP=compose_status" in payload
