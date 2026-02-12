from __future__ import annotations

import subprocess
from pathlib import Path


def test_init_clickhouse_schema_dry_run_writes_evidence(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  clickhouse:\n" "    image: clickhouse/clickhouse-server:24.8\n",
        encoding="utf-8",
    )
    schema_dir = tmp_path / "init"
    schema_dir.mkdir(parents=True, exist_ok=True)
    (schema_dir / "001_schema.sql").write_text(
        "CREATE DATABASE IF NOT EXISTS quant_hft;\n", encoding="utf-8"
    )
    evidence_file = tmp_path / "clickhouse_schema_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_clickhouse_schema.sh",
        "--compose-file",
        str(compose_file),
        "--schema-dir",
        str(schema_dir),
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "CLICKHOUSE_SCHEMA_INIT_DRY_RUN=1" in payload
    assert "CLICKHOUSE_SCHEMA_INIT_SUCCESS=true" in payload
    assert "STEP_1_NAME=verify_clickhouse" in payload
    assert "STEP_2_NAME=apply_001_schema.sql" in payload


def test_init_clickhouse_schema_defaults_to_single_host_profile(tmp_path: Path) -> None:
    evidence_file = tmp_path / "clickhouse_schema_init_result.env"
    command = [
        "bash",
        "scripts/infra/init_clickhouse_schema.sh",
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "CLICKHOUSE_SCHEMA_INIT_COMPOSE_FILE=infra/docker-compose.single-host.yaml" in payload
    assert "CLICKHOUSE_SCHEMA_INIT_PROJECT_NAME=quant-hft-single-host" in payload


def test_init_clickhouse_schema_execute_records_failure(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  clickhouse:\n" "    image: clickhouse/clickhouse-server:24.8\n",
        encoding="utf-8",
    )
    schema_dir = tmp_path / "init"
    schema_dir.mkdir(parents=True, exist_ok=True)
    (schema_dir / "001_schema.sql").write_text(
        "CREATE DATABASE IF NOT EXISTS quant_hft;\n", encoding="utf-8"
    )
    evidence_file = tmp_path / "clickhouse_schema_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_clickhouse_schema.sh",
        "--compose-file",
        str(compose_file),
        "--schema-dir",
        str(schema_dir),
        "--output-file",
        str(evidence_file),
        "--docker-bin",
        "/bin/false",
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = evidence_file.read_text(encoding="utf-8")
    assert "CLICKHOUSE_SCHEMA_INIT_SUCCESS=false" in payload
    assert "CLICKHOUSE_SCHEMA_INIT_FAILED_STEP=verify_clickhouse" in payload
