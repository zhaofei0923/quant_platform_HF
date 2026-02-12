from __future__ import annotations

import subprocess
from pathlib import Path


def test_init_timescale_schema_dry_run_writes_evidence(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  timescale-primary:\n" "    image: timescale/timescaledb:2.14.2-pg16\n",
        encoding="utf-8",
    )
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        "CREATE TABLE IF NOT EXISTS market_snapshots(ts_ns BIGINT);\n",
        encoding="utf-8",
    )
    evidence_file = tmp_path / "timescale_schema_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_timescale_schema.sh",
        "--compose-file",
        str(compose_file),
        "--schema-file",
        str(schema_file),
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "TIMESCALE_SCHEMA_INIT_DRY_RUN=1" in payload
    assert "TIMESCALE_SCHEMA_INIT_SUCCESS=true" in payload


def test_init_timescale_schema_dry_run_supports_schema_dir(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  timescale-primary:\n" "    image: timescale/timescaledb:2.14.2-pg16\n",
        encoding="utf-8",
    )
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir(parents=True)
    (schema_dir / "001_a.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS analytics_ts;\n",
        encoding="utf-8",
    )
    (schema_dir / "002_b.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS trading_core;\n",
        encoding="utf-8",
    )
    evidence_file = tmp_path / "timescale_schema_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_timescale_schema.sh",
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
    assert "TIMESCALE_SCHEMA_INIT_SCHEMA_FILE_COUNT=2" in payload
    assert "TIMESCALE_SCHEMA_INIT_SUCCESS=true" in payload


def test_init_timescale_schema_execute_records_failure(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  timescale-primary:\n" "    image: timescale/timescaledb:2.14.2-pg16\n",
        encoding="utf-8",
    )
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        "CREATE TABLE IF NOT EXISTS market_snapshots(ts_ns BIGINT);\n",
        encoding="utf-8",
    )
    evidence_file = tmp_path / "timescale_schema_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_timescale_schema.sh",
        "--compose-file",
        str(compose_file),
        "--schema-file",
        str(schema_file),
        "--output-file",
        str(evidence_file),
        "--docker-bin",
        "/bin/false",
        "--ready-timeout-sec",
        "1",
        "--ready-poll-interval-sec",
        "1",
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = evidence_file.read_text(encoding="utf-8")
    assert "TIMESCALE_SCHEMA_INIT_SUCCESS=false" in payload
    assert "TIMESCALE_SCHEMA_INIT_FAILED_STEP=wait_ready" in payload
