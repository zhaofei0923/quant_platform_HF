from __future__ import annotations

import subprocess
from pathlib import Path


def test_init_kafka_topics_dry_run_writes_evidence(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  kafka:\n" "    image: redpandadata/redpanda:v24.2.7\n",
        encoding="utf-8",
    )
    evidence_file = tmp_path / "kafka_topic_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_kafka_topics.sh",
        "--compose-file",
        str(compose_file),
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "KAFKA_TOPIC_INIT_DRY_RUN=1" in payload
    assert "KAFKA_TOPIC_INIT_SUCCESS=true" in payload
    assert "KAFKA_TOPIC_INIT_TOPIC_COUNT=" in payload
    assert "STEP_1_NAME=create_market.ticks.v1" in payload
    assert "STEP_2_NAME=verify_market.ticks.v1" in payload


def test_init_kafka_topics_defaults_to_single_host_profile(tmp_path: Path) -> None:
    evidence_file = tmp_path / "kafka_topic_init_result.env"
    command = [
        "bash",
        "scripts/infra/init_kafka_topics.sh",
        "--output-file",
        str(evidence_file),
        "--dry-run",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = evidence_file.read_text(encoding="utf-8")
    assert "KAFKA_TOPIC_INIT_COMPOSE_FILE=infra/docker-compose.single-host.yaml" in payload
    assert "KAFKA_TOPIC_INIT_PROJECT_NAME=quant-hft-single-host" in payload


def test_init_kafka_topics_execute_records_failure(tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n" "  kafka:\n" "    image: redpandadata/redpanda:v24.2.7\n",
        encoding="utf-8",
    )
    evidence_file = tmp_path / "kafka_topic_init_result.env"

    command = [
        "bash",
        "scripts/infra/init_kafka_topics.sh",
        "--compose-file",
        str(compose_file),
        "--output-file",
        str(evidence_file),
        "--docker-bin",
        "/bin/false",
        "--execute",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    payload = evidence_file.read_text(encoding="utf-8")
    assert "KAFKA_TOPIC_INIT_SUCCESS=false" in payload
    assert "KAFKA_TOPIC_INIT_FAILED_STEP=create_market.ticks.v1" in payload
