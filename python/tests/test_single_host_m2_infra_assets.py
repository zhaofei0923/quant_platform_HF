from __future__ import annotations

from pathlib import Path

REQUIRED_PATHS = (
    Path("infra/docker-compose.single-host.m2.yaml"),
    Path("infra/kafka-connect/Dockerfile"),
    Path("infra/clickhouse/init/001_market_realtime.sql"),
    Path("infra/clickhouse/init/002_trading_core_cdc.sql"),
    Path("infra/debezium/connectors/trading_core.json"),
    Path("configs/data_lifecycle/policies.yaml"),
)


def test_single_host_m2_infra_assets_exist() -> None:
    for path in REQUIRED_PATHS:
        assert path.exists(), f"missing single-host-m2 infra asset: {path}"


def test_single_host_m2_compose_declares_required_services() -> None:
    compose = Path("infra/docker-compose.single-host.m2.yaml").read_text(encoding="utf-8")
    required_services = (
        "redis-primary:",
        "timescale-primary:",
        "kafka:",
        "kafka-connect:",
        "clickhouse:",
        "prometheus:",
        "alertmanager:",
        "loki:",
        "tempo:",
        "grafana:",
        "minio:",
    )
    for service in required_services:
        assert service in compose
