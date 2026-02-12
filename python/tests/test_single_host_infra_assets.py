from __future__ import annotations

from pathlib import Path

REQUIRED_PATHS = (
    Path("infra/docker-compose.single-host.yaml"),
    Path("infra/timescale/init/001_quant_hft_schema.sql"),
    Path("infra/timescale/init/004_trading_core_domain_tables.sql"),
    Path("infra/timescale/init/005_ops_audit_tables.sql"),
    Path("infra/timescale/init/006_partition_maintenance.sql"),
    Path("infra/clickhouse/init/001_market_ticks_kafka.sql"),
    Path("infra/clickhouse/init/002_trading_core_cdc.sql"),
    Path("infra/debezium/connector-trading-core.json"),
    Path("infra/debezium/connector-ops-audit.json"),
    Path("scripts/infra/init_debezium_connectors.sh"),
    Path("infra/prometheus/prometheus.yml"),
    Path("infra/alertmanager/alertmanager.yml"),
    Path("infra/loki/loki-config.yml"),
    Path("infra/tempo/tempo.yml"),
    Path("infra/grafana/provisioning/datasources/datasources.yml"),
    Path("infra/env/prodlike.env"),
)


def test_single_host_infra_assets_exist() -> None:
    for path in REQUIRED_PATHS:
        assert path.exists(), f"missing single-host infra asset: {path}"


def test_single_host_compose_declares_required_services() -> None:
    compose = Path("infra/docker-compose.single-host.yaml").read_text(encoding="utf-8")
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
