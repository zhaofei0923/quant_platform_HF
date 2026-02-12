from __future__ import annotations

from pathlib import Path

REQUIRED_PATHS = (
    Path("infra/docker-compose.single-host.yaml"),
    Path("infra/timescale/init/001_quant_hft_schema.sql"),
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
        "prometheus:",
        "alertmanager:",
        "loki:",
        "tempo:",
        "grafana:",
        "minio:",
    )
    for service in required_services:
        assert service in compose
