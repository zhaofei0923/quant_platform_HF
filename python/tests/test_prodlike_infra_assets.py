from __future__ import annotations

from pathlib import Path

REQUIRED_PATHS = (
    Path("infra/docker-compose.prodlike.yaml"),
    Path("infra/prometheus/prometheus.yml"),
    Path("infra/alertmanager/alertmanager.yml"),
    Path("infra/loki/loki-config.yml"),
    Path("infra/tempo/tempo.yml"),
    Path("infra/grafana/provisioning/datasources/datasources.yml"),
    Path("infra/env/prodlike.env"),
)


def test_prodlike_infra_assets_exist() -> None:
    for path in REQUIRED_PATHS:
        assert path.exists(), f"missing infra asset: {path}"


def test_prodlike_compose_declares_required_services() -> None:
    compose = Path("infra/docker-compose.prodlike.yaml").read_text(encoding="utf-8")
    required_services = (
        "redis-primary:",
        "redis-replica:",
        "redis-sentinel:",
        "timescale-primary:",
        "timescale-replica:",
        "prometheus:",
        "alertmanager:",
        "loki:",
        "tempo:",
        "grafana:",
        "minio:",
    )
    for service in required_services:
        assert service in compose
