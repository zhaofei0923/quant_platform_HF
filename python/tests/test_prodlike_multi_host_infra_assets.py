from __future__ import annotations

from pathlib import Path

REQUIRED_PATHS = (
    Path("infra/docker-compose.prodlike.multi-host.yaml"),
    Path("infra/timescale/init/001_quant_hft_schema.sql"),
    Path("infra/env/prodlike-primary.env"),
    Path("infra/env/prodlike-standby.env"),
)


def test_prodlike_multi_host_assets_exist() -> None:
    for path in REQUIRED_PATHS:
        assert path.exists(), f"missing multi-host infra asset: {path}"


def test_prodlike_multi_host_compose_declares_required_services() -> None:
    compose = Path("infra/docker-compose.prodlike.multi-host.yaml").read_text(encoding="utf-8")
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
    schema_mount = "/docker-entrypoint-initdb.d/001_quant_hft_schema.sql"
    assert schema_mount not in compose


def test_multi_host_envs_contain_role_keys() -> None:
    primary = Path("infra/env/prodlike-primary.env").read_text(encoding="utf-8")
    standby = Path("infra/env/prodlike-standby.env").read_text(encoding="utf-8")
    assert "ROLE=primary" in primary
    assert "ROLE=standby" in standby
