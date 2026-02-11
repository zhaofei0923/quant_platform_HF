from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SystemdRenderConfig:
    repo_root: Path
    output_dir: Path
    build_dir: Path | str = Path("build")
    python_bin: Path | str = Path(".venv/bin/python")
    core_config: Path | str = Path("configs/sim/ctp.yaml")
    analytics_db: Path | str = Path("runtime/analytics.duckdb")
    export_dir: Path | str = Path("runtime/exports")
    archive_local_dir: Path | str = Path("runtime/archive")
    archive_endpoint: str = "localhost:9000"
    archive_bucket: str = "quant-archive"
    archive_prefix: str = "etl"
    interval_seconds: float = 60.0
    service_user: str | None = None
    service_group: str | None = None


def _resolve_path(repo_root: Path, value: Path | str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (repo_root / path).resolve()


def _identity_lines(config: SystemdRenderConfig) -> list[str]:
    lines: list[str] = []
    if config.service_user:
        lines.append(f"User={config.service_user}")
    if config.service_group:
        lines.append(f"Group={config.service_group}")
    return lines


def render_systemd_bundle(config: SystemdRenderConfig) -> dict[str, str]:
    repo_root = config.repo_root.resolve()
    output_dir = _resolve_path(repo_root, config.output_dir)
    build_dir = _resolve_path(repo_root, config.build_dir)
    python_bin = _resolve_path(repo_root, config.python_bin)
    core_config = _resolve_path(repo_root, config.core_config)
    analytics_db = _resolve_path(repo_root, config.analytics_db)
    export_dir = _resolve_path(repo_root, config.export_dir)
    archive_local_dir = _resolve_path(repo_root, config.archive_local_dir)
    core_binary = build_dir / "core_engine"
    pipeline_script = repo_root / "scripts/data_pipeline/run_pipeline.py"
    core_env = output_dir / "quant-hft-core-engine.env"
    pipeline_env = output_dir / "quant-hft-data-pipeline.env"

    identity_lines = _identity_lines(config)
    identity = "\n".join(identity_lines)
    if identity:
        identity = f"{identity}\n"

    core_service = (
        "[Unit]\n"
        "Description=Quant HFT Core Engine (bootstrap)\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"{identity}"
        f"WorkingDirectory={repo_root}\n"
        f"EnvironmentFile=-{core_env}\n"
        f"ExecStart={core_binary} {core_config}\n"
        "Restart=always\n"
        "RestartSec=2\n"
        "LimitNOFILE=65535\n"
        "NoNewPrivileges=true\n"
        "ProtectSystem=full\n"
        "ProtectHome=true\n"
        "PrivateTmp=true\n\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )

    pipeline_service = (
        "[Unit]\n"
        "Description=Quant HFT Data Pipeline\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"{identity}"
        f"WorkingDirectory={repo_root}\n"
        f"EnvironmentFile=-{pipeline_env}\n"
        f"ExecStart={python_bin} {pipeline_script}"
        f" --analytics-db {analytics_db}"
        f" --export-dir {export_dir}"
        f" --archive-endpoint {config.archive_endpoint}"
        f" --archive-bucket {config.archive_bucket}"
        f" --archive-prefix {config.archive_prefix}"
        f" --archive-local-dir {archive_local_dir}"
        " --iterations 0"
        f" --interval-seconds {config.interval_seconds:g}\n"
        "Restart=always\n"
        "RestartSec=5\n"
        "LimitNOFILE=65535\n"
        "NoNewPrivileges=true\n"
        "ProtectSystem=full\n"
        "ProtectHome=true\n"
        "PrivateTmp=true\n\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )

    core_env_example = (
        "# CTP\n"
        "CTP_SIM_PASSWORD=replace_with_simnow_password\n\n"
        "# Storage modes\n"
        "QUANT_HFT_REDIS_MODE=in_memory\n"
        "QUANT_HFT_TIMESCALE_MODE=in_memory\n"
        "QUANT_HFT_STORAGE_ALLOW_FALLBACK=true\n"
    )
    pipeline_env_example = (
        "# Optional archive overrides (run_pipeline.py supports CLI override first)\n"
        "QUANT_HFT_ARCHIVE_ENDPOINT=localhost:9000\n"
        "QUANT_HFT_ARCHIVE_ACCESS_KEY=minioadmin\n"
        "QUANT_HFT_ARCHIVE_SECRET_KEY=minioadmin\n"
        "QUANT_HFT_ARCHIVE_BUCKET=quant-archive\n"
    )

    return {
        "quant-hft-core-engine.service": core_service,
        "quant-hft-data-pipeline.service": pipeline_service,
        "quant-hft-core-engine.env.example": core_env_example,
        "quant-hft-data-pipeline.env.example": pipeline_env_example,
    }


def write_systemd_bundle(config: SystemdRenderConfig) -> tuple[Path, ...]:
    output_dir = _resolve_path(config.repo_root.resolve(), config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for filename, content in render_systemd_bundle(config).items():
        path = output_dir / filename
        path.write_text(content, encoding="utf-8")
        written.append(path)
    return tuple(written)
