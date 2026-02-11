from __future__ import annotations

from pathlib import Path

from quant_hft.ops.systemd import SystemdRenderConfig, render_systemd_bundle, write_systemd_bundle


def test_render_systemd_bundle_contains_expected_services(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    output_dir = tmp_path / "out"
    config = SystemdRenderConfig(
        repo_root=repo_root,
        output_dir=output_dir,
        service_user="trader",
        service_group="trader",
    )

    bundle = render_systemd_bundle(config)
    assert "quant-hft-core-engine.service" in bundle
    assert "quant-hft-data-pipeline.service" in bundle
    assert "quant-hft-core-engine.env.example" in bundle
    assert "quant-hft-data-pipeline.env.example" in bundle

    core_service = bundle["quant-hft-core-engine.service"]
    assert f"WorkingDirectory={repo_root}" in core_service
    assert (
        f"ExecStart={repo_root / 'build' / 'core_engine'} {repo_root / 'configs/sim/ctp.yaml'}"
        in core_service
    )
    assert f"EnvironmentFile=-{output_dir / 'quant-hft-core-engine.env'}" in core_service
    assert "User=trader" in core_service
    assert "Group=trader" in core_service

    pipeline_service = bundle["quant-hft-data-pipeline.service"]
    pipeline_python = repo_root / ".venv/bin/python"
    pipeline_entry = repo_root / "scripts/data_pipeline/run_pipeline.py"
    assert f"ExecStart={pipeline_python} {pipeline_entry}" in pipeline_service
    assert f"--archive-local-dir {repo_root / 'runtime/archive'}" in pipeline_service


def test_write_systemd_bundle_writes_all_files(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    output_dir = tmp_path / "generated"
    config = SystemdRenderConfig(repo_root=repo_root, output_dir=output_dir)

    written_files = write_systemd_bundle(config)

    assert len(written_files) == 4
    for path in written_files:
        assert path.exists()
        assert path.read_text(encoding="utf-8").strip()
