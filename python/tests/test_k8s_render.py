from __future__ import annotations

from pathlib import Path

from quant_hft.ops.k8s import K8sRenderConfig, render_k8s_bundle, write_k8s_bundle


def test_render_k8s_bundle_contains_expected_manifests(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    output_dir = tmp_path / "out"
    config = K8sRenderConfig(
        repo_root=repo_root,
        output_dir=output_dir,
        namespace="quant-hft-dev",
        image_repository="ghcr.io/acme/quant-hft",
        image_tag="v0.1.0",
    )

    bundle = render_k8s_bundle(config)
    assert "namespace.yaml" in bundle
    assert "configmap-data-pipeline.yaml" in bundle
    assert "deployment-data-pipeline.yaml" in bundle
    assert "persistentvolumeclaim-runtime.yaml" in bundle
    assert "secret-archive.example.yaml" in bundle
    assert "kustomization.yaml" in bundle

    deployment = bundle["deployment-data-pipeline.yaml"]
    assert "name: quant-hft-data-pipeline" in deployment
    assert "namespace: quant-hft-dev" in deployment
    assert "image: ghcr.io/acme/quant-hft:v0.1.0" in deployment
    assert "scripts/data_pipeline/run_pipeline.py" in deployment


def test_write_k8s_bundle_writes_files(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    output_dir = tmp_path / "deploy" / "k8s"
    config = K8sRenderConfig(
        repo_root=repo_root,
        output_dir=output_dir,
        namespace="quant-hft-test",
    )

    written = write_k8s_bundle(config)
    assert len(written) == 6
    for path in written:
        assert path.exists()
        assert path.read_text(encoding="utf-8").strip()
