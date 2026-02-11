from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath


@dataclass(frozen=True)
class K8sRenderConfig:
    repo_root: Path
    output_dir: Path
    namespace: str = "quant-hft"
    image_repository: str = "quant-hft"
    image_tag: str = "latest"
    replicas: int = 1
    runtime_pvc_size: str = "20Gi"
    container_workdir: str = "/app"
    python_bin: str = ".venv/bin/python"
    pipeline_entrypoint: str = "scripts/data_pipeline/run_pipeline.py"
    analytics_db: str = "runtime/analytics.duckdb"
    export_dir: str = "runtime/exports"
    archive_local_dir: str = "runtime/archive"
    archive_endpoint: str = "minio:9000"
    archive_bucket: str = "quant-archive"
    archive_prefix: str = "etl"
    interval_seconds: float = 60.0


def _container_path(workdir: str, value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute():
        return str(path)
    return str(PurePosixPath(workdir) / path)


def render_k8s_bundle(config: K8sRenderConfig) -> dict[str, str]:
    if not config.namespace.strip():
        raise ValueError("namespace is required")
    if config.replicas <= 0:
        raise ValueError("replicas must be > 0")
    if config.interval_seconds <= 0:
        raise ValueError("interval_seconds must be > 0")
    if not config.image_repository.strip():
        raise ValueError("image_repository is required")
    if not config.image_tag.strip():
        raise ValueError("image_tag is required")

    namespace = config.namespace.strip()
    image = f"{config.image_repository}:{config.image_tag}"
    python_bin = _container_path(config.container_workdir, config.python_bin)
    pipeline_entry = _container_path(config.container_workdir, config.pipeline_entrypoint)
    analytics_db = _container_path(config.container_workdir, config.analytics_db)
    export_dir = _container_path(config.container_workdir, config.export_dir)
    archive_local_dir = _container_path(config.container_workdir, config.archive_local_dir)

    namespace_yaml = "apiVersion: v1\n" "kind: Namespace\n" "metadata:\n" f"  name: {namespace}\n"

    configmap_yaml = (
        "apiVersion: v1\n"
        "kind: ConfigMap\n"
        "metadata:\n"
        "  name: quant-hft-data-pipeline-config\n"
        f"  namespace: {namespace}\n"
        "data:\n"
        f'  QUANT_HFT_ARCHIVE_ENDPOINT: "{config.archive_endpoint}"\n'
        f'  QUANT_HFT_ARCHIVE_BUCKET: "{config.archive_bucket}"\n'
        f'  QUANT_HFT_ARCHIVE_PREFIX: "{config.archive_prefix}"\n'
        f'  QUANT_HFT_ARCHIVE_LOCAL_DIR: "{archive_local_dir}"\n'
    )

    secret_example_yaml = (
        "apiVersion: v1\n"
        "kind: Secret\n"
        "metadata:\n"
        "  name: quant-hft-archive-secret\n"
        f"  namespace: {namespace}\n"
        "type: Opaque\n"
        "stringData:\n"
        '  QUANT_HFT_ARCHIVE_ACCESS_KEY: "replace_with_access_key"\n'
        '  QUANT_HFT_ARCHIVE_SECRET_KEY: "replace_with_secret_key"\n'
    )

    pvc_yaml = (
        "apiVersion: v1\n"
        "kind: PersistentVolumeClaim\n"
        "metadata:\n"
        "  name: quant-hft-runtime\n"
        f"  namespace: {namespace}\n"
        "spec:\n"
        "  accessModes:\n"
        "    - ReadWriteOnce\n"
        "  resources:\n"
        "    requests:\n"
        f"      storage: {config.runtime_pvc_size}\n"
    )

    deployment_yaml = (
        "apiVersion: apps/v1\n"
        "kind: Deployment\n"
        "metadata:\n"
        "  name: quant-hft-data-pipeline\n"
        f"  namespace: {namespace}\n"
        "  labels:\n"
        "    app.kubernetes.io/name: quant-hft-data-pipeline\n"
        "spec:\n"
        f"  replicas: {config.replicas}\n"
        "  selector:\n"
        "    matchLabels:\n"
        "      app.kubernetes.io/name: quant-hft-data-pipeline\n"
        "  template:\n"
        "    metadata:\n"
        "      labels:\n"
        "        app.kubernetes.io/name: quant-hft-data-pipeline\n"
        "    spec:\n"
        "      containers:\n"
        "        - name: data-pipeline\n"
        f"          image: {image}\n"
        "          imagePullPolicy: IfNotPresent\n"
        f"          workingDir: {config.container_workdir}\n"
        "          command:\n"
        f"            - {python_bin}\n"
        f"            - {pipeline_entry}\n"
        "            - --analytics-db\n"
        f"            - {analytics_db}\n"
        "            - --export-dir\n"
        f"            - {export_dir}\n"
        "            - --archive-local-dir\n"
        f"            - {archive_local_dir}\n"
        "            - --iterations\n"
        '            - "0"\n'
        "            - --interval-seconds\n"
        f'            - "{config.interval_seconds:g}"\n'
        "          envFrom:\n"
        "            - configMapRef:\n"
        "                name: quant-hft-data-pipeline-config\n"
        "            - secretRef:\n"
        "                name: quant-hft-archive-secret\n"
        "          volumeMounts:\n"
        "            - name: runtime-volume\n"
        f"              mountPath: {_container_path(config.container_workdir, 'runtime')}\n"
        "      volumes:\n"
        "        - name: runtime-volume\n"
        "          persistentVolumeClaim:\n"
        "            claimName: quant-hft-runtime\n"
    )

    kustomization_yaml = (
        "apiVersion: kustomize.config.k8s.io/v1beta1\n"
        "kind: Kustomization\n"
        f"namespace: {namespace}\n"
        "resources:\n"
        "  - namespace.yaml\n"
        "  - configmap-data-pipeline.yaml\n"
        "  - secret-archive.example.yaml\n"
        "  - persistentvolumeclaim-runtime.yaml\n"
        "  - deployment-data-pipeline.yaml\n"
    )

    return {
        "namespace.yaml": namespace_yaml,
        "configmap-data-pipeline.yaml": configmap_yaml,
        "secret-archive.example.yaml": secret_example_yaml,
        "persistentvolumeclaim-runtime.yaml": pvc_yaml,
        "deployment-data-pipeline.yaml": deployment_yaml,
        "kustomization.yaml": kustomization_yaml,
    }


def write_k8s_bundle(config: K8sRenderConfig) -> tuple[Path, ...]:
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for name, content in render_k8s_bundle(config).items():
        path = output_dir / name
        path.write_text(content, encoding="utf-8")
        written.append(path)
    return tuple(written)
