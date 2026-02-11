from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path


def _required_relative_paths() -> list[str]:
    return [
        "deploy/release_manifest.json",
        "docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md",
        "docs/K8S_DEPLOYMENT_RUNBOOK.md",
        "docs/WAL_RECOVERY_RUNBOOK.md",
        "scripts/build/package_nonhotpath_release.sh",
        "scripts/build/release_audit_summary.py",
        "scripts/build/verify_nonhotpath_release.py",
        "scripts/build/verify_release_audit_summary.py",
        "scripts/ops/run_reconnect_evidence.py",
        "scripts/ops/reconnect_slo_report.py",
        "configs/sim/ctp.yaml",
        "python/quant_hft/__init__.py",
    ]


def _build_bundle(
    tmp_path: Path,
    *,
    version: str = "v1.2.3",
    missing_relative_path: str | None = None,
) -> tuple[Path, Path]:
    bundle_name = f"quant-hft-nonhotpath-{version}"
    payload_root = tmp_path / bundle_name
    payload_root.mkdir(parents=True, exist_ok=True)

    for relative in _required_relative_paths():
        if relative == missing_relative_path:
            continue
        target = payload_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if relative.endswith(".json"):
            target.write_text(
                json.dumps(
                    {
                        "release_version": version,
                        "build_ts_utc": "2026-02-11T00:00:00Z",
                        "git_commit": "deadbeefcafe",
                        "bundle_name": bundle_name,
                        "components": ["core_engine(service template)"],
                    }
                ),
                encoding="utf-8",
            )
        else:
            target.write_text("fixture\n", encoding="utf-8")

    bundle_path = tmp_path / f"{bundle_name}.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as archive:
        archive.add(payload_root, arcname=bundle_name)

    digest = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    checksum_path = Path(f"{bundle_path}.sha256")
    checksum_path.write_text(f"{digest}  {bundle_path.name}\n", encoding="utf-8")
    return bundle_path, checksum_path


def test_verify_nonhotpath_release_passes_for_valid_bundle(tmp_path: Path) -> None:
    bundle_path, checksum_path = _build_bundle(tmp_path)
    command = [
        sys.executable,
        "scripts/build/verify_nonhotpath_release.py",
        "--bundle",
        str(bundle_path),
        "--checksum",
        str(checksum_path),
        "--expect-version",
        "v1.2.3",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_nonhotpath_release_fails_on_checksum_mismatch(tmp_path: Path) -> None:
    bundle_path, checksum_path = _build_bundle(tmp_path)
    checksum_path.write_text(f"{'0' * 64}  {bundle_path.name}\n", encoding="utf-8")
    command = [
        sys.executable,
        "scripts/build/verify_nonhotpath_release.py",
        "--bundle",
        str(bundle_path),
        "--checksum",
        str(checksum_path),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "checksum" in (completed.stdout + completed.stderr).lower()


def test_verify_nonhotpath_release_fails_when_required_file_is_missing(
    tmp_path: Path,
) -> None:
    bundle_path, checksum_path = _build_bundle(
        tmp_path, missing_relative_path="docs/K8S_DEPLOYMENT_RUNBOOK.md"
    )
    command = [
        sys.executable,
        "scripts/build/verify_nonhotpath_release.py",
        "--bundle",
        str(bundle_path),
        "--checksum",
        str(checksum_path),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "required file" in (completed.stdout + completed.stderr).lower()
