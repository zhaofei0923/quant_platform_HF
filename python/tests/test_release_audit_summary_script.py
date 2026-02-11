from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path


def _build_bundle(tmp_path: Path, *, version: str = "v1.2.3") -> tuple[Path, Path]:
    bundle_name = f"quant-hft-nonhotpath-{version}"
    payload_root = tmp_path / bundle_name
    payload_root.mkdir(parents=True, exist_ok=True)

    manifest_path = payload_root / "deploy" / "release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "release_version": version,
                "build_ts_utc": "2026-02-11T00:00:00Z",
                "git_commit": "deadbeefcafe",
                "bundle_name": bundle_name,
                "components": [
                    "core_engine(service template)",
                    "data_pipeline(non-hotpath)",
                ],
            }
        ),
        encoding="utf-8",
    )

    bundle_path = tmp_path / f"{bundle_name}.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as archive:
        archive.add(payload_root, arcname=bundle_name)

    digest = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    checksum_path = Path(f"{bundle_path}.sha256")
    checksum_path.write_text(f"{digest}  {bundle_path.name}\n", encoding="utf-8")
    return bundle_path, checksum_path


def test_release_audit_summary_generates_markdown_file(tmp_path: Path) -> None:
    bundle_path, checksum_path = _build_bundle(tmp_path, version="v1.2.3")
    output_path = tmp_path / "release_audit.md"
    command = [
        sys.executable,
        "scripts/build/release_audit_summary.py",
        "--bundle",
        str(bundle_path),
        "--checksum",
        str(checksum_path),
        "--output",
        str(output_path),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr + completed.stdout

    summary = output_path.read_text(encoding="utf-8")
    assert "# Release Audit Summary" in summary
    assert "- Release Version: `v1.2.3`" in summary
    assert "- Git Commit: `deadbeefcafe`" in summary
    assert "- SHA256: `" in summary
    assert "- Component Count: `2`" in summary


def test_release_audit_summary_fails_when_manifest_is_missing(tmp_path: Path) -> None:
    bundle_name = "quant-hft-nonhotpath-v1.2.3"
    payload_root = tmp_path / bundle_name
    payload_root.mkdir(parents=True, exist_ok=True)
    bundle_path = tmp_path / f"{bundle_name}.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as archive:
        archive.add(payload_root, arcname=bundle_name)
    digest = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    checksum_path = Path(f"{bundle_path}.sha256")
    checksum_path.write_text(f"{digest}  {bundle_path.name}\n", encoding="utf-8")

    command = [
        sys.executable,
        "scripts/build/release_audit_summary.py",
        "--bundle",
        str(bundle_path),
        "--checksum",
        str(checksum_path),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "manifest" in (completed.stdout + completed.stderr).lower()
