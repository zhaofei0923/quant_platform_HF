from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_summary_json(
    path: Path,
    *,
    release_version: str = "v1.2.3",
    git_commit: str = "deadbeefcafe",
    component_count: int = 2,
    components: list[str] | None = None,
) -> None:
    if components is None:
        components = ["core_engine(service template)", "data_pipeline(non-hotpath)"]
    payload = {
        "bundle_name": f"quant-hft-nonhotpath-{release_version}.tar.gz",
        "bundle_size_bytes": 12345,
        "bundle_root_directory": f"quant-hft-nonhotpath-{release_version}",
        "release_version": release_version,
        "build_ts_utc": "2026-02-11T00:00:00Z",
        "git_commit": git_commit,
        "manifest_bundle_name": f"quant-hft-nonhotpath-{release_version}",
        "sha256": "a" * 64,
        "component_count": component_count,
        "components": components,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_verify_release_audit_summary_passes_for_valid_json(tmp_path: Path) -> None:
    summary_json = tmp_path / "release_audit_summary.json"
    _write_summary_json(summary_json)
    command = [
        sys.executable,
        "scripts/build/verify_release_audit_summary.py",
        "--summary-json",
        str(summary_json),
        "--expect-version",
        "v1.2.3",
        "--expect-git-commit",
        "deadbeefcafe",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_release_audit_summary_fails_when_component_count_mismatch(
    tmp_path: Path,
) -> None:
    summary_json = tmp_path / "release_audit_summary.json"
    _write_summary_json(summary_json, component_count=1)
    command = [
        sys.executable,
        "scripts/build/verify_release_audit_summary.py",
        "--summary-json",
        str(summary_json),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "component_count" in (completed.stdout + completed.stderr)
