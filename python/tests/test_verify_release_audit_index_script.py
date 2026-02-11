from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_summary_json(path: Path) -> dict[str, object]:
    payload: dict[str, object] = {
        "bundle_name": "quant-hft-nonhotpath-v1.2.3.tar.gz",
        "bundle_size_bytes": 12345,
        "bundle_root_directory": "quant-hft-nonhotpath-v1.2.3",
        "release_version": "v1.2.3",
        "build_ts_utc": "2026-02-11T00:00:00Z",
        "git_commit": "deadbeefcafe",
        "manifest_bundle_name": "quant-hft-nonhotpath-v1.2.3",
        "sha256": "a" * 64,
        "component_count": 2,
        "components": [
            "core_engine(service template)",
            "data_pipeline(non-hotpath)",
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _write_index_jsonl(path: Path, payload: dict[str, object]) -> None:
    record = {
        "release_version": payload["release_version"],
        "git_commit": payload["git_commit"],
        "sha256": payload["sha256"],
        "component_count": payload["component_count"],
        "bundle_name": payload["bundle_name"],
        "build_ts_utc": payload["build_ts_utc"],
    }
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")


def test_verify_release_audit_index_passes_for_consistent_summary_and_index(
    tmp_path: Path,
) -> None:
    summary_json = tmp_path / "release_audit_summary.json"
    index_jsonl = tmp_path / "release_audit_index.jsonl"
    payload = _write_summary_json(summary_json)
    _write_index_jsonl(index_jsonl, payload)

    command = [
        sys.executable,
        "scripts/build/verify_release_audit_index.py",
        "--summary-json",
        str(summary_json),
        "--index-jsonl",
        str(index_jsonl),
        "--expect-version",
        "v1.2.3",
        "--expect-git-commit",
        "deadbeefcafe",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "verification passed" in (completed.stdout + completed.stderr).lower()


def test_verify_release_audit_index_fails_when_index_differs_from_summary(
    tmp_path: Path,
) -> None:
    summary_json = tmp_path / "release_audit_summary.json"
    index_jsonl = tmp_path / "release_audit_index.jsonl"
    payload = _write_summary_json(summary_json)
    _write_index_jsonl(index_jsonl, payload)

    # Inject mismatch: component_count in index no longer matches summary.
    bad_record = json.loads(index_jsonl.read_text(encoding="utf-8").splitlines()[0])
    bad_record["component_count"] = 3
    index_jsonl.write_text(json.dumps(bad_record) + "\n", encoding="utf-8")

    command = [
        sys.executable,
        "scripts/build/verify_release_audit_index.py",
        "--summary-json",
        str(summary_json),
        "--index-jsonl",
        str(index_jsonl),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "mismatch" in (completed.stdout + completed.stderr).lower()
