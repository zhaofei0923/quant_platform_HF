from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_summary_json(path: Path) -> None:
    payload = {
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


def test_release_audit_index_generates_single_jsonl_line(tmp_path: Path) -> None:
    summary_json = tmp_path / "release_audit_summary.json"
    output_jsonl = tmp_path / "release_audit_index.jsonl"
    _write_summary_json(summary_json)

    command = [
        sys.executable,
        "scripts/build/release_audit_index.py",
        "--summary-json",
        str(summary_json),
        "--output-jsonl",
        str(output_jsonl),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr + completed.stdout

    lines = output_jsonl.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["release_version"] == "v1.2.3"
    assert payload["git_commit"] == "deadbeefcafe"
    assert payload["component_count"] == 2
    assert payload["sha256"] == "a" * 64


def test_release_audit_index_fails_when_summary_missing_field(tmp_path: Path) -> None:
    summary_json = tmp_path / "release_audit_summary.json"
    output_jsonl = tmp_path / "release_audit_index.jsonl"
    summary_json.write_text(
        json.dumps({"release_version": "v1.2.3", "git_commit": "deadbeefcafe"}),
        encoding="utf-8",
    )

    command = [
        sys.executable,
        "scripts/build/release_audit_index.py",
        "--summary-json",
        str(summary_json),
        "--output-jsonl",
        str(output_jsonl),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "missing required key" in (completed.stdout + completed.stderr).lower()
