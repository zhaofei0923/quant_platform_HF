from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


def _write_csv(path: Path, headers: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(headers)
        writer.writerows(rows)


def test_export_daily_regulatory_bundle_from_source_dir(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    _write_csv(
        source_dir / "order_events.csv",
        ["client_order_id", "status", "ts_ns"],
        [["ord-1", "2", 100]],
    )
    _write_csv(
        source_dir / "trade_events.csv",
        ["client_order_id", "trade_id", "ts_ns"],
        [["ord-1", "trade-1", 101]],
    )
    _write_csv(
        source_dir / "account_snapshots.csv",
        ["account_id", "balance", "ts_ns"],
        [["acc-1", 1000000, 102]],
    )
    _write_csv(
        source_dir / "position_snapshots.csv",
        ["account_id", "instrument_id", "position", "ts_ns"],
        [["acc-1", "SHFE.ag2406", 2, 103]],
    )

    output_dir = tmp_path / "output"
    archive_dir = tmp_path / "archive"
    trade_date = "20260212"
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/ops/export_daily_regulatory_bundle.py",
            "--trade-date",
            trade_date,
            "--source-dir",
            str(source_dir),
            "--output-dir",
            str(output_dir),
            "--bucket",
            "quant-archive",
            "--archive-local-dir",
            str(archive_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(completed.stdout.strip())
    assert payload["trade_date"] == trade_date
    assert payload["files"] == 4
    assert payload["archived_files"] == 5
    assert payload["archive_uri_prefix"] == "minio://quant-archive/compliance/20260212"

    bundle_dir = output_dir / trade_date
    manifest = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["trade_date"] == trade_date
    assert len(manifest["files"]) == 4
    assert all(len(item["sha256"]) == 64 for item in manifest["files"])

    archive_root = archive_dir / "quant-archive" / "compliance" / trade_date
    assert (archive_root / "order_events.csv").exists()
    assert (archive_root / "trade_events.csv").exists()
    assert (archive_root / "account_snapshots.csv").exists()
    assert (archive_root / "position_snapshots.csv").exists()
    assert (archive_root / "manifest.json").exists()
