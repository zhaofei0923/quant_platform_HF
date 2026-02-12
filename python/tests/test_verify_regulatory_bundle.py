from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path


def _write_csv(path: Path, headers: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(headers)
        writer.writerows(rows)


def _build_bundle(tmp_path: Path) -> tuple[Path, Path]:
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
    return output_dir / trade_date, archive_dir


def test_verify_regulatory_bundle_passes_for_valid_bundle(tmp_path: Path) -> None:
    bundle_dir, archive_dir = _build_bundle(tmp_path)
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/ops/verify_regulatory_bundle.py",
            "--bundle-dir",
            str(bundle_dir),
            "--verify-archive",
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


def test_verify_regulatory_bundle_detects_hash_mismatch(tmp_path: Path) -> None:
    bundle_dir, archive_dir = _build_bundle(tmp_path)
    with (bundle_dir / "order_events.csv").open("a", encoding="utf-8") as fp:
        fp.write("ord-2,5,200\n")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/ops/verify_regulatory_bundle.py",
            "--bundle-dir",
            str(bundle_dir),
            "--verify-archive",
            "--bucket",
            "quant-archive",
            "--archive-local-dir",
            str(archive_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    assert "mismatch" in (completed.stdout + completed.stderr).lower()
