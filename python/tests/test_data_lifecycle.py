from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from quant_hft.data_pipeline.lifecycle_policy import (
    LifecyclePolicy,
    LifecyclePolicyConfig,
    StorageTier,
)


def _set_age_days(path: Path, days: int, *, now_seconds: float) -> None:
    ts = now_seconds - (days * 86_400)
    os.utime(path, (ts, ts))


def test_lifecycle_policy_classifies_hot_warm_cold(tmp_path: Path) -> None:
    now_seconds = 2_000_000_000.0
    source = tmp_path / "source"
    source.mkdir(parents=True)
    hot = source / "hot.log"
    warm = source / "warm.log"
    cold = source / "cold.log"
    hot.write_text("hot\n", encoding="utf-8")
    warm.write_text("warm\n", encoding="utf-8")
    cold.write_text("cold\n", encoding="utf-8")
    _set_age_days(hot, 0, now_seconds=now_seconds)
    _set_age_days(warm, 2, now_seconds=now_seconds)
    _set_age_days(cold, 10, now_seconds=now_seconds)

    policy = LifecyclePolicy(
        source_root=source,
        config=LifecyclePolicyConfig(hot_retention_days=1, warm_retention_days=7),
        now_epoch_seconds_fn=lambda: now_seconds,
    )

    decisions = policy.plan()
    by_name = {item.relative_path.name: item for item in decisions}
    assert by_name["hot.log"].tier is StorageTier.HOT
    assert by_name["warm.log"].tier is StorageTier.WARM
    assert by_name["cold.log"].tier is StorageTier.COLD


def test_run_lifecycle_script_execute_moves_warm_and_cold_files(tmp_path: Path) -> None:
    now_seconds = time.time()
    source = tmp_path / "source"
    source.mkdir(parents=True)
    hot = source / "hot.log"
    warm = source / "warm.log"
    cold = source / "cold.log"
    hot.write_text("hot\n", encoding="utf-8")
    warm.write_text("warm\n", encoding="utf-8")
    cold.write_text("cold\n", encoding="utf-8")
    _set_age_days(hot, 0, now_seconds=now_seconds)
    _set_age_days(warm, 2, now_seconds=now_seconds)
    _set_age_days(cold, 12, now_seconds=now_seconds)

    report_file = tmp_path / "lifecycle.json"
    warm_dir = tmp_path / "warm"
    cold_dir = tmp_path / "cold"

    cmd = [
        sys.executable,
        "scripts/data_pipeline/run_lifecycle.py",
        "--source-dir",
        str(source),
        "--warm-dir",
        str(warm_dir),
        "--cold-dir",
        str(cold_dir),
        "--hot-retention-days",
        "1",
        "--warm-retention-days",
        "7",
        "--report-json",
        str(report_file),
        "--execute",
        "--now-epoch-seconds",
        str(now_seconds),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["scanned_files"] == 3
    assert payload["moved_files"] == 2
    assert payload["replication_status"] == "placeholder"

    assert (source / "hot.log").exists()
    assert not (source / "warm.log").exists()
    assert not (source / "cold.log").exists()
    assert (warm_dir / "warm.log").exists()
    assert (cold_dir / "cold.log").exists()
