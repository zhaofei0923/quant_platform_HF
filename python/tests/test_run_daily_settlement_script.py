from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _write_fake_py(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def _run_script(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "scripts/ops/run_daily_settlement.sh", *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_run_daily_settlement_script_default_path_calls_orchestrator_only(tmp_path: Path) -> None:
    log_file = tmp_path / "orchestrator_log.json"
    fake_orch = tmp_path / "fake_orchestrator.py"
    _write_fake_py(
        fake_orch,
        (
            "from __future__ import annotations\n"
            "import json, pathlib, sys\n"
            f"pathlib.Path({str(log_file)!r}).write_text("
            "json.dumps({'argv': sys.argv[1:]}, ensure_ascii=True), encoding='utf-8')\n"
            "raise SystemExit(0)\n"
        ),
    )

    env = os.environ.copy()
    env["PYTHON_BIN"] = sys.executable
    env["DAILY_SETTLEMENT_ORCHESTRATOR"] = str(fake_orch)

    completed = _run_script(["--trading-day", "20260212"], env)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(log_file.read_text(encoding="utf-8"))
    argv = payload["argv"]
    assert "--trading-day" in argv
    assert "20260212" in argv
    assert "--execute" not in argv
    assert "--settlement-price-json" in argv
    assert "--price-cache-db" in argv


def test_run_daily_settlement_script_runs_readiness_gates(tmp_path: Path) -> None:
    orchestrator_log = tmp_path / "orchestrator_log.json"
    precision_log = tmp_path / "precision_log.json"
    benchmark_log = tmp_path / "benchmark_log.json"
    fake_orch = tmp_path / "fake_orchestrator.py"
    fake_precision = tmp_path / "fake_precision.py"
    fake_benchmark = tmp_path / "fake_benchmark.py"
    dataset = tmp_path / "precision_dataset.json"
    dataset.write_text("[]", encoding="utf-8")

    _write_fake_py(
        fake_orch,
        (
            "from __future__ import annotations\n"
            "import json, pathlib, sys\n"
            f"pathlib.Path({str(orchestrator_log)!r}).write_text("
            "json.dumps({'argv': sys.argv[1:]}, ensure_ascii=True), encoding='utf-8')\n"
            "raise SystemExit(0)\n"
        ),
    )
    _write_fake_py(
        fake_precision,
        (
            "from __future__ import annotations\n"
            "import json, pathlib, sys\n"
            f"pathlib.Path({str(precision_log)!r}).write_text("
            "json.dumps({'argv': sys.argv[1:]}, ensure_ascii=True), encoding='utf-8')\n"
            "raise SystemExit(0)\n"
        ),
    )
    _write_fake_py(
        fake_benchmark,
        (
            "from __future__ import annotations\n"
            "import json, pathlib, sys\n"
            f"pathlib.Path({str(benchmark_log)!r}).write_text("
            "json.dumps({'argv': sys.argv[1:]}, ensure_ascii=True), encoding='utf-8')\n"
            "raise SystemExit(0)\n"
        ),
    )

    env = os.environ.copy()
    env["PYTHON_BIN"] = sys.executable
    env["DAILY_SETTLEMENT_ORCHESTRATOR"] = str(fake_orch)
    env["PRECISION_VERIFY_SCRIPT"] = str(fake_precision)
    env["SETTLEMENT_BENCHMARK_SCRIPT"] = str(fake_benchmark)

    completed = _run_script(
        [
            "--trading-day",
            "2026-02-12",
            "--execute",
            "--run-readiness-gates",
            "--precision-dataset-json",
            str(dataset),
            "--precision-result-json",
            str(tmp_path / "precision_result.json"),
            "--benchmark-result-json",
            str(tmp_path / "benchmark_result.json"),
        ],
        env,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr

    orch_payload = json.loads(orchestrator_log.read_text(encoding="utf-8"))
    precision_payload = json.loads(precision_log.read_text(encoding="utf-8"))
    benchmark_payload = json.loads(benchmark_log.read_text(encoding="utf-8"))

    assert "--execute" in orch_payload["argv"]
    assert "--dataset-json" in precision_payload["argv"]
    assert str(dataset) in precision_payload["argv"]
    assert "--positions" in benchmark_payload["argv"]
    assert "--target-p99-ms" in benchmark_payload["argv"]


def test_run_daily_settlement_script_blocks_missing_precision_dataset(tmp_path: Path) -> None:
    log_file = tmp_path / "orchestrator_log.json"
    fake_orch = tmp_path / "fake_orchestrator.py"
    _write_fake_py(
        fake_orch,
        (
            "from __future__ import annotations\n"
            "import pathlib\n"
            f"pathlib.Path({str(log_file)!r}).write_text('called', encoding='utf-8')\n"
            "raise SystemExit(0)\n"
        ),
    )

    env = os.environ.copy()
    env["PYTHON_BIN"] = sys.executable
    env["DAILY_SETTLEMENT_ORCHESTRATOR"] = str(fake_orch)

    completed = _run_script(
        ["--trading-day", "2026-02-12", "--execute", "--run-readiness-gates"],
        env,
    )
    assert completed.returncode == 2
    assert not log_file.exists()
