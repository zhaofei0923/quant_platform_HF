from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


def _load_script_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class _FakeProcess:
    def __init__(self) -> None:
        self.terminated = False
        self.killed = False
        self.waited = False
        self._poll_code: int | None = None

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout: float | None = None) -> int:
        _ = timeout
        self.waited = True
        return 0

    def poll(self) -> int | None:
        return self._poll_code

    def kill(self) -> None:
        self.killed = True


def test_run_reconnect_evidence_exits_130_on_fault_inject_sigint(
    tmp_path: Path, monkeypatch
) -> None:
    script_path = Path("scripts/ops/run_reconnect_evidence.py")
    module = _load_script_module(script_path, "run_reconnect_evidence_script")

    probe_bin = tmp_path / "probe_bin"
    probe_bin.write_text("noop", encoding="utf-8")
    config_path = tmp_path / "ctp.yaml"
    config_path.write_text("ctp:\n  environment: sim\n", encoding="utf-8")
    fault_script = tmp_path / "fault.py"
    fault_script.write_text("print('noop')\n", encoding="utf-8")
    report_script = tmp_path / "report.py"
    report_script.write_text("print('noop')\n", encoding="utf-8")

    fake_proc = _FakeProcess()
    fake_proc._poll_code = None

    def _fake_run_and_capture(*args, **kwargs):
        _ = args, kwargs
        return fake_proc

    def _fake_run(command: list[str], *, dry_run: bool) -> None:
        _ = dry_run
        if any(str(fault_script) == part for part in command):
            raise subprocess.CalledProcessError(returncode=130, cmd=command)

    monkeypatch.setattr(module, "_run_and_capture", _fake_run_and_capture)
    monkeypatch.setattr(module, "_run", _fake_run)
    monkeypatch.setattr(module.time, "sleep", lambda *_args, **_kwargs: None)

    argv = [
        str(script_path),
        "--probe-bin",
        str(probe_bin),
        "--config",
        str(config_path),
        "--probe-log",
        str(tmp_path / "probe.log"),
        "--event-log",
        str(tmp_path / "events.jsonl"),
        "--report-file",
        str(tmp_path / "report.md"),
        "--fault-script",
        str(fault_script),
        "--report-script",
        str(report_script),
        "--skip-preflight",
        "--scenarios",
        "combined",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    # Expect the wrapper to treat 130 as a user abort and exit cleanly.
    exit_code = module.main()
    assert exit_code == 130
    assert fake_proc.terminated is True


def test_run_reconnect_evidence_fails_when_probe_exits_early(tmp_path: Path, monkeypatch) -> None:
    script_path = Path("scripts/ops/run_reconnect_evidence.py")
    module = _load_script_module(script_path, "run_reconnect_evidence_script_probe_exit")

    probe_bin = tmp_path / "probe_bin"
    probe_bin.write_text("noop", encoding="utf-8")
    config_path = tmp_path / "ctp.yaml"
    config_path.write_text("ctp:\n  environment: sim\n", encoding="utf-8")
    fault_script = tmp_path / "fault.py"
    fault_script.write_text("print('noop')\n", encoding="utf-8")
    report_script = tmp_path / "report.py"
    report_script.write_text("print('noop')\n", encoding="utf-8")

    fake_proc = _FakeProcess()
    fake_proc._poll_code = 3

    def _fake_run_and_capture(*args, **kwargs):
        _ = args, kwargs
        return fake_proc

    invoked: list[list[str]] = []

    def _fake_run(command: list[str], *, dry_run: bool) -> None:
        _ = dry_run
        invoked.append(command)

    monkeypatch.setattr(module, "_run_and_capture", _fake_run_and_capture)
    monkeypatch.setattr(module, "_run", _fake_run)
    monkeypatch.setattr(module.time, "sleep", lambda *_args, **_kwargs: None)

    argv = [
        str(script_path),
        "--probe-bin",
        str(probe_bin),
        "--config",
        str(config_path),
        "--probe-log",
        str(tmp_path / "probe.log"),
        "--event-log",
        str(tmp_path / "events.jsonl"),
        "--report-file",
        str(tmp_path / "report.md"),
        "--fault-script",
        str(fault_script),
        "--report-script",
        str(report_script),
        "--skip-preflight",
        "--scenarios",
        "disconnect",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    exit_code = module.main()
    assert exit_code == 2
    assert fake_proc.terminated is True
    assert invoked == []


def test_run_reconnect_evidence_passes_chain_status_to_report(tmp_path: Path, monkeypatch) -> None:
    script_path = Path("scripts/ops/run_reconnect_evidence.py")
    module = _load_script_module(script_path, "run_reconnect_evidence_script_chain_status")

    probe_bin = tmp_path / "probe_bin"
    probe_bin.write_text("noop", encoding="utf-8")
    config_path = tmp_path / "ctp.yaml"
    config_path.write_text("ctp:\n  environment: sim\n", encoding="utf-8")
    fault_script = tmp_path / "fault.py"
    fault_script.write_text("print('noop')\n", encoding="utf-8")
    report_script = tmp_path / "report.py"
    report_script.write_text("print('noop')\n", encoding="utf-8")

    invoked: list[list[str]] = []

    def _fake_run(command: list[str], *, dry_run: bool) -> None:
        _ = dry_run
        invoked.append(command)

    monkeypatch.setattr(module, "_run", _fake_run)

    argv = [
        str(script_path),
        "--probe-bin",
        str(probe_bin),
        "--config",
        str(config_path),
        "--probe-log",
        str(tmp_path / "probe.log"),
        "--event-log",
        str(tmp_path / "events.jsonl"),
        "--report-file",
        str(tmp_path / "report.md"),
        "--fault-script",
        str(fault_script),
        "--report-script",
        str(report_script),
        "--skip-preflight",
        "--scenarios",
        "disconnect",
        "--dry-run",
        "--strategy-bridge-chain-status",
        "complete",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    exit_code = module.main()
    assert exit_code == 0
    report_invocations = [cmd for cmd in invoked if str(report_script) in cmd]
    assert len(report_invocations) == 1
    report_cmd = report_invocations[0]
    assert "--strategy-bridge-chain-status" in report_cmd
    idx = report_cmd.index("--strategy-bridge-chain-status")
    assert report_cmd[idx + 1] == "complete"
