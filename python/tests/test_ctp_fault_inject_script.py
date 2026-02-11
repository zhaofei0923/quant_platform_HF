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


def test_ctp_fault_inject_run_clears_on_keyboard_interrupt(tmp_path: Path, monkeypatch) -> None:
    script_path = Path("scripts/ops/ctp_fault_inject.py")
    module = _load_script_module(script_path, "ctp_fault_inject_script")

    executed: list[str] = []

    def _fake_run(cmd: str, *, shell: bool, check: bool) -> subprocess.CompletedProcess[str]:
        assert shell is True
        assert check is True
        executed.append(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    def _fake_sleep(seconds: float) -> None:
        _ = seconds
        raise KeyboardInterrupt

    def _fake_resolve(candidates, **kwargs) -> str:
        _ = kwargs
        if any("tc" == item or item.endswith("/tc") for item in candidates):
            return "/usr/sbin/tc"
        if any("iptables" in item for item in candidates):
            return "/usr/sbin/iptables"
        return candidates[0]

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    monkeypatch.setattr(module.time, "sleep", _fake_sleep)
    monkeypatch.setattr(module, "resolve_command_binary", _fake_resolve)

    event_log = tmp_path / "fault_events.jsonl"

    argv = [
        str(script_path),
        "run",
        "--scenario",
        "combined",
        "--iface",
        "eth0",
        "--delay-ms",
        "200",
        "--jitter-ms",
        "40",
        "--loss-percent",
        "3",
        "--duration-sec",
        "30",
        "--event-log-file",
        str(event_log),
        "--execute",
    ]

    monkeypatch.setattr(sys, "argv", argv)

    try:
        exit_code = module.main()
    except KeyboardInterrupt:
        exit_code = None

    assert exit_code == 130
    assert any("qdisc replace dev eth0 root netem" in cmd for cmd in executed)
    assert any("qdisc del dev eth0 root" in cmd for cmd in executed)

    lines = event_log.read_text(encoding="utf-8").strip().splitlines()
    assert any('"phase": "apply"' in line for line in lines)
    assert any('"phase": "clear"' in line for line in lines)
