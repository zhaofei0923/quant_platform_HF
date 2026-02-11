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


class _FakeRedisClient:
    def __init__(self, storage: dict[str, dict[str, str]]) -> None:
        self._storage = storage

    def ping(self) -> bool:
        return True

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._storage.get(key, {}))


def _state_snapshot_fields(instrument_id: str) -> dict[str, str]:
    return {
        "instrument_id": instrument_id,
        "trend_score": "0.1",
        "trend_confidence": "0.9",
        "volatility_score": "0.2",
        "volatility_confidence": "0.9",
        "liquidity_score": "0.3",
        "liquidity_confidence": "0.9",
        "sentiment_score": "0.4",
        "sentiment_confidence": "0.9",
        "seasonality_score": "0.5",
        "seasonality_confidence": "0.9",
        "pattern_score": "0.6",
        "pattern_confidence": "0.9",
        "event_drive_score": "0.7",
        "event_drive_confidence": "0.9",
        "ts_ns": "123",
    }


def _order_event_fields(client_order_id: str) -> dict[str, str]:
    return {
        "account_id": "sim-account",
        "client_order_id": client_order_id,
        "instrument_id": "SHFE.ag2406",
        "status": "ACCEPTED",
        "total_volume": "1",
        "filled_volume": "0",
        "avg_fill_price": "0",
        "reason": "ok",
        "ts_ns": "456",
        "trace_id": client_order_id,
    }


def _fake_runner_config():
    return type("RunnerConfig", (), {"instruments": ["SHFE.ag2406"], "strategy_id": "demo"})()


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


def test_run_reconnect_evidence_auto_detects_chain_status(tmp_path: Path, monkeypatch) -> None:
    script_path = Path("scripts/ops/run_reconnect_evidence.py")
    module = _load_script_module(script_path, "run_reconnect_evidence_script_auto_chain_status")

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

    detect_calls: list[Path] = []

    def _fake_detect(cfg_path: Path) -> str:
        detect_calls.append(cfg_path)
        return "complete"

    monkeypatch.setattr(module, "_run", _fake_run)
    monkeypatch.setattr(module, "_detect_strategy_bridge_chain_status", _fake_detect)

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
        "auto",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    exit_code = module.main()
    assert exit_code == 0
    assert detect_calls == [config_path]
    report_invocations = [cmd for cmd in invoked if str(report_script) in cmd]
    assert len(report_invocations) == 1
    report_cmd = report_invocations[0]
    assert "--strategy-bridge-chain-status" in report_cmd
    idx = report_cmd.index("--strategy-bridge-chain-status")
    assert report_cmd[idx + 1] == "complete"


def test_detect_strategy_bridge_chain_status_returns_complete(tmp_path: Path, monkeypatch) -> None:
    script_path = Path("scripts/ops/run_reconnect_evidence.py")
    module = _load_script_module(script_path, "run_reconnect_evidence_script_detect_complete")

    config_path = tmp_path / "ctp.yaml"
    config_path.write_text("ctp:\n  environment: sim\n", encoding="utf-8")

    storage = {
        "market:state7d:SHFE.ag2406:latest": _state_snapshot_fields("SHFE.ag2406"),
        "strategy:intent:demo:latest": {
            "seq": "1",
            "count": "1",
            "intent_0": "SHFE.ag2406|BUY|OPEN|1|4500|123|trace-1",
            "ts_ns": "123",
        },
        "trade:order:trace-1:info": _order_event_fields("trace-1"),
    }

    monkeypatch.setattr(module, "load_runner_config", lambda _path: _fake_runner_config())
    monkeypatch.setattr(
        module,
        "load_redis_client_from_env",
        lambda _factory: _FakeRedisClient(storage),
    )

    status = module._detect_strategy_bridge_chain_status(config_path)
    assert status == "complete"


def test_detect_strategy_bridge_chain_status_returns_incomplete_when_order_missing(
    tmp_path: Path, monkeypatch
) -> None:
    script_path = Path("scripts/ops/run_reconnect_evidence.py")
    module = _load_script_module(script_path, "run_reconnect_evidence_script_detect_incomplete")

    config_path = tmp_path / "ctp.yaml"
    config_path.write_text("ctp:\n  environment: sim\n", encoding="utf-8")

    storage = {
        "market:state7d:SHFE.ag2406:latest": _state_snapshot_fields("SHFE.ag2406"),
        "strategy:intent:demo:latest": {
            "seq": "1",
            "count": "1",
            "intent_0": "SHFE.ag2406|BUY|OPEN|1|4500|123|trace-1",
            "ts_ns": "123",
        },
    }

    monkeypatch.setattr(module, "load_runner_config", lambda _path: _fake_runner_config())
    monkeypatch.setattr(
        module,
        "load_redis_client_from_env",
        lambda _factory: _FakeRedisClient(storage),
    )

    status = module._detect_strategy_bridge_chain_status(config_path)
    assert status == "incomplete"
