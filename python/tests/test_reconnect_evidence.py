from __future__ import annotations

from pathlib import Path

from quant_hft.ops.ctp_preflight import CtpPreflightItem, CtpPreflightReport
from quant_hft.ops.reconnect_evidence import (
    ScenarioConfig,
    build_fault_inject_command,
    build_probe_command,
    default_scenarios,
    has_reachable_group_hint,
    parse_fallback_config_paths,
    required_tools_for_scenarios,
    select_scenarios,
)


def test_build_probe_command_contains_health_flags() -> None:
    cmd = build_probe_command(
        probe_bin=Path("./build-real/simnow_probe"),
        config_path=Path("configs/sim/ctp.yaml"),
        monitor_seconds=900,
        health_interval_ms=1000,
    )
    assert cmd == [
        "build-real/simnow_probe",
        "configs/sim/ctp.yaml",
        "--monitor-seconds",
        "900",
        "--health-interval-ms",
        "1000",
    ]


def test_build_fault_inject_command_for_disconnect() -> None:
    cmd = build_fault_inject_command(
        launcher_python="python3",
        fault_script=Path("scripts/ops/ctp_fault_inject.py"),
        scenario=ScenarioConfig(name="disconnect", duration_sec=20),
        event_log_file=Path("runtime/fault_events.jsonl"),
        iface="eth0",
        target_ip="182.254.243.31",
        ports="40001,40011",
        disconnect_mode="reset",
        execute=True,
        use_sudo=True,
    )
    assert cmd[:3] == ["sudo", "python3", "scripts/ops/ctp_fault_inject.py"]
    assert "--scenario" in cmd and "disconnect" in cmd
    assert "--execute" in cmd
    assert "--target-ip" in cmd and "182.254.243.31" in cmd
    assert "--ports" in cmd and "40001,40011" in cmd
    assert "--disconnect-mode" in cmd and "reset" in cmd


def test_build_fault_inject_command_for_combined_includes_netem_flags() -> None:
    cmd = build_fault_inject_command(
        launcher_python="python3",
        fault_script=Path("scripts/ops/ctp_fault_inject.py"),
        scenario=ScenarioConfig(
            name="combined",
            duration_sec=30,
            delay_ms=200,
            jitter_ms=40,
            loss_percent=3.0,
        ),
        event_log_file=Path("runtime/fault_events.jsonl"),
        iface="eth0",
        target_ip="182.254.243.31",
        ports="40001,40011",
        execute=False,
        use_sudo=False,
    )
    assert cmd[:2] == ["python3", "scripts/ops/ctp_fault_inject.py"]
    assert "--iface" in cmd and "eth0" in cmd
    assert "--delay-ms" in cmd and "200" in cmd
    assert "--jitter-ms" in cmd and "40" in cmd
    assert "--loss-percent" in cmd and "3" in cmd
    assert "--execute" not in cmd


def test_default_scenarios_contains_expected_order() -> None:
    names = [item.name for item in default_scenarios()]
    assert names == ["disconnect", "jitter", "loss", "combined"]


def test_select_scenarios_supports_subset() -> None:
    selected = select_scenarios("jitter,loss")
    assert [item.name for item in selected] == ["jitter", "loss"]


def test_select_scenarios_rejects_unknown_name() -> None:
    try:
        select_scenarios("disconnect,unknown")
    except ValueError as exc:
        assert "unknown scenario" in str(exc)
    else:
        raise AssertionError("expected ValueError for unknown scenario")


def test_required_tools_for_selected_scenarios() -> None:
    selected = select_scenarios("disconnect,jitter")
    tools = required_tools_for_scenarios(selected, execute_faults=True)
    assert tools == ("iptables", "tc")


def test_parse_fallback_config_paths_deduplicates_and_skips_empty() -> None:
    paths = parse_fallback_config_paths(
        "configs/sim/ctp_trading_hours_group2.yaml,,configs/sim/ctp_trading_hours_group2.yaml,configs/sim/ctp_trading_hours_group3.yaml"
    )
    assert paths == (
        Path("configs/sim/ctp_trading_hours_group2.yaml"),
        Path("configs/sim/ctp_trading_hours_group3.yaml"),
    )


def test_has_reachable_group_hint_true_on_service_window_item() -> None:
    report = CtpPreflightReport(
        ok=False,
        items=(
            CtpPreflightItem(
                name="service_window_hint",
                ok=True,
                detail="configured group timed out; reachable groups: group2(30002/30012)",
                skipped=True,
            ),
        ),
    )
    assert has_reachable_group_hint(report) is True


def test_has_reachable_group_hint_false_without_group_detail() -> None:
    report = CtpPreflightReport(
        ok=False,
        items=(
            CtpPreflightItem(
                name="service_window_hint",
                ok=True,
                detail="tcp timeout may indicate out-of-session hours",
                skipped=True,
            ),
        ),
    )
    assert has_reachable_group_hint(report) is False
