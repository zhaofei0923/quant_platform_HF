from __future__ import annotations

import json
import stat
from pathlib import Path

from quant_hft.ops.fault_injection import (
    append_fault_event,
    build_disconnect_plan,
    build_disconnect_reset_plan,
    build_fault_event,
    build_netem_plan,
    build_netem_reset_plan,
    resolve_command_binary,
)


def test_disconnect_plan_contains_bidirectional_rules() -> None:
    plan = build_disconnect_plan(target_ip="182.254.243.31", ports=[40001, 40011])

    assert len(plan) == 4
    assert "iptables -I OUTPUT -p tcp -d 182.254.243.31 --dport 40001 -j DROP" in plan
    assert "iptables -I INPUT -p tcp -s 182.254.243.31 --sport 40011 -j DROP" in plan


def test_disconnect_plan_supports_custom_firewall_command() -> None:
    plan = build_disconnect_plan(
        target_ip="182.254.243.31",
        ports=[40001],
        firewall_cmd="/usr/sbin/iptables-nft",
    )

    assert plan == [
        "/usr/sbin/iptables-nft -I OUTPUT -p tcp -d 182.254.243.31 --dport 40001 -j DROP",
        "/usr/sbin/iptables-nft -I INPUT -p tcp -s 182.254.243.31 --sport 40001 -j DROP",
    ]


def test_disconnect_plan_supports_tcp_reset_mode() -> None:
    plan = build_disconnect_plan(
        target_ip="182.254.243.31",
        ports=[40001],
        disconnect_mode="reset",
    )

    assert plan == [
        (
            "iptables -I OUTPUT -p tcp -d 182.254.243.31 --dport 40001 -j REJECT "
            "--reject-with tcp-reset"
        ),
        (
            "iptables -I INPUT -p tcp -s 182.254.243.31 --sport 40001 -j REJECT "
            "--reject-with tcp-reset"
        ),
    ]


def test_disconnect_reset_plan_supports_tcp_reset_mode() -> None:
    plan = build_disconnect_reset_plan(
        target_ip="182.254.243.31",
        ports=[40011],
        disconnect_mode="reset",
    )

    assert plan == [
        (
            "iptables -D OUTPUT -p tcp -d 182.254.243.31 --dport 40011 -j REJECT "
            "--reject-with tcp-reset || true"
        ),
        (
            "iptables -D INPUT -p tcp -s 182.254.243.31 --sport 40011 -j REJECT "
            "--reject-with tcp-reset || true"
        ),
    ]


def test_netem_plan_combines_delay_jitter_and_loss() -> None:
    plan = build_netem_plan(
        iface="eth0",
        delay_ms=250,
        jitter_ms=30,
        loss_percent=2.5,
    )

    assert plan == ["tc qdisc replace dev eth0 root netem delay 250ms 30ms loss 2.5%"]


def test_netem_reset_plan_deletes_root_qdisc() -> None:
    assert build_netem_reset_plan(iface="ens33") == [
        "tc qdisc del dev ens33 root || true",
    ]


def test_fault_event_persists_as_jsonl(tmp_path: Path) -> None:
    event_file = tmp_path / "fault_events.jsonl"
    event = build_fault_event(
        mode="run",
        scenario="disconnect",
        phase="apply",
        execute=True,
        ts_ns=1_234_567,
        parameters={"duration_sec": "20"},
    )
    append_fault_event(event_file, event)

    lines = event_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["mode"] == "run"
    assert payload["scenario"] == "disconnect"
    assert payload["phase"] == "apply"
    assert payload["execute"] is True
    assert payload["ts_ns"] == 1_234_567
    assert payload["parameters"]["duration_sec"] == "20"


def test_resolve_command_binary_falls_back_to_search_dirs(tmp_path: Path, monkeypatch) -> None:
    custom_sbin = tmp_path / "sbin"
    custom_sbin.mkdir(parents=True, exist_ok=True)
    tool_path = custom_sbin / "iptables-nft"
    tool_path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    tool_path.chmod(tool_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    monkeypatch.setenv("PATH", "")

    resolved = resolve_command_binary(
        ("iptables", "iptables-nft"),
        search_dirs=(custom_sbin,),
    )

    assert resolved == str(tool_path)
