#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FrontGroup:
    group_id: int
    trader_port: int
    market_port: int


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auto-select reachable SimNow front group and emit CTP env exports"
    )
    parser.add_argument("--host", default="182.254.243.31")
    parser.add_argument("--timeout-ms", type=int, default=1500)
    parser.add_argument("--broker-id", default="9999")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--investor-id", default="")
    parser.add_argument("--password", required=True)
    parser.add_argument("--app-id", default="simnow_client_test")
    parser.add_argument("--auth-code", default="0000000000000000")
    parser.add_argument("--enable-terminal-auth", choices=["true", "false"], default="true")
    parser.add_argument(
        "--output-env-file",
        default="/tmp/ctp_sim_selected.env",
        help="Write shell exports to this file",
    )
    parser.add_argument("--json", action="store_true")
    return parser


def _probe(host: str, port: int, timeout_ms: int) -> tuple[bool, str]:
    timeout_sec = max(0.05, timeout_ms / 1000.0)
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True, "ok"
    except OSError as exc:
        return False, str(exc)


def _discover_group(
    host: str, timeout_ms: int
) -> tuple[FrontGroup | None, list[dict[str, object]]]:
    groups = (
        FrontGroup(1, 30001, 30011),
        FrontGroup(2, 30002, 30012),
        FrontGroup(3, 30003, 30013),
    )
    report: list[dict[str, object]] = []
    selected: FrontGroup | None = None
    for group in groups:
        trader_ok, trader_detail = _probe(host, group.trader_port, timeout_ms)
        market_ok, market_detail = _probe(host, group.market_port, timeout_ms)
        item = {
            "group": group.group_id,
            "trader_port": group.trader_port,
            "market_port": group.market_port,
            "trader_ok": trader_ok,
            "market_ok": market_ok,
            "trader_detail": trader_detail,
            "market_detail": market_detail,
        }
        report.append(item)
        if selected is None and trader_ok and market_ok:
            selected = group
    return selected, report


def _render_exports(args: argparse.Namespace, group: FrontGroup, host: str) -> str:
    investor_id = args.investor_id or args.user_id
    lines = [
        f"export CTP_SIM_BROKER_ID='{args.broker_id}'",
        f"export CTP_SIM_USER_ID='{args.user_id}'",
        f"export CTP_SIM_INVESTOR_ID='{investor_id}'",
        f"export CTP_SIM_PASSWORD='{args.password}'",
        f"export CTP_SIM_APP_ID='{args.app_id}'",
        f"export CTP_SIM_AUTH_CODE='{args.auth_code}'",
        f"export CTP_SIM_TRADER_FRONT='tcp://{host}:{group.trader_port}'",
        f"export CTP_SIM_MARKET_FRONT='tcp://{host}:{group.market_port}'",
        f"export CTP_SIM_ENABLE_TERMINAL_AUTH='{args.enable_terminal_auth}'",
        f"export CTP_SIM_SELECTED_GROUP='{group.group_id}'",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    args = _build_parser().parse_args()
    selected, report = _discover_group(args.host, max(100, args.timeout_ms))
    if selected is None:
        payload = {
            "ok": False,
            "selected_group": None,
            "host": args.host,
            "report": report,
            "detail": "no reachable front group",
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            for item in report:
                print(
                    "group{group} trader:{trader_port}={trader_ok} ({trader_detail}) "
                    "market:{market_port}={market_ok} ({market_detail})".format(**item)
                )
            print("overall: FAIL")
        return 2

    exports_text = _render_exports(args, selected, args.host)
    output_path = Path(args.output_env_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(exports_text, encoding="utf-8")

    payload = {
        "ok": True,
        "selected_group": selected.group_id,
        "host": args.host,
        "trader_front": f"tcp://{args.host}:{selected.trader_port}",
        "market_front": f"tcp://{args.host}:{selected.market_port}",
        "output_env_file": str(output_path),
        "report": report,
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        for item in report:
            print(
                "group{group} trader:{trader_port}={trader_ok} ({trader_detail}) "
                "market:{market_port}={market_ok} ({market_detail})".format(**item)
            )
        print(f"selected_group: {selected.group_id}")
        print(f"output_env_file: {output_path}")
        print(f"trader_front: tcp://{args.host}:{selected.trader_port}")
        print(f"market_front: tcp://{args.host}:{selected.market_port}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
