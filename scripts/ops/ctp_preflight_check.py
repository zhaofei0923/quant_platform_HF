#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from quant_hft.ops.ctp_preflight import (
        CtpPreflightConfig,
        run_ctp_preflight,
    )
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.ops.ctp_preflight import (  # type: ignore[no-redef]
        CtpPreflightConfig,
        run_ctp_preflight,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CTP real-API preflight checks")
    parser.add_argument("--config", default="configs/sim/ctp_trading_hours.yaml")
    parser.add_argument(
        "--ctp-lib-dir",
        default="ctp_api/v6.7.11_20250617_api_traderapi_se_linux64",
    )
    parser.add_argument("--connect-timeout-ms", type=int, default=1500)
    parser.add_argument("--skip-network-check", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report = run_ctp_preflight(
        CtpPreflightConfig(
            config_path=Path(args.config),
            ctp_lib_dir=Path(args.ctp_lib_dir),
            connect_timeout_ms=max(100, args.connect_timeout_ms),
            skip_network_check=args.skip_network_check,
        )
    )

    if args.json:
        print(
            json.dumps(
                {
                    "ok": report.ok,
                    "items": [
                        {
                            "name": item.name,
                            "ok": item.ok,
                            "skipped": item.skipped,
                            "detail": item.detail,
                        }
                        for item in report.items
                    ],
                },
                ensure_ascii=True,
                indent=2,
            )
        )
    else:
        for item in report.items:
            status = "PASS"
            if item.skipped:
                status = "SKIP"
            elif not item.ok:
                status = "FAIL"
            print(f"[{status}] {item.name}: {item.detail}")
        print(f"overall: {'PASS' if report.ok else 'FAIL'}")

    return 0 if report.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
