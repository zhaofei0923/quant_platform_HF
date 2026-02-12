#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from quant_hft.ops.monitoring import build_ops_health_report, ops_health_report_to_dict
except ModuleNotFoundError:  # pragma: no cover - script fallback path
    REPO_ROOT = Path(__file__).resolve().parents[2]
    PYTHON_ROOT = REPO_ROOT / "python"
    if str(PYTHON_ROOT) not in sys.path:
        sys.path.insert(0, str(PYTHON_ROOT))
    from quant_hft.ops.monitoring import (  # type: ignore[no-redef]
        build_ops_health_report,
        ops_health_report_to_dict,
    )


def _parse_bool(raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"invalid boolean value: {raw}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate CTP readiness evidence JSON from CTP-specific SLI samples."
    )
    parser.add_argument("--output-json", required=True, help="Output readiness JSON file path")
    parser.add_argument("--query-latency-ms", type=float, required=True)
    parser.add_argument("--query-latency-target-ms", type=float, default=2000.0)
    parser.add_argument("--flow-control-hits", type=float, required=True)
    parser.add_argument("--flow-control-hits-target", type=float, default=10.0)
    parser.add_argument("--disconnect-recovery-success-rate", type=float, required=True)
    parser.add_argument("--disconnect-recovery-target", type=float, default=0.99)
    parser.add_argument("--reject-classified-ratio", type=float, required=True)
    parser.add_argument("--reject-classified-target", type=float, default=0.95)
    parser.add_argument("--strategy-bridge-latency-ms", type=float, default=0.0)
    parser.add_argument("--strategy-bridge-target-ms", type=float, default=1500.0)
    parser.add_argument("--strategy-bridge-chain-status", default="unknown")
    parser.add_argument("--core-process-alive", default="true")
    parser.add_argument("--redis-health", default="unknown")
    parser.add_argument("--timescale-health", default="unknown")
    parser.add_argument("--environment", default="prodlike")
    parser.add_argument("--service", default="core_engine")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        core_process_alive = _parse_bool(args.core_process_alive)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    report = build_ops_health_report(
        strategy_bridge_latency_ms=args.strategy_bridge_latency_ms,
        strategy_bridge_target_ms=args.strategy_bridge_target_ms,
        strategy_bridge_chain_status=args.strategy_bridge_chain_status,
        core_process_alive=core_process_alive,
        redis_health=args.redis_health,
        timescale_health=args.timescale_health,
        ctp_query_latency_ms=args.query_latency_ms,
        ctp_query_latency_target_ms=args.query_latency_target_ms,
        ctp_flow_control_hits=args.flow_control_hits,
        ctp_flow_control_hits_target=args.flow_control_hits_target,
        ctp_disconnect_recovery_success_rate=args.disconnect_recovery_success_rate,
        ctp_disconnect_recovery_target=args.disconnect_recovery_target,
        ctp_reject_classified_ratio=args.reject_classified_ratio,
        ctp_reject_classified_target=args.reject_classified_target,
        environment=args.environment,
        service=args.service,
        metadata={"evidence_type": "ctp_readiness"},
    )

    payload = ops_health_report_to_dict(report)
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(output_path))
    return 0 if report.overall_healthy else 3


if __name__ == "__main__":
    raise SystemExit(main())
