#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from quant_hft.ops.alert_policy import (
        alert_report_to_dict,
        evaluate_alert_policy,
        render_alert_report_markdown,
    )
    from quant_hft.ops.monitoring import OpsHealthReport, SliRecord
except ModuleNotFoundError:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    sys.path.insert(0, str(repo_python))
    from quant_hft.ops.alert_policy import (  # type: ignore[no-redef]
        alert_report_to_dict,
        evaluate_alert_policy,
        render_alert_report_markdown,
    )
    from quant_hft.ops.monitoring import OpsHealthReport, SliRecord  # type: ignore[no-redef]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render ops alert report from ops health JSON")
    parser.add_argument("--health-json-file", required=True)
    parser.add_argument("--output-json-file", default="docs/results/ops_alert_report.json")
    parser.add_argument("--output-markdown-file", default="docs/results/ops_alert_report.md")
    return parser.parse_args()


def _load_health(path: Path) -> OpsHealthReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    slis = tuple(
        SliRecord(
            name=str(item["name"]),
            value=item.get("value"),
            target=item.get("target"),
            unit=str(item["unit"]),
            healthy=bool(item["healthy"]),
            detail=str(item["detail"]),
        )
        for item in payload.get("slis", [])
    )
    return OpsHealthReport(
        generated_ts_ns=int(payload.get("generated_ts_ns", 0)),
        scope=str(payload.get("scope", "")),
        overall_healthy=bool(payload.get("overall_healthy", False)),
        slis=slis,
        metadata={str(k): str(v) for k, v in dict(payload.get("metadata", {})).items()},
    )


def main() -> int:
    args = _parse_args()
    health_file = Path(args.health_json_file)
    if not health_file.exists():
        print(f"error: health json not found: {health_file}", file=sys.stderr)
        return 2

    report = evaluate_alert_policy(_load_health(health_file))
    json_file = Path(args.output_json_file)
    md_file = Path(args.output_markdown_file)
    json_file.parent.mkdir(parents=True, exist_ok=True)
    md_file.parent.mkdir(parents=True, exist_ok=True)
    json_file.write_text(
        json.dumps(alert_report_to_dict(report), ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    md_file.write_text(render_alert_report_markdown(report), encoding="utf-8")

    print(str(json_file))
    print(str(md_file))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
