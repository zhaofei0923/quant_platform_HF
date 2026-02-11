from __future__ import annotations

from dataclasses import dataclass

from quant_hft.ops.monitoring import OpsHealthReport
from quant_hft.ops.sli_catalog import strip_prefix, with_prefix


@dataclass(frozen=True)
class AlertItem:
    code: str
    severity: str
    message: str
    sli_name: str


@dataclass(frozen=True)
class AlertReport:
    generated_ts_ns: int
    overall_healthy: bool
    alerts: tuple[AlertItem, ...]


def evaluate_alert_policy(report: OpsHealthReport) -> AlertReport:
    alerts: list[AlertItem] = []
    for sli in report.slis:
        if sli.healthy:
            continue
        base_name = strip_prefix(sli.name)
        severity = "warn"
        if base_name in {
            "core_process_alive",
            "strategy_bridge_chain_integrity",
            "storage_redis_health",
            "storage_timescale_health",
        }:
            severity = "critical"
        alerts.append(
            AlertItem(
                code=f"OPS_{base_name.upper()}_UNHEALTHY",
                severity=severity,
                message=f"{base_name} unhealthy: {sli.detail}",
                sli_name=with_prefix(base_name),
            )
        )

    if not alerts:
        alerts.append(
            AlertItem(
                code="OPS_ALL_HEALTHY",
                severity="info",
                message="all SLI checks are healthy",
                sli_name=with_prefix("core_process_alive"),
            )
        )

    return AlertReport(
        generated_ts_ns=report.generated_ts_ns,
        overall_healthy=report.overall_healthy,
        alerts=tuple(alerts),
    )


def alert_report_to_dict(report: AlertReport) -> dict[str, object]:
    return {
        "generated_ts_ns": report.generated_ts_ns,
        "overall_healthy": report.overall_healthy,
        "alerts": [
            {
                "code": item.code,
                "severity": item.severity,
                "message": item.message,
                "sli_name": item.sli_name,
            }
            for item in report.alerts
        ],
    }


def render_alert_report_markdown(report: AlertReport) -> str:
    lines = [
        "# Ops Alert Report",
        "",
        f"- Generated TS (ns): {report.generated_ts_ns}",
        f"- Overall healthy: {'yes' if report.overall_healthy else 'no'}",
        "",
        "| Code | Severity | SLI | Message |",
        "|---|---|---|---|",
    ]
    for item in report.alerts:
        lines.append(f"| {item.code} | {item.severity} | {item.sli_name} | {item.message} |")
    return "\n".join(lines) + "\n"
