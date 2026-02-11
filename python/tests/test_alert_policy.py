from __future__ import annotations

from quant_hft.ops.alert_policy import (
    alert_report_to_dict,
    evaluate_alert_policy,
    render_alert_report_markdown,
)
from quant_hft.ops.monitoring import build_ops_health_report


def test_alert_policy_emits_info_when_all_sli_healthy() -> None:
    health = build_ops_health_report(
        strategy_bridge_latency_ms=120.0,
        strategy_bridge_target_ms=800.0,
        strategy_bridge_chain_status="complete",
        core_process_alive=True,
        redis_health="healthy",
        timescale_health="healthy",
        now_ns_fn=lambda: 123,
    )
    report = evaluate_alert_policy(health)
    payload = alert_report_to_dict(report)

    assert payload["overall_healthy"] is True
    alerts = payload["alerts"]
    assert isinstance(alerts, list)
    assert alerts[0]["severity"] == "info"
    assert alerts[0]["code"] == "OPS_ALL_HEALTHY"


def test_alert_policy_emits_critical_for_chain_and_storage_unhealthy() -> None:
    health = build_ops_health_report(
        strategy_bridge_latency_ms=3000.0,
        strategy_bridge_target_ms=800.0,
        strategy_bridge_chain_status="incomplete",
        core_process_alive=True,
        redis_health="unhealthy",
        timescale_health="healthy",
        now_ns_fn=lambda: 456,
    )
    report = evaluate_alert_policy(health)
    markdown = render_alert_report_markdown(report)

    assert report.overall_healthy is False
    assert any(item.severity == "critical" for item in report.alerts)
    assert "quant_hft_strategy_bridge_chain_integrity" in markdown
    assert "Ops Alert Report" in markdown
