from __future__ import annotations

import pytest
from quant_hft.ops.monitoring import (
    InMemoryObservability,
    build_ops_health_report,
    ops_health_report_to_dict,
    render_ops_health_markdown,
)


def test_observability_records_metric_span_and_alert() -> None:
    tick = {"value": 0}

    def fake_now_ns() -> int:
        tick["value"] += 1_000
        return tick["value"]

    obs = InMemoryObservability(now_ns_fn=fake_now_ns)
    span = obs.start_span(
        "data_pipeline.run_once",
        trace_id="trace-1",
        attributes={"component": "data_pipeline"},
    )

    obs.record_metric(
        "quant_hft_data_pipeline_runs_total",
        1.0,
        kind="counter",
        labels={"result": "ok"},
    )
    obs.emit_alert(
        code="PIPELINE_EXPORT_EMPTY",
        severity="warning",
        message="no rows exported",
        labels={"run_id": "trace-1"},
    )
    span.end(attributes={"status": "ok"})

    snapshot = obs.snapshot()
    assert len(snapshot.metrics) == 1
    assert snapshot.metrics[0].name == "quant_hft_data_pipeline_runs_total"
    assert snapshot.metrics[0].labels["result"] == "ok"

    assert len(snapshot.traces) == 1
    assert snapshot.traces[0].trace_id == "trace-1"
    assert snapshot.traces[0].name == "data_pipeline.run_once"
    assert snapshot.traces[0].duration_ns > 0

    assert len(snapshot.alerts) == 1
    assert snapshot.alerts[0].code == "PIPELINE_EXPORT_EMPTY"
    assert snapshot.alerts[0].severity == "warn"


def test_observability_rejects_unsupported_alert_severity() -> None:
    obs = InMemoryObservability()
    with pytest.raises(ValueError, match="alert severity must be one of"):
        obs.emit_alert(
            code="PIPELINE_EXPORT_EMPTY",
            severity="notice",
            message="unsupported severity",
        )


def test_ops_health_report_build_and_render() -> None:
    report = build_ops_health_report(
        strategy_bridge_latency_ms=320.0,
        strategy_bridge_target_ms=1000.0,
        strategy_bridge_chain_status="complete",
        core_process_alive=True,
        redis_health="healthy",
        timescale_health="healthy",
        environment="sim",
        service="core_engine",
        metadata={"env": "sim"},
        now_ns_fn=lambda: 12345,
    )

    payload = ops_health_report_to_dict(report)
    assert payload["overall_healthy"] is True
    assert payload["generated_ts_ns"] == 12345
    assert isinstance(payload["slis"], list)
    assert len(payload["slis"]) == 6
    assert payload["metadata"]["environment"] == "sim"
    assert payload["metadata"]["service"] == "core_engine"
    sli_names = [item["name"] for item in payload["slis"]]
    assert "quant_hft_strategy_bridge_chain_integrity" in sli_names
    assert "quant_hft_storage_postgres_health" in sli_names
    assert all(item["slo_name"] for item in payload["slis"])
    assert all(item["environment"] == "sim" for item in payload["slis"])
    assert all(item["service"] == "core_engine" for item in payload["slis"])

    markdown = render_ops_health_markdown(report)
    assert "# Ops Health Report" in markdown
    assert "quant_hft_strategy_bridge_latency_p99_ms" in markdown
    assert "quant_hft_strategy_bridge_chain_integrity" in markdown
    assert "Overall healthy: yes" in markdown


def test_ops_health_report_includes_ctp_specific_slis() -> None:
    report = build_ops_health_report(
        strategy_bridge_latency_ms=320.0,
        strategy_bridge_target_ms=1000.0,
        strategy_bridge_chain_status="complete",
        core_process_alive=True,
        redis_health="healthy",
        timescale_health="healthy",
        ctp_query_latency_ms=1800.0,
        ctp_query_latency_target_ms=2000.0,
        ctp_flow_control_hits=4,
        ctp_flow_control_hits_target=10,
        ctp_disconnect_recovery_success_rate=1.0,
        ctp_disconnect_recovery_target=0.99,
        ctp_reject_classified_ratio=1.0,
        ctp_reject_classified_target=0.95,
        environment="sim",
        service="core_engine",
        now_ns_fn=lambda: 67890,
    )

    payload = ops_health_report_to_dict(report)
    sli_names = [item["name"] for item in payload["slis"]]
    assert "quant_hft_ctp_query_latency_p99_ms" in sli_names
    assert "quant_hft_ctp_flow_control_hits" in sli_names
    assert "quant_hft_ctp_disconnect_recovery_success_rate" in sli_names
    assert "quant_hft_ctp_reject_classified_ratio" in sli_names
    assert len(payload["slis"]) == 10
