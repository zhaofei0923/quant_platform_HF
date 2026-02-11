from __future__ import annotations

from quant_hft.ops.monitoring import InMemoryObservability


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
    assert snapshot.alerts[0].severity == "warning"
