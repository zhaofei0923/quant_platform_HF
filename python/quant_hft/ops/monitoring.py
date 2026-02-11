from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricRecord:
    name: str
    value: float
    kind: str
    labels: dict[str, str]
    ts_ns: int


@dataclass(frozen=True)
class TraceSpanRecord:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    start_ns: int
    end_ns: int
    duration_ns: int
    attributes: dict[str, str]


@dataclass(frozen=True)
class AlertRecord:
    code: str
    severity: str
    message: str
    labels: dict[str, str]
    ts_ns: int


@dataclass(frozen=True)
class ObservabilitySnapshot:
    metrics: tuple[MetricRecord, ...]
    traces: tuple[TraceSpanRecord, ...]
    alerts: tuple[AlertRecord, ...]


@dataclass(frozen=True)
class SliRecord:
    name: str
    value: float | None
    target: float | None
    unit: str
    healthy: bool
    detail: str


@dataclass(frozen=True)
class OpsHealthReport:
    generated_ts_ns: int
    scope: str
    overall_healthy: bool
    slis: tuple[SliRecord, ...]
    metadata: dict[str, str]


def _normalize_labels(labels: Mapping[str, str] | None) -> dict[str, str]:
    if labels is None:
        return {}
    return {str(key): str(value) for key, value in labels.items()}


class _ActiveSpan:
    def __init__(
        self,
        sink: InMemoryObservability,
        *,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        name: str,
        start_ns: int,
        attributes: Mapping[str, str] | None = None,
    ) -> None:
        self._sink = sink
        self._trace_id = trace_id
        self._span_id = span_id
        self._parent_span_id = parent_span_id
        self._name = name
        self._start_ns = start_ns
        self._attributes = _normalize_labels(attributes)
        self._closed = False

    @property
    def trace_id(self) -> str:
        return self._trace_id

    @property
    def span_id(self) -> str:
        return self._span_id

    def end(self, attributes: Mapping[str, str] | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        merged_attributes = dict(self._attributes)
        if attributes is not None:
            merged_attributes.update(_normalize_labels(attributes))
        end_ns = self._sink.now_ns()
        self._sink._append_trace(
            TraceSpanRecord(
                trace_id=self._trace_id,
                span_id=self._span_id,
                parent_span_id=self._parent_span_id,
                name=self._name,
                start_ns=self._start_ns,
                end_ns=end_ns,
                duration_ns=max(0, end_ns - self._start_ns),
                attributes=merged_attributes,
            )
        )


class InMemoryObservability:
    def __init__(self, now_ns_fn: Callable[[], int] | None = None) -> None:
        self._now_ns_fn = now_ns_fn or time.time_ns
        self._metrics: list[MetricRecord] = []
        self._traces: list[TraceSpanRecord] = []
        self._alerts: list[AlertRecord] = []

    def now_ns(self) -> int:
        return int(self._now_ns_fn())

    def record_metric(
        self,
        name: str,
        value: float,
        *,
        kind: str = "gauge",
        labels: Mapping[str, str] | None = None,
    ) -> None:
        if not name:
            raise ValueError("metric name is required")
        if kind not in {"counter", "gauge", "histogram"}:
            raise ValueError(f"unsupported metric kind: {kind}")
        self._metrics.append(
            MetricRecord(
                name=name,
                value=float(value),
                kind=kind,
                labels=_normalize_labels(labels),
                ts_ns=self.now_ns(),
            )
        )

    def start_span(
        self,
        name: str,
        *,
        trace_id: str | None = None,
        parent_span_id: str | None = None,
        attributes: Mapping[str, str] | None = None,
    ) -> _ActiveSpan:
        if not name:
            raise ValueError("span name is required")
        span_trace_id = trace_id or uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        return _ActiveSpan(
            self,
            trace_id=span_trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            name=name,
            start_ns=self.now_ns(),
            attributes=attributes,
        )

    def emit_alert(
        self,
        *,
        code: str,
        severity: str,
        message: str,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        if not code:
            raise ValueError("alert code is required")
        if not severity:
            raise ValueError("alert severity is required")
        if not message:
            raise ValueError("alert message is required")
        self._alerts.append(
            AlertRecord(
                code=code,
                severity=severity,
                message=message,
                labels=_normalize_labels(labels),
                ts_ns=self.now_ns(),
            )
        )

    def snapshot(self) -> ObservabilitySnapshot:
        return ObservabilitySnapshot(
            metrics=tuple(self._metrics),
            traces=tuple(self._traces),
            alerts=tuple(self._alerts),
        )

    def reset(self) -> None:
        self._metrics.clear()
        self._traces.clear()
        self._alerts.clear()

    def _append_trace(self, span: TraceSpanRecord) -> None:
        self._traces.append(span)


def _normalize_health_flag(raw: str) -> bool | None:
    normalized = raw.strip().lower()
    if normalized in {"healthy", "ok", "true", "1"}:
        return True
    if normalized in {"unhealthy", "failed", "false", "0"}:
        return False
    return None


def _normalize_chain_status(raw: str) -> bool | None:
    normalized = raw.strip().lower()
    if normalized in {"complete", "ok", "healthy", "true", "1"}:
        return True
    if normalized in {"incomplete", "broken", "false", "0", "unhealthy"}:
        return False
    return None


def build_ops_health_report(
    *,
    strategy_bridge_latency_ms: float | None,
    strategy_bridge_target_ms: float,
    strategy_bridge_chain_status: str = "unknown",
    core_process_alive: bool,
    redis_health: str = "unknown",
    timescale_health: str = "unknown",
    scope: str = "core_engine + strategy_bridge + storage",
    metadata: Mapping[str, str] | None = None,
    now_ns_fn: Callable[[], int] | None = None,
) -> OpsHealthReport:
    if strategy_bridge_target_ms <= 0:
        raise ValueError("strategy_bridge_target_ms must be > 0")
    now_ns = int((now_ns_fn or time.time_ns)())
    slis: list[SliRecord] = []
    slis.append(
        SliRecord(
            name="core_process_alive",
            value=1.0 if core_process_alive else 0.0,
            target=1.0,
            unit="bool",
            healthy=core_process_alive,
            detail="probe process stayed alive during collection",
        )
    )

    latency_healthy = (
        strategy_bridge_latency_ms is not None
        and strategy_bridge_latency_ms <= strategy_bridge_target_ms
    )
    slis.append(
        SliRecord(
            name="strategy_bridge_latency_p99_ms",
            value=strategy_bridge_latency_ms,
            target=strategy_bridge_target_ms,
            unit="ms",
            healthy=latency_healthy,
            detail="derived from reconnect recovery samples",
        )
    )

    chain_ok = _normalize_chain_status(strategy_bridge_chain_status)
    slis.append(
        SliRecord(
            name="strategy_bridge_chain_integrity",
            value=1.0 if chain_ok else 0.0 if chain_ok is False else None,
            target=1.0,
            unit="bool",
            healthy=chain_ok is True,
            detail=f"input={strategy_bridge_chain_status}",
        )
    )

    redis_ok = _normalize_health_flag(redis_health)
    slis.append(
        SliRecord(
            name="storage_redis_health",
            value=1.0 if redis_ok else 0.0 if redis_ok is False else None,
            target=1.0,
            unit="bool",
            healthy=redis_ok is True,
            detail=f"input={redis_health}",
        )
    )

    timescale_ok = _normalize_health_flag(timescale_health)
    slis.append(
        SliRecord(
            name="storage_timescale_health",
            value=1.0 if timescale_ok else 0.0 if timescale_ok is False else None,
            target=1.0,
            unit="bool",
            healthy=timescale_ok is True,
            detail=f"input={timescale_health}",
        )
    )

    overall_healthy = all(item.healthy for item in slis)
    return OpsHealthReport(
        generated_ts_ns=now_ns,
        scope=scope,
        overall_healthy=overall_healthy,
        slis=tuple(slis),
        metadata=_normalize_labels(metadata),
    )


def ops_health_report_to_dict(report: OpsHealthReport) -> dict[str, object]:
    return {
        "generated_ts_ns": report.generated_ts_ns,
        "scope": report.scope,
        "overall_healthy": report.overall_healthy,
        "metadata": dict(report.metadata),
        "slis": [
            {
                "name": item.name,
                "value": item.value,
                "target": item.target,
                "unit": item.unit,
                "healthy": item.healthy,
                "detail": item.detail,
            }
            for item in report.slis
        ],
    }


def render_ops_health_markdown(report: OpsHealthReport) -> str:
    lines = [
        "# Ops Health Report",
        "",
        f"- Scope: {report.scope}",
        f"- Generated TS (ns): {report.generated_ts_ns}",
        f"- Overall healthy: {'yes' if report.overall_healthy else 'no'}",
        "",
        "## SLI",
        "| Name | Value | Target | Healthy | Detail |",
        "|---|---:|---:|---|---|",
    ]
    for item in report.slis:
        value = "n/a" if item.value is None else f"{item.value:g}"
        target = "n/a" if item.target is None else f"{item.target:g}"
        lines.append(
            "| {name} | {value} | {target} | {healthy} | {detail} |".format(
                name=item.name,
                value=value,
                target=target,
                healthy="yes" if item.healthy else "no",
                detail=item.detail,
            )
        )
    if report.metadata:
        lines.extend(["", "## Metadata"])
        for key in sorted(report.metadata):
            lines.append(f"- {key}: {report.metadata[key]}")
    return "\n".join(lines) + "\n"
