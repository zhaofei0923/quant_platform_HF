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
