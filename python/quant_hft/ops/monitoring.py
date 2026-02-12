from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from quant_hft.ops.sli_catalog import with_prefix

_CANONICAL_ALERT_SEVERITIES = {"info", "warn", "critical"}


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
    slo_name: str
    environment: str
    service: str
    value: float | None
    target: float | None
    unit: str
    healthy: bool
    detail: str


@dataclass(frozen=True)
class SliSloConfig:
    strategy_bridge_latency_target_ms: float = 1500.0


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


def _normalize_alert_severity(severity: str) -> str:
    normalized = severity.strip().lower()
    if normalized == "warning":
        normalized = "warn"
    if normalized not in _CANONICAL_ALERT_SEVERITIES:
        supported = "|".join(sorted(_CANONICAL_ALERT_SEVERITIES))
        raise ValueError(f"alert severity must be one of {supported}")
    return normalized


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
        normalized_severity = _normalize_alert_severity(severity)
        self._alerts.append(
            AlertRecord(
                code=code,
                severity=normalized_severity,
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
    postgres_health: str = "",
    ctp_query_latency_ms: float | None = None,
    ctp_query_latency_target_ms: float = 2000.0,
    ctp_flow_control_hits: float | None = None,
    ctp_flow_control_hits_target: float = 10.0,
    ctp_disconnect_recovery_success_rate: float | None = None,
    ctp_disconnect_recovery_target: float = 0.99,
    ctp_reject_classified_ratio: float | None = None,
    ctp_reject_classified_target: float = 0.95,
    kafka_publish_success_rate: float | None = None,
    kafka_publish_success_target: float = 0.99,
    kafka_spool_backlog: float | None = None,
    kafka_spool_backlog_target: float = 1000.0,
    cdc_lag_seconds: float | None = None,
    cdc_lag_target_seconds: float = 5.0,
    clickhouse_ingest_lag_seconds: float | None = None,
    clickhouse_ingest_lag_target_seconds: float = 3.0,
    parquet_lifecycle_success: float | None = None,
    parquet_lifecycle_success_target: float = 1.0,
    scope: str = "core_engine + strategy_bridge + storage",
    environment: str = "unknown",
    service: str = "core_engine",
    sli_slo_config: SliSloConfig | None = None,
    metadata: Mapping[str, str] | None = None,
    now_ns_fn: Callable[[], int] | None = None,
) -> OpsHealthReport:
    effective_config = sli_slo_config or SliSloConfig(
        strategy_bridge_latency_target_ms=strategy_bridge_target_ms
    )
    if effective_config.strategy_bridge_latency_target_ms <= 0:
        raise ValueError("strategy_bridge_target_ms must be > 0")
    if ctp_query_latency_target_ms <= 0:
        raise ValueError("ctp_query_latency_target_ms must be > 0")
    if ctp_flow_control_hits_target < 0:
        raise ValueError("ctp_flow_control_hits_target must be >= 0")
    if ctp_disconnect_recovery_target < 0 or ctp_disconnect_recovery_target > 1:
        raise ValueError("ctp_disconnect_recovery_target must be in [0, 1]")
    if ctp_reject_classified_target < 0 or ctp_reject_classified_target > 1:
        raise ValueError("ctp_reject_classified_target must be in [0, 1]")
    if kafka_publish_success_target < 0 or kafka_publish_success_target > 1:
        raise ValueError("kafka_publish_success_target must be in [0, 1]")
    if kafka_spool_backlog_target < 0:
        raise ValueError("kafka_spool_backlog_target must be >= 0")
    if cdc_lag_target_seconds < 0:
        raise ValueError("cdc_lag_target_seconds must be >= 0")
    if clickhouse_ingest_lag_target_seconds < 0:
        raise ValueError("clickhouse_ingest_lag_target_seconds must be >= 0")
    if parquet_lifecycle_success_target < 0 or parquet_lifecycle_success_target > 1:
        raise ValueError("parquet_lifecycle_success_target must be in [0, 1]")
    now_ns = int((now_ns_fn or time.time_ns)())
    slis: list[SliRecord] = []
    slis.append(
        SliRecord(
            name=with_prefix("core_process_alive"),
            slo_name=with_prefix("core_process_alive"),
            environment=environment,
            service=service,
            value=1.0 if core_process_alive else 0.0,
            target=1.0,
            unit="bool",
            healthy=core_process_alive,
            detail="probe process stayed alive during collection",
        )
    )

    latency_healthy = (
        strategy_bridge_latency_ms is not None
        and strategy_bridge_latency_ms <= effective_config.strategy_bridge_latency_target_ms
    )
    slis.append(
        SliRecord(
            name=with_prefix("strategy_bridge_latency_p99_ms"),
            slo_name=with_prefix("strategy_bridge_latency_p99_ms"),
            environment=environment,
            service=service,
            value=strategy_bridge_latency_ms,
            target=effective_config.strategy_bridge_latency_target_ms,
            unit="ms",
            healthy=latency_healthy,
            detail="derived from reconnect recovery samples",
        )
    )

    chain_ok = _normalize_chain_status(strategy_bridge_chain_status)
    slis.append(
        SliRecord(
            name=with_prefix("strategy_bridge_chain_integrity"),
            slo_name=with_prefix("strategy_bridge_chain_integrity"),
            environment=environment,
            service=service,
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
            name=with_prefix("storage_redis_health"),
            slo_name=with_prefix("storage_redis_health"),
            environment=environment,
            service=service,
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
            name=with_prefix("storage_timescale_health"),
            slo_name=with_prefix("storage_timescale_health"),
            environment=environment,
            service=service,
            value=1.0 if timescale_ok else 0.0 if timescale_ok is False else None,
            target=1.0,
            unit="bool",
            healthy=timescale_ok is True,
            detail=f"input={timescale_health}",
        )
    )

    effective_postgres_health = postgres_health.strip() if postgres_health else timescale_health
    postgres_ok = _normalize_health_flag(effective_postgres_health)
    slis.append(
        SliRecord(
            name=with_prefix("storage_postgres_health"),
            slo_name=with_prefix("storage_postgres_health"),
            environment=environment,
            service=service,
            value=1.0 if postgres_ok else 0.0 if postgres_ok is False else None,
            target=1.0,
            unit="bool",
            healthy=postgres_ok is True,
            detail=f"input={effective_postgres_health}",
        )
    )

    if ctp_query_latency_ms is not None:
        query_latency_healthy = ctp_query_latency_ms <= ctp_query_latency_target_ms
        slis.append(
            SliRecord(
                name=with_prefix("ctp_query_latency_p99_ms"),
                slo_name=with_prefix("ctp_query_latency_p99_ms"),
                environment=environment,
                service=service,
                value=ctp_query_latency_ms,
                target=ctp_query_latency_target_ms,
                unit="ms",
                healthy=query_latency_healthy,
                detail="CTP ReqQry* to callback p99 latency",
            )
        )

    if ctp_flow_control_hits is not None:
        flow_control_healthy = ctp_flow_control_hits <= ctp_flow_control_hits_target
        slis.append(
            SliRecord(
                name=with_prefix("ctp_flow_control_hits"),
                slo_name=with_prefix("ctp_flow_control_hits"),
                environment=environment,
                service=service,
                value=ctp_flow_control_hits,
                target=ctp_flow_control_hits_target,
                unit="count",
                healthy=flow_control_healthy,
                detail="CTP flow-control and query-not-ready hits",
            )
        )

    if ctp_disconnect_recovery_success_rate is not None:
        recovery_healthy = ctp_disconnect_recovery_success_rate >= ctp_disconnect_recovery_target
        slis.append(
            SliRecord(
                name=with_prefix("ctp_disconnect_recovery_success_rate"),
                slo_name=with_prefix("ctp_disconnect_recovery_success_rate"),
                environment=environment,
                service=service,
                value=ctp_disconnect_recovery_success_rate,
                target=ctp_disconnect_recovery_target,
                unit="ratio",
                healthy=recovery_healthy,
                detail="CTP disconnect to healthy reconnect success rate",
            )
        )

    if ctp_reject_classified_ratio is not None:
        reject_classified_healthy = ctp_reject_classified_ratio >= ctp_reject_classified_target
        slis.append(
            SliRecord(
                name=with_prefix("ctp_reject_classified_ratio"),
                slo_name=with_prefix("ctp_reject_classified_ratio"),
                environment=environment,
                service=service,
                value=ctp_reject_classified_ratio,
                target=ctp_reject_classified_target,
                unit="ratio",
                healthy=reject_classified_healthy,
                detail="ratio of rejected orders with structured classification",
            )
        )

    if kafka_publish_success_rate is not None:
        kafka_publish_healthy = kafka_publish_success_rate >= kafka_publish_success_target
        slis.append(
            SliRecord(
                name=with_prefix("kafka_publish_success_rate"),
                slo_name=with_prefix("kafka_publish_success_rate"),
                environment=environment,
                service=service,
                value=kafka_publish_success_rate,
                target=kafka_publish_success_target,
                unit="ratio",
                healthy=kafka_publish_healthy,
                detail="market snapshot publish success ratio to Kafka topic",
            )
        )

    if kafka_spool_backlog is not None:
        kafka_spool_healthy = kafka_spool_backlog <= kafka_spool_backlog_target
        slis.append(
            SliRecord(
                name=with_prefix("kafka_spool_backlog"),
                slo_name=with_prefix("kafka_spool_backlog"),
                environment=environment,
                service=service,
                value=kafka_spool_backlog,
                target=kafka_spool_backlog_target,
                unit="count",
                healthy=kafka_spool_healthy,
                detail="local market bus spool backlog entries",
            )
        )

    if cdc_lag_seconds is not None:
        cdc_lag_healthy = cdc_lag_seconds <= cdc_lag_target_seconds
        slis.append(
            SliRecord(
                name=with_prefix("cdc_lag_seconds"),
                slo_name=with_prefix("cdc_lag_seconds"),
                environment=environment,
                service=service,
                value=cdc_lag_seconds,
                target=cdc_lag_target_seconds,
                unit="seconds",
                healthy=cdc_lag_healthy,
                detail="trading_core CDC lag from PostgreSQL to ClickHouse",
            )
        )

    if clickhouse_ingest_lag_seconds is not None:
        clickhouse_lag_healthy = (
            clickhouse_ingest_lag_seconds <= clickhouse_ingest_lag_target_seconds
        )
        slis.append(
            SliRecord(
                name=with_prefix("clickhouse_ingest_lag_seconds"),
                slo_name=with_prefix("clickhouse_ingest_lag_seconds"),
                environment=environment,
                service=service,
                value=clickhouse_ingest_lag_seconds,
                target=clickhouse_ingest_lag_target_seconds,
                unit="seconds",
                healthy=clickhouse_lag_healthy,
                detail="Kafka to ClickHouse ingestion lag",
            )
        )

    if parquet_lifecycle_success is not None:
        lifecycle_healthy = parquet_lifecycle_success >= parquet_lifecycle_success_target
        slis.append(
            SliRecord(
                name=with_prefix("parquet_lifecycle_success"),
                slo_name=with_prefix("parquet_lifecycle_success"),
                environment=environment,
                service=service,
                value=parquet_lifecycle_success,
                target=parquet_lifecycle_success_target,
                unit="ratio",
                healthy=lifecycle_healthy,
                detail="daily parquet lifecycle task success ratio",
            )
        )

    overall_healthy = all(item.healthy for item in slis)
    return OpsHealthReport(
        generated_ts_ns=now_ns,
        scope=scope,
        overall_healthy=overall_healthy,
        slis=tuple(slis),
        metadata={
            **_normalize_labels(metadata),
            "environment": environment,
            "service": service,
        },
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
                "slo_name": item.slo_name,
                "environment": item.environment,
                "service": item.service,
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
