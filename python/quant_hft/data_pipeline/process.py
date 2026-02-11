from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from quant_hft.data_pipeline.adapters import DuckDbAnalyticsStore, MinioArchiveStore
from quant_hft.data_pipeline.data_dictionary import DataDictionary
from quant_hft.ops.monitoring import InMemoryObservability


@dataclass(frozen=True)
class ArchiveConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    use_ssl: bool = False
    local_fallback_dir: Path | str | None = None


@dataclass(frozen=True)
class DataPipelineConfig:
    analytics_db_path: Path | str
    export_dir: Path | str
    archive: ArchiveConfig
    prefer_duckdb: bool = True
    interval_seconds: float = 60.0
    archive_prefix: str = "etl"
    export_tables: tuple[str, ...] = ("market_snapshots", "order_events")
    max_run_latency_ms: float = 2_000.0
    min_total_export_rows: int = 1
    min_archived_objects: int = 1
    validate_data_dictionary: bool = True


@dataclass(frozen=True)
class PipelineRunReport:
    run_id: str
    trace_id: str
    exported_rows: dict[str, int]
    exported_files: dict[str, Path]
    run_latency_ms: float
    archived_objects_count: int
    alert_codes: tuple[str, ...]
    data_dictionary_violations: tuple[str, ...]
    manifest_path: Path


class DataPipelineProcess:
    def __init__(
        self,
        config: DataPipelineConfig,
        *,
        observability: InMemoryObservability | None = None,
    ) -> None:
        self._config = config
        self._obs = observability
        self._store = DuckDbAnalyticsStore(
            db_path=config.analytics_db_path, prefer_duckdb=config.prefer_duckdb
        )
        self._archive = MinioArchiveStore(
            endpoint=config.archive.endpoint,
            access_key=config.archive.access_key,
            secret_key=config.archive.secret_key,
            bucket=config.archive.bucket,
            use_ssl=config.archive.use_ssl,
            local_fallback_dir=config.archive.local_fallback_dir,
        )
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._store.close()
        self._closed = True

    def run_once(self) -> PipelineRunReport:
        if self._closed:
            raise RuntimeError("process is already closed")

        started_perf_ns = time.perf_counter_ns()
        ts = datetime.now(timezone.utc)
        run_id = ts.strftime("%Y%m%dT%H%M%S%fZ")
        trace_id = run_id
        export_root = Path(self._config.export_dir) / run_id
        export_root.mkdir(parents=True, exist_ok=True)

        exported_rows: dict[str, int] = {}
        exported_files: dict[str, Path] = {}
        archived_names: list[str] = []
        run_latency_ms = 0.0
        alert_codes: tuple[str, ...] = ()
        data_dictionary_violations: tuple[str, ...] = ()
        root_span = None
        if self._obs is not None:
            root_span = self._obs.start_span(
                "data_pipeline.run_once",
                trace_id=trace_id,
                attributes={"run_id": run_id},
            )

        try:
            object_base = f"{self._config.archive_prefix.strip('/')}/{run_id}".strip("/")
            for table in self._config.export_tables:
                table_span = None
                if self._obs is not None:
                    table_span = self._obs.start_span(
                        "data_pipeline.export_table",
                        trace_id=trace_id,
                        parent_span_id=root_span.span_id if root_span is not None else None,
                        attributes={"table": table},
                    )

                table_started_ns = time.perf_counter_ns()
                output = export_root / f"{table}.csv"
                rows = self._store.export_table_to_csv(table, output)
                exported_rows[table] = rows
                exported_files[table] = output

                object_name = f"{object_base}/{table}.csv"
                self._archive.put_file(object_name, output)
                archived_names.append(object_name)

                table_latency_ms = (time.perf_counter_ns() - table_started_ns) / 1_000_000.0
                if self._obs is not None:
                    self._obs.record_metric(
                        "quant_hft_data_pipeline_table_export_rows",
                        float(rows),
                        kind="gauge",
                        labels={"table": table},
                    )
                    self._obs.record_metric(
                        "quant_hft_data_pipeline_table_export_latency_ms",
                        table_latency_ms,
                        kind="histogram",
                        labels={"table": table},
                    )
                if table_span is not None:
                    table_span.end(
                        {
                            "rows": str(rows),
                            "archive_object": object_name,
                        }
                    )

            manifest_path = export_root / "manifest.json"
            run_latency_ms = (time.perf_counter_ns() - started_perf_ns) / 1_000_000.0
            data_dictionary_violations = self._validate_data_dictionary()
            alert_codes = self._evaluate_alerts(
                total_export_rows=sum(exported_rows.values()),
                archived_objects_count=len(archived_names) + 1,
                run_latency_ms=run_latency_ms,
                has_data_dictionary_violation=bool(data_dictionary_violations),
            )
            manifest = {
                "run_id": run_id,
                "trace_id": trace_id,
                "generated_at": ts.isoformat(),
                "exported_rows": exported_rows,
                "exported_files": {table: str(path) for table, path in exported_files.items()},
                "run_latency_ms": run_latency_ms,
                "alert_codes": list(alert_codes),
                "data_dictionary_violations": list(data_dictionary_violations),
                "archive_prefix": object_base,
                "archive_bucket": self._config.archive.bucket,
                "archive_mode": self._archive.mode,
                "archive_objects": archived_names + [f"{object_base}/manifest.json"],
            }
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )

            manifest_object = f"{object_base}/manifest.json"
            self._archive.put_file(manifest_object, manifest_path)
            archived_names.append(manifest_object)

            self._record_observability(
                run_id=run_id,
                trace_id=trace_id,
                run_latency_ms=run_latency_ms,
                exported_rows=exported_rows,
                archived_objects_count=len(archived_names),
                alert_codes=alert_codes,
                status="ok",
            )
            if root_span is not None:
                root_span.end(
                    {
                        "status": "ok",
                        "run_latency_ms": f"{run_latency_ms:.6f}",
                        "archived_objects_count": str(len(archived_names)),
                        "data_dictionary_violations": str(len(data_dictionary_violations)),
                    }
                )
        except Exception as exc:
            self._record_observability(
                run_id=run_id,
                trace_id=trace_id,
                run_latency_ms=(time.perf_counter_ns() - started_perf_ns) / 1_000_000.0,
                exported_rows=exported_rows,
                archived_objects_count=len(archived_names),
                alert_codes=("PIPELINE_RUN_FAILED",),
                status="error",
            )
            if self._obs is not None:
                self._obs.emit_alert(
                    code="PIPELINE_RUN_FAILED",
                    severity="critical",
                    message=str(exc),
                    labels={"run_id": run_id},
                )
            if root_span is not None:
                root_span.end({"status": "error", "error": str(exc)})
            raise

        return PipelineRunReport(
            run_id=run_id,
            trace_id=trace_id,
            exported_rows=exported_rows,
            exported_files=exported_files,
            run_latency_ms=run_latency_ms,
            archived_objects_count=len(archived_names),
            alert_codes=alert_codes,
            data_dictionary_violations=data_dictionary_violations,
            manifest_path=manifest_path,
        )

    def run_loop(self, *, max_iterations: int | None = None) -> list[PipelineRunReport]:
        reports: list[PipelineRunReport] = []
        iteration = 0
        while True:
            reports.append(self.run_once())
            iteration += 1
            if max_iterations is not None and iteration >= max_iterations:
                break
            if self._config.interval_seconds > 0:
                time.sleep(self._config.interval_seconds)
        return reports

    @property
    def config_dict(self) -> dict[str, object]:
        payload = asdict(self._config)
        payload["analytics_db_path"] = str(Path(self._config.analytics_db_path))
        payload["export_dir"] = str(Path(self._config.export_dir))
        archive = payload.get("archive")
        if isinstance(archive, dict):
            local_dir = archive.get("local_fallback_dir")
            if local_dir is not None:
                archive["local_fallback_dir"] = str(Path(local_dir))
        return payload

    def _evaluate_alerts(
        self,
        *,
        total_export_rows: int,
        archived_objects_count: int,
        run_latency_ms: float,
        has_data_dictionary_violation: bool,
    ) -> tuple[str, ...]:
        alerts: list[str] = []
        if total_export_rows < self._config.min_total_export_rows:
            alerts.append("PIPELINE_EXPORT_EMPTY")
        if archived_objects_count < self._config.min_archived_objects:
            alerts.append("PIPELINE_ARCHIVE_INCOMPLETE")
        if run_latency_ms > self._config.max_run_latency_ms:
            alerts.append("PIPELINE_RUN_SLOW")
        if has_data_dictionary_violation:
            alerts.append("PIPELINE_DATA_DICTIONARY_VIOLATION")
        return tuple(alerts)

    def _validate_data_dictionary(self) -> tuple[str, ...]:
        if not self._config.validate_data_dictionary:
            return ()
        if "order_events" not in self._config.export_tables:
            return ()

        dictionary = DataDictionary()
        issues: list[str] = []
        for index, row in enumerate(self._store.read_table_as_dicts("order_events", limit=2000)):
            for issue in dictionary.validate("timescale_order_event", row):
                issues.append(f"order_events[{index}] {issue}")
        return tuple(issues)

    def _record_observability(
        self,
        *,
        run_id: str,
        trace_id: str,
        run_latency_ms: float,
        exported_rows: Mapping[str, int],
        archived_objects_count: int,
        alert_codes: tuple[str, ...],
        status: str,
    ) -> None:
        if self._obs is None:
            return
        self._obs.record_metric(
            "quant_hft_data_pipeline_runs_total",
            1.0,
            kind="counter",
            labels={"status": status},
        )
        self._obs.record_metric(
            "quant_hft_data_pipeline_run_latency_ms",
            run_latency_ms,
            kind="histogram",
            labels={"status": status},
        )
        self._obs.record_metric(
            "quant_hft_data_pipeline_archived_objects",
            float(archived_objects_count),
            kind="gauge",
            labels={"run_id": run_id},
        )
        self._obs.record_metric(
            "quant_hft_data_pipeline_export_rows_total",
            float(sum(exported_rows.values())),
            kind="gauge",
            labels={"run_id": run_id},
        )
        for code in alert_codes:
            self._obs.emit_alert(
                code=code,
                severity="warning",
                message=f"pipeline alert triggered: {code}",
                labels={"run_id": run_id, "trace_id": trace_id},
            )
