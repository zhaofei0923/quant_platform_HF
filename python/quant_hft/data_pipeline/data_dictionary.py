from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class FieldRule:
    name: str
    required: bool
    value_type: str


@dataclass(frozen=True)
class SchemaDiff:
    schema_name: str
    missing_required: tuple[str, ...]
    unexpected_fields: tuple[str, ...]


@dataclass(frozen=True)
class SchemaMetadata:
    version: str
    migration_strategy: str


class DataDictionary:
    """Repository-local data dictionary for Redis/Timescale contracts."""

    def __init__(self) -> None:
        self._schemas: dict[str, tuple[FieldRule, ...]] = {
            "redis_order_event": (
                FieldRule("account_id", True, "str"),
                FieldRule("client_order_id", True, "str"),
                FieldRule("instrument_id", True, "str"),
                FieldRule("status", True, "str"),
                FieldRule("total_volume", True, "int"),
                FieldRule("filled_volume", True, "int"),
                FieldRule("avg_fill_price", True, "float"),
                FieldRule("reason", True, "str"),
                FieldRule("ts_ns", True, "int"),
                FieldRule("trace_id", False, "str"),
                FieldRule("exchange_order_id", False, "str"),
                FieldRule("execution_algo_id", False, "str"),
                FieldRule("slice_index", False, "int"),
                FieldRule("slice_total", False, "int"),
                FieldRule("throttle_applied", False, "bool"),
                FieldRule("venue", False, "str"),
                FieldRule("route_id", False, "str"),
                FieldRule("slippage_bps", False, "float"),
                FieldRule("impact_cost", False, "float"),
            ),
            "timescale_order_event": (
                FieldRule("account_id", True, "str"),
                FieldRule("client_order_id", True, "str"),
                FieldRule("instrument_id", True, "str"),
                FieldRule("status", True, "str"),
                FieldRule("total_volume", True, "int"),
                FieldRule("filled_volume", True, "int"),
                FieldRule("avg_fill_price", True, "float"),
                FieldRule("reason", True, "str"),
                FieldRule("ts_ns", True, "int"),
                FieldRule("trace_id", True, "str"),
                FieldRule("exchange_order_id", False, "str"),
                FieldRule("execution_algo_id", False, "str"),
                FieldRule("slice_index", False, "int"),
                FieldRule("slice_total", False, "int"),
                FieldRule("throttle_applied", False, "bool"),
                FieldRule("venue", False, "str"),
                FieldRule("route_id", False, "str"),
                FieldRule("slippage_bps", False, "float"),
                FieldRule("impact_cost", False, "float"),
            ),
            "redis_state_snapshot": (
                FieldRule("instrument_id", True, "str"),
                FieldRule("trend_score", True, "float"),
                FieldRule("trend_confidence", True, "float"),
                FieldRule("volatility_score", True, "float"),
                FieldRule("volatility_confidence", True, "float"),
                FieldRule("liquidity_score", True, "float"),
                FieldRule("liquidity_confidence", True, "float"),
                FieldRule("sentiment_score", True, "float"),
                FieldRule("sentiment_confidence", True, "float"),
                FieldRule("seasonality_score", True, "float"),
                FieldRule("seasonality_confidence", True, "float"),
                FieldRule("pattern_score", True, "float"),
                FieldRule("pattern_confidence", True, "float"),
                FieldRule("event_drive_score", True, "float"),
                FieldRule("event_drive_confidence", True, "float"),
                FieldRule("ts_ns", True, "int"),
            ),
        }
        self._schema_metadata: dict[str, SchemaMetadata] = {
            schema_name: SchemaMetadata(
                version="v1",
                migration_strategy="additive_backward_compatible",
            )
            for schema_name in self._schemas
        }

    def schema_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._schemas))

    def required_fields(self, schema_name: str) -> tuple[str, ...]:
        schema = self._schema(schema_name)
        return tuple(item.name for item in schema if item.required)

    def all_fields(self, schema_name: str) -> tuple[str, ...]:
        schema = self._schema(schema_name)
        return tuple(item.name for item in schema)

    def validate(self, schema_name: str, record: Mapping[str, object]) -> list[str]:
        schema = self._schema(schema_name)
        errors: list[str] = []
        for rule in schema:
            if rule.name not in record:
                if rule.required:
                    errors.append(f"missing required field: {rule.name}")
                continue
            if not self._matches_type(record[rule.name], rule.value_type):
                actual = type(record[rule.name]).__name__
                errors.append(f"field {rule.name} expected {rule.value_type}, got {actual}")
        return errors

    def diff_record(self, schema_name: str, record: Mapping[str, object]) -> SchemaDiff:
        schema = self._schema(schema_name)
        expected_fields = {item.name for item in schema}
        required_fields = {item.name for item in schema if item.required}

        missing_required = tuple(sorted(name for name in required_fields if name not in record))
        unexpected = tuple(sorted(name for name in record if name not in expected_fields))
        return SchemaDiff(
            schema_name=schema_name,
            missing_required=missing_required,
            unexpected_fields=unexpected,
        )

    def validate_schema_alignment(self, left_schema: str, right_schema: str) -> tuple[str, ...]:
        left_required = set(self.required_fields(left_schema))
        right_required = set(self.required_fields(right_schema))
        missing = sorted(left_required - right_required)
        return tuple(missing)

    def schema_version(self, schema_name: str) -> str:
        self._schema(schema_name)
        return self._schema_meta(schema_name).version

    def migration_strategy(self, schema_name: str) -> str:
        self._schema(schema_name)
        return self._schema_meta(schema_name).migration_strategy

    def _schema(self, schema_name: str) -> tuple[FieldRule, ...]:
        schema = self._schemas.get(schema_name)
        if schema is None:
            allowed = ", ".join(sorted(self._schemas))
            raise ValueError(f"unknown schema: {schema_name}; allowed schemas: {allowed}")
        return schema

    def _schema_meta(self, schema_name: str) -> SchemaMetadata:
        metadata = self._schema_metadata.get(schema_name)
        if metadata is None:
            allowed = ", ".join(sorted(self._schema_metadata))
            raise ValueError(f"unknown schema metadata: {schema_name}; allowed schemas: {allowed}")
        return metadata

    @staticmethod
    def _matches_type(value: object, value_type: str) -> bool:
        if value_type == "str":
            return isinstance(value, str)
        if value_type == "int":
            return isinstance(value, int) and not isinstance(value, bool)
        if value_type == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if value_type == "bool":
            if isinstance(value, bool):
                return True
            if isinstance(value, str):
                return value.strip().lower() in {"0", "1", "true", "false", "yes", "no"}
            return False
        return False
