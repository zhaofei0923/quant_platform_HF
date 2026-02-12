from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from quant_hft.contracts import OrderEvent


class TradeRecorder(Protocol):
    def record_order_event(self, event: OrderEvent) -> None: ...

    def list_order_events(self) -> list[OrderEvent]: ...


@dataclass
class InMemoryTradeRecorder(TradeRecorder):
    events: list[OrderEvent] = field(default_factory=list)

    def record_order_event(self, event: OrderEvent) -> None:
        self.events.append(event)

    def list_order_events(self) -> list[OrderEvent]:
        return list(self.events)


def _is_valid_identifier(name: str) -> bool:
    if not name:
        return False
    if not (name[0].isalpha() or name[0] == "_"):
        return False
    return all(ch.isalnum() or ch == "_" for ch in name)


@dataclass
class PostgresTradeRecorder(TradeRecorder):
    dsn: str
    schema: str = "trading_core"
    table: str = "order_events"
    connection_factory: Callable[[str], Any] | None = None
    _conn: Any | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if not _is_valid_identifier(self.schema):
            raise ValueError(f"invalid schema: {self.schema}")
        if not _is_valid_identifier(self.table):
            raise ValueError(f"invalid table: {self.table}")

    def _connect(self) -> Any:
        if self._conn is not None:
            return self._conn
        if self.connection_factory is not None:
            self._conn = self.connection_factory(self.dsn)
            return self._conn

        try:
            import psycopg  # type: ignore

            self._conn = psycopg.connect(self.dsn)
            return self._conn
        except ModuleNotFoundError:
            pass
        try:
            import psycopg2  # type: ignore

            self._conn = psycopg2.connect(self.dsn)
            return self._conn
        except ModuleNotFoundError as exc:
            raise RuntimeError("psycopg/psycopg2 is required for PostgresTradeRecorder") from exc

    @property
    def _qualified_table(self) -> str:
        return f"{self.schema}.{self.table}"

    def record_order_event(self, event: OrderEvent) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._qualified_table} (
                    account_id,
                    client_order_id,
                    exchange_order_id,
                    instrument_id,
                    exchange_id,
                    status,
                    total_volume,
                    filled_volume,
                    avg_fill_price,
                    reason,
                    status_msg,
                    order_submit_status,
                    order_ref,
                    front_id,
                    session_id,
                    trade_id,
                    event_source,
                    exchange_ts_ns,
                    recv_ts_ns,
                    ts_ns,
                    trace_id,
                    execution_algo_id,
                    slice_index,
                    slice_total,
                    throttle_applied,
                    venue,
                    route_id,
                    slippage_bps,
                    impact_cost
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                (
                    event.account_id,
                    event.client_order_id,
                    event.exchange_order_id,
                    event.instrument_id,
                    event.exchange_id,
                    event.status,
                    event.total_volume,
                    event.filled_volume,
                    event.avg_fill_price,
                    event.reason,
                    event.status_msg,
                    event.order_submit_status,
                    event.order_ref,
                    event.front_id,
                    event.session_id,
                    event.trade_id,
                    event.event_source,
                    event.exchange_ts_ns,
                    event.recv_ts_ns,
                    event.ts_ns,
                    event.trace_id,
                    event.execution_algo_id,
                    event.slice_index,
                    event.slice_total,
                    1 if event.throttle_applied else 0,
                    event.venue,
                    event.route_id,
                    event.slippage_bps,
                    event.impact_cost,
                ),
            )
        conn.commit()

    def list_order_events(self) -> list[OrderEvent]:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    account_id, client_order_id, exchange_order_id, instrument_id, exchange_id,
                    status, total_volume, filled_volume, avg_fill_price, reason,
                    status_msg, order_submit_status, order_ref, front_id, session_id,
                    trade_id, event_source, exchange_ts_ns, recv_ts_ns, ts_ns, trace_id,
                    execution_algo_id, slice_index, slice_total, throttle_applied,
                    venue, route_id, slippage_bps, impact_cost
                FROM {self._qualified_table}
                ORDER BY ts_ns ASC
                """)
            rows = list(cur.fetchall())
        return [
            OrderEvent(
                account_id=str(row[0]),
                client_order_id=str(row[1]),
                exchange_order_id=str(row[2]),
                instrument_id=str(row[3]),
                exchange_id=str(row[4]),
                status=str(row[5]),
                total_volume=int(row[6]),
                filled_volume=int(row[7]),
                avg_fill_price=float(row[8]),
                reason=str(row[9]),
                status_msg=str(row[10]),
                order_submit_status=str(row[11]),
                order_ref=str(row[12]),
                front_id=int(row[13]),
                session_id=int(row[14]),
                trade_id=str(row[15]),
                event_source=str(row[16]),
                exchange_ts_ns=int(row[17]),
                recv_ts_ns=int(row[18]),
                ts_ns=int(row[19]),
                trace_id=str(row[20]),
                execution_algo_id=str(row[21]),
                slice_index=int(row[22]),
                slice_total=int(row[23]),
                throttle_applied=int(row[24]) > 0,
                venue=str(row[25]),
                route_id=str(row[26]),
                slippage_bps=float(row[27]),
                impact_cost=float(row[28]),
            )
            for row in rows
        ]

    def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None
