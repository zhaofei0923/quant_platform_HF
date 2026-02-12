from __future__ import annotations

from dataclasses import dataclass, field

from quant_hft.contracts import OrderEvent
from quant_hft.runtime.trade_recorder import InMemoryTradeRecorder, PostgresTradeRecorder


def _build_event() -> OrderEvent:
    return OrderEvent(
        account_id="acc-1",
        client_order_id="ord-1",
        instrument_id="SHFE.ag2406",
        status="FILLED",
        total_volume=2,
        filled_volume=2,
        avg_fill_price=4500.5,
        reason="ok",
        ts_ns=100,
        trace_id="trace-1",
        exchange_ts_ns=99,
        recv_ts_ns=100,
    )


def test_inmemory_trade_recorder_round_trip() -> None:
    recorder = InMemoryTradeRecorder()
    event = _build_event()
    recorder.record_order_event(event)
    assert recorder.list_order_events() == [event]


@dataclass
class _FakeCursor:
    rows: list[tuple[object, ...]] = field(default_factory=list)
    statements: list[str] = field(default_factory=list)

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.statements.append(sql)
        self._last_params = params

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self.rows)


@dataclass
class _FakeConnection:
    cursor_obj: _FakeCursor
    commits: int = 0
    closed: bool = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


def test_postgres_trade_recorder_record_and_list_with_fake_connection() -> None:
    rows = [
        (
            "acc-1",
            "ord-1",
            "",
            "SHFE.ag2406",
            "",
            "FILLED",
            2,
            2,
            4500.5,
            "ok",
            "",
            "",
            "",
            0,
            0,
            "",
            "",
            99,
            100,
            100,
            "trace-1",
            "",
            0,
            0,
            0,
            "",
            "",
            0.0,
            0.0,
        )
    ]
    fake_cursor = _FakeCursor(rows=rows)
    fake_conn = _FakeConnection(cursor_obj=fake_cursor)
    recorder = PostgresTradeRecorder(
        dsn="postgres://unused",
        connection_factory=lambda _: fake_conn,
    )

    recorder.record_order_event(_build_event())
    assert fake_conn.commits == 1
    assert any("INSERT INTO" in statement for statement in fake_cursor.statements)

    events = recorder.list_order_events()
    assert len(events) == 1
    assert events[0].client_order_id == "ord-1"
    assert events[0].filled_volume == 2

    recorder.close()
    assert fake_conn.closed is True
