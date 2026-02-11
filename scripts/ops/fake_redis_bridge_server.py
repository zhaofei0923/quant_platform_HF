#!/usr/bin/env python3
from __future__ import annotations

import argparse
import socket
import socketserver
from collections.abc import Sequence


def _build_fixture() -> dict[str, dict[str, str]]:
    return {
        "market:state7d:SHFE.ag2406:latest": {
            "instrument_id": "SHFE.ag2406",
            "trend_score": "0.1",
            "trend_confidence": "0.9",
            "volatility_score": "0.2",
            "volatility_confidence": "0.9",
            "liquidity_score": "0.3",
            "liquidity_confidence": "0.9",
            "sentiment_score": "0.4",
            "sentiment_confidence": "0.9",
            "seasonality_score": "0.5",
            "seasonality_confidence": "0.9",
            "pattern_score": "0.6",
            "pattern_confidence": "0.9",
            "event_drive_score": "0.7",
            "event_drive_confidence": "0.9",
            "ts_ns": "1700000000000000000",
        },
        "market:state7d:SHFE.rb2405:latest": {
            "instrument_id": "SHFE.rb2405",
            "trend_score": "0.1",
            "trend_confidence": "0.9",
            "volatility_score": "0.2",
            "volatility_confidence": "0.9",
            "liquidity_score": "0.3",
            "liquidity_confidence": "0.9",
            "sentiment_score": "0.4",
            "sentiment_confidence": "0.9",
            "seasonality_score": "0.5",
            "seasonality_confidence": "0.9",
            "pattern_score": "0.6",
            "pattern_confidence": "0.9",
            "event_drive_score": "0.7",
            "event_drive_confidence": "0.9",
            "ts_ns": "1700000000000000100",
        },
        "strategy:intent:demo:latest": {
            "seq": "1",
            "count": "1",
            "intent_0": "SHFE.ag2406|BUY|OPEN|1|4500|1700000000000000200|trace-1",
            "ts_ns": "1700000000000000200",
        },
        "trade:order:trace-1:info": {
            "account_id": "sim-account",
            "client_order_id": "trace-1",
            "instrument_id": "SHFE.ag2406",
            "status": "ACCEPTED",
            "total_volume": "1",
            "filled_volume": "0",
            "avg_fill_price": "0",
            "reason": "ok",
            "ts_ns": "1700000000000000300",
            "trace_id": "trace-1",
        },
    }


FIXTURE = _build_fixture()


def _read_line(fileobj) -> bytes:
    line = fileobj.readline()
    if not line:
        raise EOFError
    if not line.endswith(b"\r\n"):
        raise RuntimeError("invalid RESP line ending")
    return line[:-2]


def _read_bulk_string(fileobj) -> str:
    length_line = _read_line(fileobj)
    if not length_line.startswith(b"$"):
        raise RuntimeError("expected bulk string")
    length = int(length_line[1:].decode("utf-8"))
    data = fileobj.read(length)
    trailer = fileobj.read(2)
    if trailer != b"\r\n":
        raise RuntimeError("invalid bulk trailer")
    return data.decode("utf-8")


def _read_command(fileobj) -> list[str]:
    first = _read_line(fileobj)
    if not first.startswith(b"*"):
        raise RuntimeError("expected array")
    count = int(first[1:].decode("utf-8"))
    parts: list[str] = []
    for _ in range(count):
        parts.append(_read_bulk_string(fileobj))
    return parts


def _encode_simple(value: str) -> bytes:
    return f"+{value}\r\n".encode()


def _encode_error(value: str) -> bytes:
    return f"-ERR {value}\r\n".encode()


def _encode_array(values: Sequence[str]) -> bytes:
    chunks = [f"*{len(values)}\r\n".encode()]
    for value in values:
        raw = value.encode("utf-8")
        chunks.append(f"${len(raw)}\r\n".encode())
        chunks.append(raw)
        chunks.append(b"\r\n")
    return b"".join(chunks)


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        while True:
            try:
                command = _read_command(self.rfile)
            except EOFError:
                return
            except Exception:
                self.wfile.write(_encode_error("protocol"))
                return
            if not command:
                self.wfile.write(_encode_error("empty command"))
                return

            op = command[0].upper()
            if op == "PING":
                self.wfile.write(_encode_simple("PONG"))
                continue
            if op == "HGETALL" and len(command) == 2:
                key = command[1]
                fields = FIXTURE.get(key, {})
                payload: list[str] = []
                for field, value in fields.items():
                    payload.append(field)
                    payload.append(value)
                self.wfile.write(_encode_array(payload))
                continue
            if op == "AUTH":
                self.wfile.write(_encode_simple("OK"))
                continue
            self.wfile.write(_encode_error(f"unsupported command: {op}"))


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fake Redis server for strategy bridge smoke checks"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=16379)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    with _Server((args.host, args.port), _Handler) as server:
        server.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
