from __future__ import annotations

import socket
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, TypeAlias

RespValue: TypeAlias = str | int | None | list["RespValue"]


class RedisHashClient(Protocol):
    def ping(self) -> bool: ...

    def hset(self, key: str, fields: Mapping[str, str]) -> bool: ...

    def hgetall(self, key: str) -> dict[str, str]: ...


@dataclass
class InMemoryRedisHashClient(RedisHashClient):
    storage: dict[str, dict[str, str]] = field(default_factory=dict)

    def ping(self) -> bool:
        return True

    def hset(self, key: str, fields: Mapping[str, str]) -> bool:
        if key not in self.storage:
            self.storage[key] = {}
        for field_name, value in fields.items():
            self.storage[key][field_name] = value
        return True

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.storage.get(key, {}))


@dataclass
class TcpRedisHashClient(RedisHashClient):
    host: str
    port: int
    username: str = ""
    password: str = ""
    connect_timeout_s: float = 1.0
    read_timeout_s: float = 1.0

    def ping(self) -> bool:
        response = self._run_command(["PING"])
        return response == "PONG"

    def hset(self, key: str, fields: Mapping[str, str]) -> bool:
        command: list[str] = ["HSET", key]
        for field_name, value in fields.items():
            command.extend([field_name, value])
        response = self._run_command(command)
        return isinstance(response, int) and response >= 0

    def hgetall(self, key: str) -> dict[str, str]:
        response = self._run_command(["HGETALL", key])
        if not isinstance(response, list):
            raise RuntimeError("redis HGETALL did not return an array response")
        if len(response) % 2 != 0:
            raise RuntimeError("redis HGETALL returned odd number of elements")
        output: dict[str, str] = {}
        index = 0
        while index < len(response):
            field = response[index]
            value = response[index + 1]
            if not isinstance(field, str) or not isinstance(value, str):
                raise RuntimeError("redis HGETALL array contains non-string entry")
            output[field] = value
            index += 2
        return output

    def _run_command(self, command: Sequence[str]) -> RespValue:
        with socket.create_connection(
            (self.host, self.port), timeout=self.connect_timeout_s
        ) as conn:
            conn.settimeout(self.read_timeout_s)
            if self.password:
                if self.username:
                    auth = self._send_and_receive(conn, ["AUTH", self.username, self.password])
                else:
                    auth = self._send_and_receive(conn, ["AUTH", self.password])
                if auth != "OK":
                    raise RuntimeError("redis AUTH failed")
            return self._send_and_receive(conn, command)

    def _send_and_receive(self, conn: socket.socket, command: Sequence[str]) -> RespValue:
        conn.sendall(_encode_command(command))
        return _read_resp(conn)


def _encode_command(parts: Sequence[str]) -> bytes:
    output = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        payload = part.encode()
        output.append(f"${len(payload)}\r\n".encode())
        output.append(payload)
        output.append(b"\r\n")
    return b"".join(output)


def _read_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("unexpected EOF while reading redis response")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_line(sock: socket.socket) -> bytes:
    data = bytearray()
    while True:
        ch = sock.recv(1)
        if not ch:
            raise RuntimeError("unexpected EOF while reading redis line")
        data.extend(ch)
        if len(data) >= 2 and data[-2:] == b"\r\n":
            return bytes(data[:-2])


def _read_resp(sock: socket.socket) -> RespValue:
    prefix = _read_exact(sock, 1)
    if prefix == b"+":
        return _read_line(sock).decode("utf-8")
    if prefix == b"-":
        message = _read_line(sock).decode("utf-8")
        raise RuntimeError(f"redis error: {message}")
    if prefix == b":":
        return int(_read_line(sock).decode("utf-8"))
    if prefix == b"$":
        length = int(_read_line(sock).decode("utf-8"))
        if length < 0:
            return None
        payload = _read_exact(sock, length)
        trailer = _read_exact(sock, 2)
        if trailer != b"\r\n":
            raise RuntimeError("invalid bulk string trailer")
        return payload.decode("utf-8")
    if prefix == b"*":
        length = int(_read_line(sock).decode("utf-8"))
        if length < 0:
            return None
        items: list[RespValue] = []
        for _ in range(length):
            items.append(_read_resp(sock))
        return items
    raise RuntimeError(f"unknown redis RESP prefix: {prefix!r}")
