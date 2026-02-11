#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path


def _extract_block(text: str, start_marker: str) -> str:
    start = text.find(start_marker)
    if start < 0:
        raise ValueError(f"missing block marker: {start_marker}")
    open_pos = text.find("{", start)
    if open_pos < 0:
        raise ValueError(f"missing opening brace for: {start_marker}")
    depth = 0
    for idx in range(open_pos, len(text)):
        char = text[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:idx]
    raise ValueError(f"missing block terminator for: {start_marker}")


def _parse_cpp_struct_fields(path: Path, struct_name: str) -> list[str]:
    text = path.read_text(encoding="utf-8")
    block = _extract_block(text, f"struct {struct_name} {{")
    fields: list[str] = []
    for raw_line in block.splitlines():
        line = raw_line.split("//", maxsplit=1)[0].strip()
        if not line or "(" in line or line.startswith("struct "):
            continue
        if not line.endswith(";"):
            continue
        match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:\{[^;]*\})?\s*;$", line)
        if match is None:
            continue
        fields.append(match.group(1))
    return fields


def _parse_proto_message_fields(path: Path, message_name: str) -> list[str]:
    text = path.read_text(encoding="utf-8")
    block = _extract_block(text, f"message {message_name} {{")
    fields: list[str] = []
    for raw_line in block.splitlines():
        line = raw_line.split("//", maxsplit=1)[0].strip()
        if not line or line.startswith("message "):
            continue
        match = re.search(r"[A-Za-z0-9_.<>]+\s+([A-Za-z_][A-Za-z0-9_]*)\s*=", line)
        if match is None:
            continue
        fields.append(match.group(1))
    return fields


def _parse_python_dataclass_fields(path: Path, class_name: str) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    class_start = -1
    for idx, raw_line in enumerate(lines):
        if raw_line.strip() == f"class {class_name}:":
            class_start = idx + 1
            break
    if class_start < 0:
        raise ValueError(f"missing dataclass: {class_name}")

    fields: list[str] = []
    for raw_line in lines[class_start:]:
        if raw_line and not raw_line.startswith(" "):
            break
        line = raw_line.strip()
        if not line:
            continue
        match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        if match is None:
            continue
        fields.append(match.group(1))
    return fields


def _assert_equal(actual: list[str], expected: list[str], contract_name: str, source: str) -> None:
    if set(actual) != set(expected):
        raise ValueError(
            f"{contract_name} mismatch in {source}: expected={expected!r} actual={actual!r}"
        )


def main() -> int:
    cpp_path = Path("include/quant_hft/contracts/types.h")
    proto_path = Path("proto/quant_hft/v1/contracts.proto")
    py_path = Path("python/quant_hft/contracts.py")

    risk_expected = [
        "action",
        "rule_id",
        "rule_group",
        "rule_version",
        "policy_id",
        "policy_scope",
        "observed_value",
        "threshold_value",
        "decision_tags",
        "reason",
        "decision_ts_ns",
    ]
    order_expected = [
        "account_id",
        "client_order_id",
        "instrument_id",
        "status",
        "total_volume",
        "filled_volume",
        "avg_fill_price",
        "reason",
        "ts_ns",
        "trace_id",
        "exchange_order_id",
        "execution_algo_id",
        "slice_index",
        "slice_total",
        "throttle_applied",
    ]

    cpp_risk = _parse_cpp_struct_fields(cpp_path, "RiskDecision")
    proto_risk = _parse_proto_message_fields(proto_path, "RiskDecision")
    py_risk = _parse_python_dataclass_fields(py_path, "RiskDecision")
    _assert_equal(cpp_risk, risk_expected, "RiskDecision", "C++")
    _assert_equal(proto_risk, risk_expected, "RiskDecision", "proto")
    _assert_equal(py_risk, risk_expected, "RiskDecision", "Python")

    cpp_order = _parse_cpp_struct_fields(cpp_path, "OrderEvent")
    proto_order = _parse_proto_message_fields(proto_path, "OrderEvent")
    py_order = _parse_python_dataclass_fields(py_path, "OrderEvent")
    _assert_equal(cpp_order, order_expected, "OrderEvent", "C++")
    _assert_equal(proto_order, order_expected, "OrderEvent", "proto")
    _assert_equal(py_order, order_expected, "OrderEvent", "Python")

    print("contract sync verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
