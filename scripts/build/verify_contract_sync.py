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

    expected_fields = {
        "MarketSnapshot": [
            "instrument_id",
            "exchange_id",
            "trading_day",
            "action_day",
            "update_time",
            "update_millisec",
            "last_price",
            "bid_price_1",
            "ask_price_1",
            "bid_volume_1",
            "ask_volume_1",
            "volume",
            "settlement_price",
            "average_price_raw",
            "average_price_norm",
            "is_valid_settlement",
            "exchange_ts_ns",
            "recv_ts_ns",
        ],
        "RiskDecision": [
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
        ],
        "OrderEvent": [
            "account_id",
            "client_order_id",
            "exchange_order_id",
            "instrument_id",
            "exchange_id",
            "status",
            "total_volume",
            "filled_volume",
            "avg_fill_price",
            "reason",
            "status_msg",
            "order_submit_status",
            "order_ref",
            "front_id",
            "session_id",
            "trade_id",
            "event_source",
            "exchange_ts_ns",
            "recv_ts_ns",
            "ts_ns",
            "trace_id",
            "execution_algo_id",
            "slice_index",
            "slice_total",
            "throttle_applied",
            "venue",
            "route_id",
            "slippage_bps",
            "impact_cost",
        ],
        "TradingAccountSnapshot": [
            "account_id",
            "investor_id",
            "balance",
            "available",
            "curr_margin",
            "frozen_margin",
            "frozen_cash",
            "frozen_commission",
            "commission",
            "close_profit",
            "position_profit",
            "trading_day",
            "ts_ns",
            "source",
        ],
        "InvestorPositionSnapshot": [
            "account_id",
            "investor_id",
            "instrument_id",
            "exchange_id",
            "posi_direction",
            "hedge_flag",
            "position_date",
            "position",
            "today_position",
            "yd_position",
            "long_frozen",
            "short_frozen",
            "open_volume",
            "close_volume",
            "position_cost",
            "open_cost",
            "position_profit",
            "close_profit",
            "margin_rate_by_money",
            "margin_rate_by_volume",
            "use_margin",
            "ts_ns",
            "source",
        ],
        "BrokerTradingParamsSnapshot": [
            "account_id",
            "investor_id",
            "margin_price_type",
            "algorithm",
            "ts_ns",
            "source",
        ],
        "InstrumentMetaSnapshot": [
            "instrument_id",
            "exchange_id",
            "product_id",
            "volume_multiple",
            "price_tick",
            "max_margin_side_algorithm",
            "ts_ns",
            "source",
        ],
    }

    for contract_name, expected in expected_fields.items():
        cpp_fields = _parse_cpp_struct_fields(cpp_path, contract_name)
        proto_fields = _parse_proto_message_fields(proto_path, contract_name)
        py_fields = _parse_python_dataclass_fields(py_path, contract_name)
        _assert_equal(cpp_fields, expected, contract_name, "C++")
        _assert_equal(proto_fields, expected, contract_name, "proto")
        _assert_equal(py_fields, expected, contract_name, "Python")

    print("contract sync verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
