#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


_COMMISSION_KEYS = (
    "open_ratio_by_money",
    "open_ratio_by_volume",
    "close_ratio_by_money",
    "close_ratio_by_volume",
    "close_today_ratio_by_money",
    "close_today_ratio_by_volume",
)


def _strip_inline_comment(text: str) -> str:
    in_single = False
    in_double = False
    out: list[str] = []
    for ch in text:
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
            continue
        if ch == "#" and not in_single and not in_double:
            break
        out.append(ch)
    return "".join(out)


def _unquote(text: str) -> str:
    if len(text) >= 2 and ((text[0] == '"' and text[-1] == '"') or (text[0] == "'" and text[-1] == "'")):
        return text[1:-1]
    return text


def _parse_scalar(text: str) -> Any:
    raw = text.strip()
    if raw == "":
        return ""
    quoted = len(raw) >= 2 and (
        (raw[0] == '"' and raw[-1] == '"') or (raw[0] == "'" and raw[-1] == "'")
    )
    value = _unquote(raw)
    if quoted:
        return value
    low = value.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    try:
        return float(value)
    except ValueError:
        return value


def _parse_key_value(text: str, line_no: int) -> tuple[str, str]:
    pos = text.find(":")
    if pos < 0:
        raise ValueError(f"line {line_no}: invalid key/value entry")
    key = _unquote(text[:pos].strip())
    value = text[pos + 1 :].strip()
    if key == "":
        raise ValueError(f"line {line_no}: empty key")
    return key, value


def _load_products_yaml(path: Path) -> dict[str, dict[str, Any]]:
    products: dict[str, dict[str, Any]] = {}
    in_products = False
    current_product: str | None = None
    section: str | None = None

    with path.open("r", encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            cleaned = _strip_inline_comment(line.rstrip("\n"))
            if cleaned.strip() == "":
                continue
            indent = len(cleaned) - len(cleaned.lstrip(" "))
            text = cleaned.strip()

            if text.startswith("-"):
                if section == "trading_sessions" and indent >= 6 and current_product is not None:
                    item = text[1:].strip()
                    products[current_product]["trading_sessions"].append(_unquote(item))
                    continue
                raise ValueError(f"line {line_no}: unexpected list item")

            key, value = _parse_key_value(text, line_no)
            if indent == 0:
                if key != "products" or value != "":
                    raise ValueError("line 1: products section is required")
                in_products = True
                continue
            if not in_products:
                raise ValueError(f"line {line_no}: products section is required")
            if indent == 2:
                if value != "":
                    raise ValueError(f"line {line_no}: instrument entry must be a YAML section")
                current_product = key
                products[current_product] = {"commission": {}, "trading_sessions": []}
                section = None
                continue
            if current_product is None:
                raise ValueError(f"line {line_no}: field appears before instrument section")

            if indent == 4:
                if key == "commission":
                    if value != "":
                        raise ValueError(f"line {line_no}: commission must be a YAML section")
                    section = "commission"
                    continue
                if key == "trading_sessions":
                    if value != "":
                        raise ValueError(f"line {line_no}: trading_sessions must be a YAML section")
                    section = "trading_sessions"
                    continue
                section = None
                products[current_product][key] = _parse_scalar(value)
                continue

            if indent == 6 and section == "commission":
                products[current_product]["commission"][key] = _parse_scalar(value)
                continue

            raise ValueError(f"line {line_no}: unsupported indentation level")

    if not in_products:
        raise ValueError("line 1: products section is required")
    return products


def _to_float(value: Any, context: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{context} must be numeric") from exc


def _normalize_entry(product_id: str, raw: dict[str, Any], source: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{source} `{product_id}` must be object")
    commission = raw.get("commission")
    if not isinstance(commission, dict):
        raise ValueError(f"{source} `{product_id}` missing commission object")

    sessions = raw.get("trading_sessions", [])
    if not isinstance(sessions, list):
        raise ValueError(f"{source} `{product_id}` trading_sessions must be list")

    normalized = {
        "product": str(raw.get("product", product_id)),
        "volume_multiple": _to_float(
            raw.get("volume_multiple", raw.get("contract_multiplier")), f"{source} `{product_id}` volume"
        ),
        "long_margin_ratio": _to_float(
            raw.get("long_margin_ratio"), f"{source} `{product_id}` long_margin_ratio"
        ),
        "short_margin_ratio": _to_float(
            raw.get("short_margin_ratio"), f"{source} `{product_id}` short_margin_ratio"
        ),
        "trading_sessions": [str(item) for item in sessions],
        "commission": {},
    }
    for key in _COMMISSION_KEYS:
        normalized["commission"][key] = _to_float(
            commission.get(key), f"{source} `{product_id}` commission.{key}"
        )
    return normalized


def _load_instrument_json(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fp:
        root = json.load(fp)
    if not isinstance(root, dict):
        raise ValueError("instrument json root must be object")
    products_obj: dict[str, Any]
    if isinstance(root.get("products"), dict):
        products_obj = root["products"]
    else:
        products_obj = root
    normalized: dict[str, dict[str, Any]] = {}
    for product_id, value in products_obj.items():
        normalized[str(product_id)] = _normalize_entry(str(product_id), value, "instrument_json")
    return normalized


def _load_products_yaml_normalized(path: Path) -> dict[str, dict[str, Any]]:
    raw = _load_products_yaml(path)
    normalized: dict[str, dict[str, Any]] = {}
    for product_id, value in raw.items():
        normalized[str(product_id)] = _normalize_entry(str(product_id), value, "products_yaml")
    return normalized


def _num_equal(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)


def _diff_products(
    instrument_map: dict[str, dict[str, Any]], products_map: dict[str, dict[str, Any]]
) -> list[str]:
    errors: list[str] = []
    left_keys = set(instrument_map.keys())
    right_keys = set(products_map.keys())
    only_left = sorted(left_keys - right_keys)
    only_right = sorted(right_keys - left_keys)
    if only_left:
        errors.append("missing in products_yaml: " + ", ".join(only_left))
    if only_right:
        errors.append("extra in products_yaml: " + ", ".join(only_right))

    for product_id in sorted(left_keys & right_keys):
        left = instrument_map[product_id]
        right = products_map[product_id]
        if left["product"] != right["product"]:
            errors.append(
                f"{product_id}.product mismatch: instrument_json={left['product']} products_yaml={right['product']}"
            )
        for key in ("volume_multiple", "long_margin_ratio", "short_margin_ratio"):
            if not _num_equal(left[key], right[key]):
                errors.append(
                    f"{product_id}.{key} mismatch: instrument_json={left[key]} products_yaml={right[key]}"
                )
        if left["trading_sessions"] != right["trading_sessions"]:
            errors.append(f"{product_id}.trading_sessions mismatch")
        for key in _COMMISSION_KEYS:
            if not _num_equal(left["commission"][key], right["commission"][key]):
                errors.append(
                    f"{product_id}.commission.{key} mismatch: instrument_json={left['commission'][key]} "
                    f"products_yaml={right['commission'][key]}"
                )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify configs/strategies/products_info.yaml stays in sync with instrument_info.json."
    )
    parser.add_argument(
        "--instrument-json",
        default="configs/strategies/instrument_info.json",
        help="Path to instrument_info.json",
    )
    parser.add_argument(
        "--products-yaml",
        default="configs/strategies/products_info.yaml",
        help="Path to products_info.yaml",
    )
    args = parser.parse_args()

    instrument_path = Path(args.instrument_json)
    products_path = Path(args.products_yaml)
    if not instrument_path.exists():
        print(f"[products-info-sync] ERROR: missing file: {instrument_path}", file=sys.stderr)
        return 2
    if not products_path.exists():
        print(f"[products-info-sync] ERROR: missing file: {products_path}", file=sys.stderr)
        return 2

    try:
        instrument_map = _load_instrument_json(instrument_path)
        products_map = _load_products_yaml_normalized(products_path)
    except Exception as exc:  # pragma: no cover - error path is exercised by script test via exit code
        print(f"[products-info-sync] ERROR: {exc}", file=sys.stderr)
        return 2

    errors = _diff_products(instrument_map, products_map)
    if errors:
        print("[products-info-sync] drift detected:", file=sys.stderr)
        for item in errors:
            print(f"  - {item}", file=sys.stderr)
        return 1

    print(
        f"[products-info-sync] OK: {len(instrument_map)} products synchronized "
        f"between {instrument_path} and {products_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
