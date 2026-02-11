from __future__ import annotations

import pytest
from quant_hft.data_pipeline.data_dictionary import DataDictionary


def test_data_dictionary_validates_redis_order_event_required_fields() -> None:
    dictionary = DataDictionary()
    row = {
        "account_id": "sim-account",
        "client_order_id": "ord-1",
        "instrument_id": "rb2405",
        "status": "FILLED",
        "total_volume": 2,
        "filled_volume": 2,
        "avg_fill_price": 4101.5,
        "reason": "ok",
        "ts_ns": 100,
        "trace_id": "trace-1",
        "execution_algo_id": "twap",
        "slice_index": 1,
        "slice_total": 2,
        "throttle_applied": False,
    }

    errors = dictionary.validate("redis_order_event", row)
    assert errors == []


def test_data_dictionary_rejects_missing_required_field_and_wrong_type() -> None:
    dictionary = DataDictionary()
    row = {
        "account_id": "sim-account",
        "client_order_id": "ord-1",
        "instrument_id": "rb2405",
        "status": "FILLED",
        "filled_volume": 2,
        "avg_fill_price": 4101.5,
        "reason": "ok",
        "ts_ns": "bad-ts",
    }

    errors = dictionary.validate("redis_order_event", row)
    assert any("missing required field: total_volume" in item for item in errors)
    assert any("field ts_ns expected int" in item for item in errors)


def test_data_dictionary_alignment_between_redis_and_timescale() -> None:
    dictionary = DataDictionary()
    missing = dictionary.validate_schema_alignment("redis_order_event", "timescale_order_event")
    assert missing == ()


def test_data_dictionary_raises_for_unknown_schema() -> None:
    dictionary = DataDictionary()
    with pytest.raises(ValueError):
        dictionary.validate("unknown", {})
