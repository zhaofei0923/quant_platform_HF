from __future__ import annotations

from quant_hft.research.factor_catalog import FactorCatalog, FactorStatus


def test_factor_catalog_upsert_and_status_transition() -> None:
    ticks = {"value": 0}

    def fake_now_ns() -> int:
        ticks["value"] += 10
        return ticks["value"]

    catalog = FactorCatalog(now_ns_fn=fake_now_ns)
    created = catalog.upsert_factor(
        factor_id="fac.momentum.001",
        description="momentum factor",
        owner="quant-team",
        tags=("trend", "momentum"),
    )
    assert created.status is FactorStatus.CANDIDATE
    assert created.updated_ts_ns == 10

    approved = catalog.update_status("fac.momentum.001", FactorStatus.APPROVED)
    assert approved.status is FactorStatus.APPROVED
    assert approved.updated_ts_ns == 20
    assert catalog.get("fac.momentum.001") == approved


def test_factor_catalog_rejects_invalid_factor_id() -> None:
    catalog = FactorCatalog()
    try:
        catalog.upsert_factor(
            factor_id="",
            description="bad",
            owner="quant-team",
        )
    except ValueError as exc:
        assert "factor_id" in str(exc)
    else:
        raise AssertionError("expected ValueError for empty factor_id")
