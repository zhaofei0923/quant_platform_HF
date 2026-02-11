from __future__ import annotations

from pathlib import Path

from quant_hft.research.experiment_tracker import ExperimentTracker


def test_experiment_tracker_appends_and_loads_jsonl_records(tmp_path: Path) -> None:
    ticks = {"value": 100}

    def fake_now_ns() -> int:
        ticks["value"] += 1
        return ticks["value"]

    output = tmp_path / "experiments.jsonl"
    tracker = ExperimentTracker(output, now_ns_fn=fake_now_ns)
    created = tracker.append(
        run_id="run-1",
        template="trend",
        factor_id="fac.momentum.001",
        spec_signature="sig-abc",
        metrics={"total_pnl": 1.2, "max_drawdown": -0.3},
    )
    assert created.created_ts_ns == 101
    assert output.exists()

    loaded = tracker.load_all()
    assert len(loaded) == 1
    assert loaded[0].run_id == "run-1"
    assert loaded[0].template == "trend"
    assert loaded[0].factor_id == "fac.momentum.001"
    assert loaded[0].metrics["total_pnl"] == 1.2


def test_experiment_tracker_requires_run_id(tmp_path: Path) -> None:
    tracker = ExperimentTracker(tmp_path / "experiments.jsonl")
    try:
        tracker.append(
            run_id="",
            template="trend",
            factor_id="fac.momentum.001",
            spec_signature="sig",
            metrics={},
        )
    except ValueError as exc:
        assert "run_id" in str(exc)
    else:
        raise AssertionError("expected ValueError for empty run_id")
