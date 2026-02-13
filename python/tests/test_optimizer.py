from __future__ import annotations

import pytest

from quant_hft.backtest.replay import BacktestRunSpec
from quant_hft.research.experiment_tracker import ExperimentTracker
from quant_hft.research.optimizer import (
    GridSearchOptimizer,
    OptimizationConfig,
    OptunaOptimizer,
)


def test_grid_search_optimizer_selects_best_trial_for_maximize(tmp_path) -> None:
    base_spec = BacktestRunSpec(csv_path="backtest_data/rb.csv", run_id="seed")
    config = OptimizationConfig(
        base_spec=base_spec,
        param_space={"max_ticks": [10, 20], "deterministic_fills": [False, True]},
        objective_name="score",
        direction="maximize",
        run_id_prefix="grid",
    )

    def evaluator(spec: BacktestRunSpec) -> float:
        return float(spec.max_ticks or 0) + (100.0 if spec.deterministic_fills else 0.0)

    tracker = ExperimentTracker(tmp_path / "experiments.jsonl")
    result = GridSearchOptimizer().optimize(config, evaluator, tracker=tracker)

    assert result.backend == "grid_search"
    assert len(result.trials) == 4
    assert result.best_trial.params["max_ticks"] == 20
    assert result.best_trial.params["deterministic_fills"] is True
    assert result.best_trial.objective_value == pytest.approx(120.0)

    records = tracker.load_all()
    assert len(records) == 4
    assert all("score" in record.metrics for record in records)


def test_grid_search_optimizer_supports_minimize() -> None:
    base_spec = BacktestRunSpec(csv_path="backtest_data/rb.csv", run_id="seed")
    config = OptimizationConfig(
        base_spec=base_spec,
        param_space={"max_ticks": [10, 20, 30]},
        objective_name="loss",
        direction="minimize",
        run_id_prefix="grid",
    )

    def evaluator(spec: BacktestRunSpec) -> float:
        return float(spec.max_ticks or 0)

    result = GridSearchOptimizer().optimize(config, evaluator)
    assert result.best_trial.params["max_ticks"] == 10
    assert result.best_trial.objective_value == pytest.approx(10.0)


def test_grid_search_optimizer_rejects_unknown_parameter() -> None:
    base_spec = BacktestRunSpec(csv_path="backtest_data/rb.csv", run_id="seed")
    config = OptimizationConfig(
        base_spec=base_spec,
        param_space={"unknown_param": [1, 2]},
    )

    with pytest.raises(ValueError, match="unknown parameter"):
        GridSearchOptimizer().optimize(config, lambda spec: 0.0)


def test_optuna_optimizer_placeholder_raises() -> None:
    base_spec = BacktestRunSpec(csv_path="backtest_data/rb.csv", run_id="seed")
    config = OptimizationConfig(base_spec=base_spec, param_space={"max_ticks": [10]})

    with pytest.raises(RuntimeError, match="Optuna backend"):
        OptunaOptimizer().optimize(config, lambda spec: 0.0)
