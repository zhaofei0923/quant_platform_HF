from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, replace
from itertools import product
from typing import TYPE_CHECKING, Any, Literal

from quant_hft.research.experiment_tracker import ExperimentTracker

if TYPE_CHECKING:
    from quant_hft.backtest.replay import BacktestRunSpec

ObjectiveDirection = Literal["maximize", "minimize"]


@dataclass(frozen=True)
class OptimizationTrial:
    trial_id: str
    run_id: str
    params: dict[str, Any]
    objective_value: float
    spec_signature: str


@dataclass(frozen=True)
class OptimizationResult:
    backend: str
    objective_name: str
    direction: ObjectiveDirection
    best_trial: OptimizationTrial
    trials: tuple[OptimizationTrial, ...]


@dataclass(frozen=True)
class OptimizationConfig:
    base_spec: BacktestRunSpec
    param_space: dict[str, list[Any]]
    objective_name: str = "objective"
    direction: ObjectiveDirection = "maximize"
    run_id_prefix: str = "opt"
    template: str = "grid_search"
    factor_id: str = "factor-default"


def _spec_signature(spec: BacktestRunSpec) -> str:
    canonical = json.dumps(spec.to_dict(), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_param_space(base_spec: BacktestRunSpec, param_space: dict[str, list[Any]]) -> None:
    if not param_space:
        raise ValueError("param_space is required")
    allowed_keys = set(base_spec.to_dict().keys())
    for key, values in param_space.items():
        if key not in allowed_keys:
            raise ValueError(f"unknown parameter in param_space: {key}")
        if not values:
            raise ValueError(f"parameter '{key}' must provide at least one candidate value")


def _is_better(candidate: float, reference: float, direction: ObjectiveDirection) -> bool:
    if direction == "maximize":
        return candidate > reference
    return candidate < reference


class OptimizerBackend(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Backend name."""

    @abstractmethod
    def optimize(
        self,
        config: OptimizationConfig,
        evaluator: Callable[[BacktestRunSpec], float],
        *,
        tracker: ExperimentTracker | None = None,
    ) -> OptimizationResult:
        """Run optimization trials and return best result."""


class GridSearchOptimizer(OptimizerBackend):
    @property
    def name(self) -> str:
        return "grid_search"

    def optimize(
        self,
        config: OptimizationConfig,
        evaluator: Callable[[BacktestRunSpec], float],
        *,
        tracker: ExperimentTracker | None = None,
    ) -> OptimizationResult:
        _validate_param_space(config.base_spec, config.param_space)

        keys = sorted(config.param_space.keys())
        trials: list[OptimizationTrial] = []

        combinations = product(*(config.param_space[key] for key in keys))
        for index, values in enumerate(combinations, start=1):
            params = {key: value for key, value in zip(keys, values, strict=True)}
            run_id = f"{config.run_id_prefix}-{index:04d}"
            trial_spec = replace(config.base_spec, run_id=run_id, **params)
            objective_value = float(evaluator(trial_spec))
            signature = _spec_signature(trial_spec)
            trial = OptimizationTrial(
                trial_id=f"trial-{index:04d}",
                run_id=run_id,
                params=params,
                objective_value=objective_value,
                spec_signature=signature,
            )
            trials.append(trial)

            if tracker is not None:
                tracker.append(
                    run_id=run_id,
                    template=config.template,
                    factor_id=config.factor_id,
                    spec_signature=signature,
                    metrics={config.objective_name: objective_value},
                )

        best_trial = trials[0]
        for trial in trials[1:]:
            if _is_better(trial.objective_value, best_trial.objective_value, config.direction):
                best_trial = trial

        return OptimizationResult(
            backend=self.name,
            objective_name=config.objective_name,
            direction=config.direction,
            best_trial=best_trial,
            trials=tuple(trials),
        )


class OptunaOptimizer(OptimizerBackend):
    @property
    def name(self) -> str:
        return "optuna"

    def optimize(
        self,
        config: OptimizationConfig,
        evaluator: Callable[[BacktestRunSpec], float],
        *,
        tracker: ExperimentTracker | None = None,
    ) -> OptimizationResult:
        _ = (config, evaluator, tracker)
        raise RuntimeError(
            "Optuna backend is reserved but not enabled in this repository build. "
            "Install optuna and provide a concrete OptunaOptimizer implementation."
        )
