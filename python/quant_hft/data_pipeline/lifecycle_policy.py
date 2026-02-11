from __future__ import annotations

import shutil
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class StorageTier(str, Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


@dataclass(frozen=True)
class LifecyclePolicyConfig:
    hot_retention_days: int = 1
    warm_retention_days: int = 7


@dataclass(frozen=True)
class LifecycleDecision:
    source_path: Path
    relative_path: Path
    age_days: float
    tier: StorageTier
    action: str


@dataclass(frozen=True)
class LifecycleReport:
    scanned_files: int
    hot_files: int
    warm_files: int
    cold_files: int
    moved_files: int
    dry_run: bool


class LifecyclePolicy:
    """Local lifecycle policy to validate hot/warm/cold transitions."""

    def __init__(
        self,
        source_root: Path | str,
        config: LifecyclePolicyConfig | None = None,
        *,
        now_epoch_seconds_fn: Callable[[], float] | None = None,
    ) -> None:
        source = Path(source_root)
        if not source.exists():
            raise ValueError(f"source root does not exist: {source}")
        self._source_root = source.resolve()
        self._config = config or LifecyclePolicyConfig()
        if self._config.hot_retention_days < 0:
            raise ValueError("hot_retention_days must be >= 0")
        if self._config.warm_retention_days < self._config.hot_retention_days:
            raise ValueError("warm_retention_days must be >= hot_retention_days")
        self._now_epoch_seconds_fn = now_epoch_seconds_fn or time.time

    def plan(self) -> list[LifecycleDecision]:
        files = sorted(path for path in self._source_root.rglob("*") if path.is_file())
        return self.plan_files(files)

    def plan_files(self, paths: Iterable[Path]) -> list[LifecycleDecision]:
        now = float(self._now_epoch_seconds_fn())
        decisions: list[LifecycleDecision] = []
        for path in paths:
            resolved = path.resolve()
            if not resolved.exists() or not resolved.is_file():
                continue
            relative = resolved.relative_to(self._source_root)
            modified_seconds = resolved.stat().st_mtime
            age_days = max(0.0, (now - modified_seconds) / 86_400.0)
            tier = self.classify(age_days)
            action = "keep"
            if tier is StorageTier.WARM:
                action = "transition_warm"
            elif tier is StorageTier.COLD:
                action = "transition_cold"
            decisions.append(
                LifecycleDecision(
                    source_path=resolved,
                    relative_path=relative,
                    age_days=age_days,
                    tier=tier,
                    action=action,
                )
            )
        return decisions

    def apply(
        self,
        decisions: Iterable[LifecycleDecision],
        *,
        warm_root: Path | str,
        cold_root: Path | str,
        execute: bool,
    ) -> LifecycleReport:
        warm = Path(warm_root)
        cold = Path(cold_root)
        if execute:
            warm.mkdir(parents=True, exist_ok=True)
            cold.mkdir(parents=True, exist_ok=True)

        scanned_files = 0
        hot_files = 0
        warm_files = 0
        cold_files = 0
        moved_files = 0

        for decision in decisions:
            scanned_files += 1
            if decision.tier is StorageTier.HOT:
                hot_files += 1
                continue
            target_root = warm if decision.tier is StorageTier.WARM else cold
            if decision.tier is StorageTier.WARM:
                warm_files += 1
            else:
                cold_files += 1
            if not execute:
                continue
            target = (target_root / decision.relative_path).resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(decision.source_path), str(target))
            moved_files += 1

        return LifecycleReport(
            scanned_files=scanned_files,
            hot_files=hot_files,
            warm_files=warm_files,
            cold_files=cold_files,
            moved_files=moved_files,
            dry_run=not execute,
        )

    def classify(self, age_days: float) -> StorageTier:
        if age_days <= float(self._config.hot_retention_days):
            return StorageTier.HOT
        if age_days <= float(self._config.warm_retention_days):
            return StorageTier.WARM
        return StorageTier.COLD
