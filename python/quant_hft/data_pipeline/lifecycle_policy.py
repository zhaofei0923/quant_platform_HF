from __future__ import annotations

import re
import shutil
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Protocol


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


@dataclass(frozen=True)
class ObjectLifecycleRule:
    dataset: str
    base_prefix: str
    hot_retention_days: int
    warm_retention_days: int
    cold_retention_days: int
    delete_after_days: int


@dataclass(frozen=True)
class ObjectLifecycleDecision:
    dataset: str
    object_name: str
    age_days: float
    current_tier: str
    action: str
    target_object: str


@dataclass(frozen=True)
class ObjectLifecycleReport:
    scanned_objects: int
    moved_objects: int
    deleted_objects: int
    kept_objects: int
    dry_run: bool


class _ArchiveStoreLike(Protocol):
    def list_objects(self, *, prefix: str = "") -> list[str]: ...

    def copy_object(self, source_object: str, destination_object: str) -> None: ...

    def remove_object(self, object_name: str) -> None: ...


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


class ObjectStoreLifecyclePolicy:
    """Prefix-based object lifecycle for MinIO/S3 archives."""

    _PARTITION_DATE_PATTERN = re.compile(
        r"(?:^|/)(?:dt|trade_date)=([0-9]{4}-[0-9]{2}-[0-9]{2})(?:/|$)"
    )

    def __init__(
        self,
        archive_store: _ArchiveStoreLike,
        rules: Iterable[ObjectLifecycleRule],
        *,
        now_epoch_seconds_fn: Callable[[], float] | None = None,
    ) -> None:
        self._archive_store = archive_store
        self._rules = list(rules)
        self._now_epoch_seconds_fn = now_epoch_seconds_fn or time.time

    @classmethod
    def _extract_partition_epoch_seconds(cls, object_name: str) -> float | None:
        matched = cls._PARTITION_DATE_PATTERN.search(object_name)
        if matched is None:
            return None
        date_text = matched.group(1)
        try:
            dt = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        return dt.timestamp()

    @staticmethod
    def _desired_tier(age_days: float, rule: ObjectLifecycleRule) -> str:
        if age_days > float(rule.delete_after_days):
            return "delete"
        if age_days <= float(rule.hot_retention_days):
            return "hot"
        if age_days <= float(rule.warm_retention_days):
            return "warm"
        return "cold"

    def plan(self) -> list[ObjectLifecycleDecision]:
        now = float(self._now_epoch_seconds_fn())
        decisions: list[ObjectLifecycleDecision] = []
        for rule in self._rules:
            base = rule.base_prefix.strip("/")
            for tier in ("hot", "warm", "cold"):
                prefix = f"{base}/{tier}".strip("/")
                objects = self._archive_store.list_objects(prefix=prefix)
                for object_name in objects:
                    partition_ts = self._extract_partition_epoch_seconds(object_name)
                    age_days = 0.0
                    if partition_ts is not None:
                        age_days = max(0.0, (now - partition_ts) / 86_400.0)

                    desired = self._desired_tier(age_days, rule)
                    action = "keep"
                    target = ""
                    if desired == "delete":
                        action = "delete"
                    elif desired != tier:
                        action = "move"
                        needle = f"/{tier}/"
                        replacement = f"/{desired}/"
                        if needle in object_name:
                            target = object_name.replace(needle, replacement, 1)
                        else:
                            target = f"{base}/{desired}/{Path(object_name).name}".strip("/")

                    decisions.append(
                        ObjectLifecycleDecision(
                            dataset=rule.dataset,
                            object_name=object_name,
                            age_days=age_days,
                            current_tier=tier,
                            action=action,
                            target_object=target,
                        )
                    )
        return decisions

    def apply(
        self,
        decisions: Iterable[ObjectLifecycleDecision],
        *,
        execute: bool,
    ) -> ObjectLifecycleReport:
        scanned = 0
        moved = 0
        deleted = 0
        kept = 0

        for decision in decisions:
            scanned += 1
            if decision.action == "keep":
                kept += 1
                continue
            if decision.action == "move":
                if execute:
                    self._archive_store.copy_object(decision.object_name, decision.target_object)
                    self._archive_store.remove_object(decision.object_name)
                moved += 1
                continue
            if decision.action == "delete":
                if execute:
                    self._archive_store.remove_object(decision.object_name)
                deleted += 1
                continue

        return ObjectLifecycleReport(
            scanned_objects=scanned,
            moved_objects=moved,
            deleted_objects=deleted,
            kept_objects=kept,
            dry_run=not execute,
        )
