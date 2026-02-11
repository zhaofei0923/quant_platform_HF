from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class FactorStatus(str, Enum):
    CANDIDATE = "candidate"
    BACKTESTED = "backtested"
    APPROVED = "approved"
    DEPRECATED = "deprecated"


@dataclass(frozen=True)
class FactorRecord:
    factor_id: str
    description: str
    owner: str
    status: FactorStatus
    tags: tuple[str, ...]
    updated_ts_ns: int


class FactorCatalog:
    def __init__(self, now_ns_fn: Callable[[], int] | None = None) -> None:
        self._now_ns_fn = now_ns_fn or time.time_ns
        self._records: dict[str, FactorRecord] = {}

    def upsert_factor(
        self,
        *,
        factor_id: str,
        description: str,
        owner: str,
        status: FactorStatus = FactorStatus.CANDIDATE,
        tags: tuple[str, ...] = (),
    ) -> FactorRecord:
        if not factor_id.strip():
            raise ValueError("factor_id is required")
        if not owner.strip():
            raise ValueError("owner is required")
        record = FactorRecord(
            factor_id=factor_id.strip(),
            description=description.strip(),
            owner=owner.strip(),
            status=status,
            tags=tuple(tag.strip() for tag in tags if tag.strip()),
            updated_ts_ns=int(self._now_ns_fn()),
        )
        self._records[record.factor_id] = record
        return record

    def update_status(self, factor_id: str, status: FactorStatus) -> FactorRecord:
        existing = self._records.get(factor_id)
        if existing is None:
            raise KeyError(f"unknown factor: {factor_id}")
        updated = FactorRecord(
            factor_id=existing.factor_id,
            description=existing.description,
            owner=existing.owner,
            status=status,
            tags=existing.tags,
            updated_ts_ns=int(self._now_ns_fn()),
        )
        self._records[factor_id] = updated
        return updated

    def get(self, factor_id: str) -> FactorRecord | None:
        return self._records.get(factor_id)

    def list_all(self) -> list[FactorRecord]:
        return sorted(self._records.values(), key=lambda item: item.factor_id)
