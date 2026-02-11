from __future__ import annotations

from dataclasses import dataclass

SLI_PREFIX = "quant_hft_"


@dataclass(frozen=True)
class SliCatalogEntry:
    name: str
    description: str
    unit: str


SLI_CATALOG: dict[str, SliCatalogEntry] = {
    "core_process_alive": SliCatalogEntry(
        name="core_process_alive",
        description="core process liveness during probe collection window",
        unit="bool",
    ),
    "strategy_bridge_latency_p99_ms": SliCatalogEntry(
        name="strategy_bridge_latency_p99_ms",
        description="P99 bridge recovery latency from reconnect samples",
        unit="ms",
    ),
    "strategy_bridge_chain_integrity": SliCatalogEntry(
        name="strategy_bridge_chain_integrity",
        description="state->intent->order key chain integrity",
        unit="bool",
    ),
    "storage_redis_health": SliCatalogEntry(
        name="storage_redis_health",
        description="redis dependency health probe",
        unit="bool",
    ),
    "storage_timescale_health": SliCatalogEntry(
        name="storage_timescale_health",
        description="timescale dependency health probe",
        unit="bool",
    ),
}


def with_prefix(name: str) -> str:
    if name.startswith(SLI_PREFIX):
        return name
    return f"{SLI_PREFIX}{name}"


def strip_prefix(name: str) -> str:
    if name.startswith(SLI_PREFIX):
        return name[len(SLI_PREFIX) :]
    return name


def canonical_sli_name(name: str) -> str:
    normalized = strip_prefix(name)
    if normalized not in SLI_CATALOG:
        raise ValueError(f"unknown sli name: {name}")
    return with_prefix(normalized)
