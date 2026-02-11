from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricSpec:
    key: str
    unit: str
    description: str


METRIC_DICTIONARY: dict[str, MetricSpec] = {
    "total_pnl": MetricSpec("total_pnl", "currency", "total pnl across deterministic replay"),
    "max_drawdown": MetricSpec("max_drawdown", "currency", "max equity drawdown"),
    "win_rate": MetricSpec("win_rate", "ratio", "filled win ratio"),
    "fill_rate": MetricSpec("fill_rate", "ratio", "filled volume / total volume"),
    "capital_efficiency": MetricSpec(
        "capital_efficiency", "ratio", "total pnl / max(abs(position_notional), 1)"
    ),
}


def metric_keys() -> tuple[str, ...]:
    return tuple(sorted(METRIC_DICTIONARY))
