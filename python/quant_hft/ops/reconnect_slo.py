from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FaultEventRecord:
    ts_ns: int
    mode: str
    scenario: str
    phase: str
    execute: bool
    parameters: dict[str, str]


@dataclass(frozen=True)
class ProbeHealthEvent:
    ts_ns: int
    state: str


@dataclass(frozen=True)
class FaultWindow:
    scenario: str
    apply_ts_ns: int
    clear_ts_ns: int
    parameters: dict[str, str]


@dataclass(frozen=True)
class ReconnectSample:
    scenario: str
    apply_ts_ns: int
    clear_ts_ns: int
    recovered: bool
    recovery_seconds: float | None
    unhealthy_observed: bool
    parameters: dict[str, str]


@dataclass(frozen=True)
class ReconnectSloReport:
    samples: tuple[ReconnectSample, ...]
    p99_recovery_seconds: float | None
    target_p99_seconds: float
    meets_target: bool


_HEALTH_PATTERN = re.compile(r"\[health\]\s+ts_ns=(\d+)\s+state=(healthy|unhealthy)")


def load_fault_events(path: Path | str) -> list[FaultEventRecord]:
    events: list[FaultEventRecord] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        payload = json.loads(raw)
        ts_ns = int(payload.get("ts_ns", 0))
        mode = str(payload.get("mode", "")).strip()
        scenario = str(payload.get("scenario", "")).strip()
        phase = str(payload.get("phase", "")).strip()
        execute = bool(payload.get("execute", False))
        parameters_raw = payload.get("parameters", {})
        parameters: dict[str, str] = {}
        if isinstance(parameters_raw, dict):
            parameters = {str(key): str(value) for key, value in parameters_raw.items()}
        if ts_ns <= 0 or not mode or not scenario or not phase:
            continue
        events.append(
            FaultEventRecord(
                ts_ns=ts_ns,
                mode=mode,
                scenario=scenario,
                phase=phase,
                execute=execute,
                parameters=parameters,
            )
        )
    events.sort(key=lambda item: item.ts_ns)
    return events


def load_probe_health_events(path: Path | str) -> list[ProbeHealthEvent]:
    events: list[ProbeHealthEvent] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        matched = _HEALTH_PATTERN.search(line)
        if matched is None:
            continue
        ts_ns = int(matched.group(1))
        state = matched.group(2)
        events.append(ProbeHealthEvent(ts_ns=ts_ns, state=state))
    events.sort(key=lambda item: item.ts_ns)
    return events


def _build_fault_windows(events: list[FaultEventRecord]) -> list[FaultWindow]:
    pending: dict[str, FaultEventRecord] = {}
    windows: list[FaultWindow] = []
    for event in events:
        if not event.execute:
            continue
        if event.phase == "apply":
            pending[event.scenario] = event
            continue
        if event.phase != "clear":
            continue
        start = pending.pop(event.scenario, None)
        if start is None:
            continue
        windows.append(
            FaultWindow(
                scenario=event.scenario,
                apply_ts_ns=start.ts_ns,
                clear_ts_ns=event.ts_ns,
                parameters=start.parameters,
            )
        )
    windows.sort(key=lambda item: item.apply_ts_ns)
    return windows


def _compute_p99(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    rank = max(0, math.ceil(0.99 * len(sorted_values)) - 1)
    return sorted_values[rank]


def evaluate_reconnect_slo(
    *,
    fault_events: list[FaultEventRecord],
    health_events: list[ProbeHealthEvent],
    target_p99_seconds: float,
) -> ReconnectSloReport:
    if target_p99_seconds <= 0:
        raise ValueError("target_p99_seconds must be > 0")

    windows = _build_fault_windows(fault_events)
    samples: list[ReconnectSample] = []
    recovered_values: list[float] = []
    for window in windows:
        unhealthy_observed = any(
            event.state == "unhealthy"
            and event.ts_ns >= window.apply_ts_ns
            and event.ts_ns <= window.clear_ts_ns
            for event in health_events
        )
        healthy_after_clear = next(
            (
                event
                for event in health_events
                if event.state == "healthy" and event.ts_ns >= window.clear_ts_ns
            ),
            None,
        )
        if healthy_after_clear is None:
            samples.append(
                ReconnectSample(
                    scenario=window.scenario,
                    apply_ts_ns=window.apply_ts_ns,
                    clear_ts_ns=window.clear_ts_ns,
                    recovered=False,
                    recovery_seconds=None,
                    unhealthy_observed=unhealthy_observed,
                    parameters=window.parameters,
                )
            )
            continue
        recovery_seconds = (healthy_after_clear.ts_ns - window.clear_ts_ns) / 1_000_000_000.0
        recovered_values.append(recovery_seconds)
        samples.append(
            ReconnectSample(
                scenario=window.scenario,
                apply_ts_ns=window.apply_ts_ns,
                clear_ts_ns=window.clear_ts_ns,
                recovered=True,
                recovery_seconds=recovery_seconds,
                unhealthy_observed=unhealthy_observed,
                parameters=window.parameters,
            )
        )

    p99 = _compute_p99(recovered_values)
    return ReconnectSloReport(
        samples=tuple(samples),
        p99_recovery_seconds=p99,
        target_p99_seconds=target_p99_seconds,
        meets_target=p99 is not None
        and p99 < target_p99_seconds
        and all(item.recovered for item in samples),
    )
