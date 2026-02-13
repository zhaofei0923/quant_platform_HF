from __future__ import annotations

import time
from dataclasses import dataclass

from quant_hft.backtest.replay import iter_csv_ticks
from quant_hft.contracts import StateSnapshot7D
from quant_hft.runtime.ctp_direct_runner import CtpDirectRunner
from quant_hft.runtime.engine import StrategyRuntime
from quant_hft.strategy.demo import DemoStrategy

from .adapters import TestMdAdapter, TestTraderAdapter
from .config import SimNowCompareConfig


@dataclass(frozen=True)
class SimNowCompareResult:
    run_id: str
    strategy_id: str
    dry_run: bool
    broker_mode: str
    max_ticks: int
    instruments: list[str]
    simnow_intents: int
    simnow_order_events: int
    backtest_intents: int
    backtest_ticks_read: int
    delta_intents: int
    delta_ratio: float
    within_threshold: bool
    attribution: dict[str, float]
    risk_decomposition: dict[str, float]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "strategy_id": self.strategy_id,
            "dry_run": self.dry_run,
            "broker_mode": self.broker_mode,
            "max_ticks": self.max_ticks,
            "instruments": list(self.instruments),
            "simnow": {
                "intents_emitted": self.simnow_intents,
                "order_events": self.simnow_order_events,
            },
            "backtest": {
                "intents_emitted": self.backtest_intents,
                "ticks_read": self.backtest_ticks_read,
            },
            "delta": {
                "intents": self.delta_intents,
                "intents_ratio": self.delta_ratio,
            },
            "threshold": {
                "intents_abs_max": 0,
                "within_threshold": self.within_threshold,
            },
            "attribution": dict(self.attribution),
            "risk_decomposition": dict(self.risk_decomposition),
        }


class SimNowComparatorRunner:
    def __init__(self, config: SimNowCompareConfig) -> None:
        self._config = config

    def run(self) -> SimNowCompareResult:
        backtest_intents, backtest_ticks_read = self._run_backtest_baseline()
        simnow_intents, simnow_order_events = self._run_simnow_side()

        delta = simnow_intents - backtest_intents
        ratio = 0.0 if backtest_intents == 0 else abs(delta) / float(backtest_intents)
        within = delta == 0
        attribution = self._build_attribution(
            backtest_intents=backtest_intents,
            simnow_intents=simnow_intents,
            simnow_order_events=simnow_order_events,
            delta_ratio=ratio,
            within_threshold=within,
        )
        risk_decomposition = self._build_risk_decomposition(
            backtest_intents=backtest_intents,
            simnow_intents=simnow_intents,
            simnow_order_events=simnow_order_events,
            delta_ratio=ratio,
        )
        return SimNowCompareResult(
            run_id=self._config.run_id,
            strategy_id=self._config.strategy_id,
            dry_run=self._config.dry_run,
            broker_mode=self._config.broker_mode,
            max_ticks=self._config.max_ticks,
            instruments=list(self._config.instruments),
            simnow_intents=simnow_intents,
            simnow_order_events=simnow_order_events,
            backtest_intents=backtest_intents,
            backtest_ticks_read=backtest_ticks_read,
            delta_intents=delta,
            delta_ratio=ratio,
            within_threshold=within,
            attribution=attribution,
            risk_decomposition=risk_decomposition,
        )

    def _build_attribution(
        self,
        *,
        backtest_intents: int,
        simnow_intents: int,
        simnow_order_events: int,
        delta_ratio: float,
        within_threshold: bool,
    ) -> dict[str, float]:
        baseline = float(max(1, backtest_intents))
        signal_parity = max(0.0, 1.0 - abs(simnow_intents - backtest_intents) / baseline)
        execution_coverage = min(1.0, float(simnow_order_events) / float(max(1, simnow_intents)))
        threshold_stability = 1.0 if within_threshold else max(0.0, 1.0 - delta_ratio)
        return {
            "signal_parity": signal_parity,
            "execution_coverage": execution_coverage,
            "threshold_stability": threshold_stability,
        }

    def _build_risk_decomposition(
        self,
        *,
        backtest_intents: int,
        simnow_intents: int,
        simnow_order_events: int,
        delta_ratio: float,
    ) -> dict[str, float]:
        baseline = float(max(1, backtest_intents))
        return {
            "model_drift": abs(simnow_intents - backtest_intents) / baseline,
            "execution_gap": max(0.0, float(backtest_intents - simnow_order_events) / baseline),
            "consistency_gap": max(0.0, delta_ratio),
        }

    def _create_runtime(self) -> tuple[StrategyRuntime, dict[str, object]]:
        runtime = StrategyRuntime()
        runtime.add_strategy(DemoStrategy(self._config.strategy_id))
        ctx: dict[str, object] = {}
        return runtime, ctx

    def _run_backtest_baseline(self) -> tuple[int, int]:
        runtime, ctx = self._create_runtime()
        intents_emitted = 0
        ticks_read = 0
        last_price_by_instrument: dict[str, float] = {}
        ts_ns = 0
        for tick in iter_csv_ticks(
            self._config.backtest_csv_path,
            max_ticks=self._config.max_ticks,
        ):
            if tick.instrument_id not in self._config.instruments:
                continue
            ticks_read += 1
            ts_ns += 1_000_000
            prev_price = last_price_by_instrument.get(tick.instrument_id)
            trend_score = 0.0
            if prev_price is not None:
                if tick.last_price > prev_price:
                    trend_score = 1.0
                elif tick.last_price < prev_price:
                    trend_score = -1.0
            last_price_by_instrument[tick.instrument_id] = tick.last_price
            state = StateSnapshot7D(
                instrument_id=tick.instrument_id,
                trend={"score": trend_score, "confidence": 1.0},
                volatility={"score": 0.0, "confidence": 0.0},
                liquidity={"score": 0.0, "confidence": 0.0},
                sentiment={"score": 0.0, "confidence": 0.0},
                seasonality={"score": 0.0, "confidence": 0.0},
                pattern={"score": 0.0, "confidence": 0.0},
                event_drive={"score": 0.0, "confidence": 0.0},
                ts_ns=ts_ns,
            )
            intents_emitted += len(runtime.on_state(ctx, state))
        return intents_emitted, ticks_read

    def _run_simnow_side(self) -> tuple[int, int]:
        runtime, ctx = self._create_runtime()
        from quant_hft.runtime.ctp_direct_runner import CtpDirectRunnerConfig

        runner_cfg = CtpDirectRunnerConfig(
            strategy_id=self._config.strategy_id,
            account_id=str(self._config.connect_config.get("investor_id", "sim-account")),
            instruments=list(self._config.instruments),
            poll_interval_ms=self._config.poll_interval_ms,
            settlement_confirm_required=self._config.settlement_confirm_required,
            query_qps_limit=10,
            dispatcher_workers=1,
            connect_config=dict(self._config.connect_config),
        )

        if self._config.dry_run:
            fake_trader = TestTraderAdapter()
            fake_md = TestMdAdapter()
            runner = CtpDirectRunner(
                runtime,
                runner_cfg,
                trader_factory=lambda qps, workers: fake_trader,
                md_factory=lambda qps, workers: fake_md,
                ctx=ctx,
            )
            if not runner.start():
                raise RuntimeError("simnow compare runner failed to start")
            try:
                emitted_total = 0
                ts_ns = 0
                for tick in iter_csv_ticks(
                    self._config.backtest_csv_path,
                    max_ticks=self._config.max_ticks,
                ):
                    if tick.instrument_id not in self._config.instruments:
                        continue
                    ts_ns += 1_000_000
                    fake_md.emit_tick(
                        {
                            "instrument_id": tick.instrument_id,
                            "last_price": tick.last_price,
                            "bid_price_1": tick.bid_price_1,
                            "ask_price_1": tick.ask_price_1,
                            "bid_volume_1": tick.bid_volume_1,
                            "ask_volume_1": tick.ask_volume_1,
                            "volume": tick.volume,
                            "ts_ns": ts_ns,
                        }
                    )
                    emitted_total += runner.run_once()
                events = ctx.get("order_events", [])
                event_count = len(events) if isinstance(events, list) else 0
                return emitted_total, event_count
            finally:
                runner.stop()

        runner = CtpDirectRunner(runtime, runner_cfg, ctx=ctx)
        if not runner.start():
            raise RuntimeError("simnow compare runner failed to start")
        try:
            time.sleep(self._config.poll_interval_ms / 1000.0)
            emitted_total = runner.run_once()
            events = ctx.get("order_events", [])
            event_count = len(events) if isinstance(events, list) else 0
            return emitted_total, event_count
        finally:
            runner.stop()
