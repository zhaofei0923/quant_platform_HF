from __future__ import annotations

import json
from pathlib import Path

from quant_hft.simnow import SimNowComparatorRunner, load_simnow_compare_config


def _write_csv(path: Path, rows: int = 5) -> None:
    header = (
        "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,"
        "BidPrice1,BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest"
    )
    lines = [header]
    for idx in range(rows):
        lines.append(
            ",".join(
                [
                    "20260102",
                    "SHFE.ag2406",
                    "09:30:00",
                    str(idx),
                    f"{4500.0 + idx}",
                    str(idx + 1),
                    f"{4499.0 + idx}",
                    "10",
                    f"{4501.0 + idx}",
                    "12",
                    f"{4500.0 + idx}",
                    f"{10000.0 + idx}",
                    f"{100.0 + idx}",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_ctp_yaml(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "ctp:",
                "environment: sim",
                "is_production_mode: false",
                "enable_real_api: false",
                "market_front: tcp://sim-md",
                "trader_front: tcp://sim-td",
                "broker_id: ${CTP_SIM_BROKER_ID}",
                "user_id: ${CTP_SIM_USER_ID}",
                "investor_id: ${CTP_SIM_INVESTOR_ID}",
                "password: ${CTP_SIM_PASSWORD}",
                "strategy_ids: demo",
                "instruments: SHFE.ag2406",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_simnow_compare_runner_dry_run(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "ticks.csv"
    cfg_path = tmp_path / "ctp.yaml"
    report_path = tmp_path / "report.json"
    _write_csv(csv_path, rows=6)
    _write_ctp_yaml(cfg_path)

    monkeypatch.setenv("CTP_SIM_BROKER_ID", "9999")
    monkeypatch.setenv("CTP_SIM_USER_ID", "191202")
    monkeypatch.setenv("CTP_SIM_INVESTOR_ID", "191202")
    monkeypatch.setenv("CTP_SIM_PASSWORD", "secret")

    cfg = load_simnow_compare_config(
        ctp_config_path=str(cfg_path),
        backtest_csv_path=str(csv_path),
        output_json_path=str(report_path),
        run_id="ut-run",
        max_ticks=6,
        dry_run=True,
    )
    assert cfg.broker_mode == "paper"
    assert cfg.connect_config["broker_id"] == "9999"

    result = SimNowComparatorRunner(cfg).run()
    payload = result.to_dict()
    assert payload["delta"]["intents"] == 0
    assert payload["threshold"]["within_threshold"] is True
    assert payload["attribution"]["signal_parity"] == 1.0
    assert payload["risk_decomposition"]["model_drift"] == 0.0

    report_path.write_text(json.dumps(payload), encoding="utf-8")
    loaded = json.loads(report_path.read_text(encoding="utf-8"))
    assert loaded["simnow"]["intents_emitted"] == loaded["backtest"]["intents_emitted"]
