from __future__ import annotations

from pathlib import Path


def test_daily_settlement_systemd_unit_files_exist_and_contain_expected_fields() -> None:
    service_path = Path("infra/systemd/quant-hft-daily-settlement.service")
    timer_path = Path("infra/systemd/quant-hft-daily-settlement.timer")

    assert service_path.exists(), f"missing file: {service_path}"
    assert timer_path.exists(), f"missing file: {timer_path}"

    service = service_path.read_text(encoding="utf-8")
    timer = timer_path.read_text(encoding="utf-8")

    assert "Type=oneshot" in service
    assert "run_daily_settlement.sh" in service
    assert "WorkingDirectory=" in service
    assert "ExecStart=" in service
    assert "User=" not in service

    assert "OnCalendar=*-*-* 03:00:00" in timer
    assert "Persistent=true" in timer
    assert "quant-hft-daily-settlement.service" in timer
