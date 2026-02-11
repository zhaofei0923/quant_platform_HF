from __future__ import annotations

import os
import socket
import threading
from pathlib import Path

from quant_hft.ops import ctp_preflight as ctp_preflight_module
from quant_hft.ops.ctp_preflight import (
    CtpPreflightConfig,
    run_ctp_preflight,
)


class _TcpListener:
    def __init__(self) -> None:
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self.port = int(self._server.getsockname()[1])
        self._server.listen(1)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        self._server.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._server.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            conn.close()

    def close(self) -> None:
        self._stop.set()
        try:
            self._server.close()
        except OSError:
            pass
        self._thread.join(timeout=1)


def _write_config(
    path: Path,
    market_front: str,
    trader_front: str,
    *,
    password_env: str,
    enable_terminal_auth: bool = True,
    is_production_mode: bool = False,
) -> None:
    path.write_text(
        "\n".join(
            [
                "ctp:",
                "  profile: simnow_trading_hours",
                "  environment: sim",
                f"  is_production_mode: {'true' if is_production_mode else 'false'}",
                "  enable_real_api: true",
                f"  enable_terminal_auth: {'true' if enable_terminal_auth else 'false'}",
                '  broker_id: "9999"',
                '  user_id: "191202"',
                '  investor_id: "191202"',
                f'  market_front: "{market_front}"',
                f'  trader_front: "{trader_front}"',
                f'  password_env: "{password_env}"',
                '  auth_code: "0000000000000000"' if enable_terminal_auth else "",
                '  app_id: "simnow_client_test"' if enable_terminal_auth else "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _prepare_ctp_lib_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for filename in (
        "ThostFtdcTraderApi.h",
        "thostmduserapi_se.so",
        "thosttraderapi_se.so",
        "error.xml",
    ):
        (path / filename).write_text("placeholder\n", encoding="utf-8")


def test_preflight_passes_with_local_reachable_fronts(tmp_path: Path) -> None:
    md = _TcpListener()
    td = _TcpListener()
    password_key = "CTP_PREFLIGHT_PASSWORD_OK"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front=f"tcp://127.0.0.1:{md.port}",
        trader_front=f"tcp://127.0.0.1:{td.port}",
        password_env=password_key,
    )
    os.environ[password_key] = "secret"
    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=500,
                skip_network_check=False,
            )
        )
    finally:
        md.close()
        td.close()
        os.environ.pop(password_key, None)

    assert report.ok is True
    assert any(item.name == "tcp_connect_market_front" and item.ok for item in report.items)
    assert any(item.name == "tcp_connect_trader_front" and item.ok for item in report.items)


def test_preflight_fails_when_password_env_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://127.0.0.1:30011",
        trader_front="tcp://127.0.0.1:30001",
        password_env="CTP_PREFLIGHT_PASSWORD_MISSING",
    )
    os.environ.pop("CTP_PREFLIGHT_PASSWORD_MISSING", None)

    report = run_ctp_preflight(
        CtpPreflightConfig(
            config_path=config_path,
            ctp_lib_dir=ctp_lib_dir,
            connect_timeout_ms=100,
            skip_network_check=True,
        )
    )

    assert report.ok is False
    assert any(item.name == "password_source" and not item.ok for item in report.items)


def test_preflight_can_skip_network_check(tmp_path: Path) -> None:
    password_key = "CTP_PREFLIGHT_PASSWORD_SKIP_NETWORK"
    os.environ[password_key] = "secret"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://127.0.0.1:35555",
        trader_front="tcp://127.0.0.1:36666",
        password_env=password_key,
    )
    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=100,
                skip_network_check=True,
            )
        )
    finally:
        os.environ.pop(password_key, None)

    assert report.ok is True
    assert any(item.name == "tcp_connect_market_front" and item.skipped for item in report.items)


def test_preflight_allows_missing_auth_fields_when_terminal_auth_disabled(
    tmp_path: Path,
) -> None:
    password_key = "CTP_PREFLIGHT_PASSWORD_NO_AUTH"
    os.environ[password_key] = "secret"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://127.0.0.1:35555",
        trader_front="tcp://127.0.0.1:36666",
        password_env=password_key,
        enable_terminal_auth=False,
    )
    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=100,
                skip_network_check=True,
            )
        )
    finally:
        os.environ.pop(password_key, None)

    assert report.ok is True
    assert any(item.name == "required_fields" and item.ok for item in report.items)


def test_preflight_adds_service_window_hint_on_trading_hours_timeout(
    tmp_path: Path, monkeypatch
) -> None:
    password_key = "CTP_PREFLIGHT_PASSWORD_WINDOW_HINT"
    os.environ[password_key] = "secret"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://182.254.243.31:30011",
        trader_front="tcp://182.254.243.31:30001",
        password_env=password_key,
    )

    def _always_timeout(host: str, port: int, timeout_ms: int) -> tuple[bool, str]:
        _ = (host, port, timeout_ms)
        return False, "connect failed (timed out)"

    monkeypatch.setattr(ctp_preflight_module, "_check_tcp_connect", _always_timeout)

    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=100,
                skip_network_check=False,
            )
        )
    finally:
        os.environ.pop(password_key, None)

    assert report.ok is False
    assert any(item.name == "service_window_hint" and item.skipped for item in report.items)


def test_preflight_allows_sim_production_mode_for_simnow_trading_hours_fronts(
    tmp_path: Path,
) -> None:
    password_key = "CTP_PREFLIGHT_PASSWORD_SIM_PROD_OK"
    os.environ[password_key] = "secret"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://182.254.243.31:30011",
        trader_front="tcp://182.254.243.31:30001",
        password_env=password_key,
        is_production_mode=True,
    )
    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=100,
                skip_network_check=True,
            )
        )
    finally:
        os.environ.pop(password_key, None)

    assert report.ok is True
    assert any(item.name == "production_mode_guard" and item.ok for item in report.items)


def test_preflight_rejects_sim_production_mode_for_unknown_fronts(tmp_path: Path) -> None:
    password_key = "CTP_PREFLIGHT_PASSWORD_SIM_PROD_REJECT"
    os.environ[password_key] = "secret"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://127.0.0.1:35555",
        trader_front="tcp://127.0.0.1:36666",
        password_env=password_key,
        is_production_mode=True,
    )
    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=100,
                skip_network_check=True,
            )
        )
    finally:
        os.environ.pop(password_key, None)

    assert report.ok is False
    assert any(item.name == "production_mode_guard" and not item.ok for item in report.items)


def test_preflight_rejects_sim_trading_hours_fronts_when_production_mode_false(
    tmp_path: Path,
) -> None:
    password_key = "CTP_PREFLIGHT_PASSWORD_SIM_TRADE_HOURS_REJECT"
    os.environ[password_key] = "secret"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://182.254.243.31:30011",
        trader_front="tcp://182.254.243.31:30001",
        password_env=password_key,
        is_production_mode=False,
    )
    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=100,
                skip_network_check=True,
            )
        )
    finally:
        os.environ.pop(password_key, None)

    assert report.ok is False
    assert any(item.name == "production_mode_guard" and not item.ok for item in report.items)


def test_preflight_suggests_reachable_trading_hours_group(tmp_path: Path, monkeypatch) -> None:
    password_key = "CTP_PREFLIGHT_PASSWORD_GROUP_HINT"
    os.environ[password_key] = "secret"
    config_path = tmp_path / "ctp.yaml"
    ctp_lib_dir = tmp_path / "ctp_lib"
    _prepare_ctp_lib_dir(ctp_lib_dir)
    _write_config(
        config_path,
        market_front="tcp://182.254.243.31:30011",
        trader_front="tcp://182.254.243.31:30001",
        password_env=password_key,
    )

    def _timeout_with_group2_available(host: str, port: int, timeout_ms: int) -> tuple[bool, str]:
        _ = (host, timeout_ms)
        if port in {30002, 30012}:
            return True, "connected"
        return False, "connect failed (timed out)"

    monkeypatch.setattr(ctp_preflight_module, "_check_tcp_connect", _timeout_with_group2_available)

    try:
        report = run_ctp_preflight(
            CtpPreflightConfig(
                config_path=config_path,
                ctp_lib_dir=ctp_lib_dir,
                connect_timeout_ms=100,
                skip_network_check=False,
            )
        )
    finally:
        os.environ.pop(password_key, None)

    hint_items = [item for item in report.items if item.name == "service_window_hint"]
    assert len(hint_items) == 1
    assert "group2(30002/30012)" in hint_items[0].detail
