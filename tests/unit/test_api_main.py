from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pms.api.app import create_app
import pms.api.__main__ as api_main
from pms.core.enums import RunMode


def test_main_invokes_uvicorn_with_settings_host_and_cli_defaults(
    monkeypatch: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.delenv("PMS_API_HOST", raising=False)
    monkeypatch.delenv("PMS_API_TOKEN", raising=False)

    exit_code = api_main.main([])

    assert exit_code == 0

    assert calls == {
        "app": "pms.api.app:create_app",
        "factory": True,
        "host": "127.0.0.1",
        "port": 8000,
        "log_level": "info",
        "reload": False,
    }


def test_main_refuses_non_loopback_bind_without_api_token(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_API_HOST", "0.0.0.0")
    monkeypatch.delenv("PMS_API_TOKEN", raising=False)

    exit_code = api_main.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert calls == {}
    assert "PMS_API_TOKEN" in captured.err
    assert "PMS_API_HOST" in captured.err


def test_main_uses_settings_host_even_when_host_flag_is_passed(
    monkeypatch: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_API_HOST", "0.0.0.0")
    monkeypatch.setenv("PMS_API_TOKEN", "cp18-token")

    exit_code = api_main.main(["--host", "127.0.0.1", "--port", "9000"])

    assert exit_code == 0
    assert calls["host"] == "0.0.0.0"
    assert calls["port"] == 9000


def test_main_loads_config_file_and_exports_path_for_app_factory(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    calls: dict[str, Any] = {}
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "api_host: 127.0.0.1",
                "mode: paper",
                "risk:",
                "  max_total_exposure: 50.0",
            ]
        ),
        encoding="utf-8",
    )

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.delenv("PMS_CONFIG_PATH", raising=False)

    exit_code = api_main.main(["--config", str(config_path)])

    assert exit_code == 0
    assert calls["host"] == "127.0.0.1"
    assert calls["app"] == "pms.api.app:create_app"
    assert calls["factory"] is True
    assert calls["reload"] is False
    assert calls["log_level"] == "info"
    assert calls["port"] == 8000
    assert os.environ["PMS_CONFIG_PATH"] == str(config_path)


def test_create_app_uses_config_path_for_default_runner(tmp_path: Path) -> None:
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "risk:",
                "  max_position_per_market: 5.0",
                "  max_total_exposure: 50.0",
            ]
        ),
        encoding="utf-8",
    )

    app = create_app(auto_start=False, config_path=str(config_path))

    assert app.state.runner.config.mode is RunMode.PAPER
    assert app.state.runner.config.risk.max_position_per_market == 5.0
    assert app.state.runner.config.risk.max_total_exposure == 50.0
