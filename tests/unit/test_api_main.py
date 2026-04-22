from __future__ import annotations

from typing import Any

import pms.api.__main__ as api_main


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
