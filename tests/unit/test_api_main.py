from __future__ import annotations

import runpy
import sys
from typing import Any


def test_module_entrypoint_invokes_uvicorn_with_cli_defaults(
    monkeypatch: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setattr(sys, "argv", ["pms-api"])

    runpy.run_module("pms.api.__main__", run_name="__main__")

    assert calls == {
        "app": "pms.api.app:create_app",
        "factory": True,
        "host": "127.0.0.1",
        "port": 8000,
        "log_level": "info",
        "reload": False,
    }
