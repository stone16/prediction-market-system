from __future__ import annotations

import importlib
import platform
import sys
import types

import pytest

import sitecustomize


def test_sitecustomize_stubs_readline_only_for_pytest_on_darwin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    original_readline = sys.modules.pop("readline", None)
    try:
        monkeypatch.setattr(sys, "argv", ["pms-api"])
        importlib.reload(sitecustomize)
        assert "readline" not in sys.modules

        monkeypatch.setattr(sys, "argv", ["pytest", "-q"])
        importlib.reload(sitecustomize)
        assert isinstance(sys.modules.get("readline"), types.ModuleType)
    finally:
        sys.modules.pop("readline", None)
        if original_readline is not None:
            sys.modules["readline"] = original_readline
        importlib.reload(sitecustomize)
