from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


ALEMBIC_ENV_PATH = Path(__file__).resolve().parents[2] / "alembic" / "env.py"


class _FakeConfig:
    def __init__(self) -> None:
        self.config_file_name: str | None = None
        self.config_ini_section = "alembic"
        self.main_options: dict[str, str] = {}

    def set_main_option(self, key: str, value: str) -> None:
        self.main_options[key] = value

    def get_section(
        self,
        section: str,
        default: dict[str, Any],
    ) -> dict[str, Any]:
        assert section == "alembic"
        merged = dict(default)
        merged.update(self.main_options)
        return merged


class _FakeTransaction:
    def __init__(self, calls: list[Any]) -> None:
        self._calls = calls

    def __enter__(self) -> "_FakeTransaction":
        self._calls.append("transaction-enter")
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        self._calls.append("transaction-exit")


class _FakeConnection:
    def __init__(self, calls: list[Any]) -> None:
        self._calls = calls

    def __enter__(self) -> "_FakeConnection":
        self._calls.append("connection-enter")
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        self._calls.append("connection-exit")


class _FakeConnectable:
    def __init__(self, calls: list[Any]) -> None:
        self._calls = calls

    def connect(self) -> _FakeConnection:
        self._calls.append("connect")
        return _FakeConnection(self._calls)


def _load_alembic_env(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv("PMS_SKIP_ALEMBIC_ENV_AUTO_RUN", "1")
    module_name = "test_alembic_env_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, ALEMBIC_ENV_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_run_migrations_offline_uses_resolved_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_alembic_env(monkeypatch)
    calls: list[object] = []
    fake_config = _FakeConfig()

    def fake_configure(**kwargs: object) -> None:
        calls.append(("configure", kwargs))

    setattr(
        module,
        "context",
        SimpleNamespace(
        config=fake_config,
        configure=fake_configure,
        begin_transaction=lambda: _FakeTransaction(calls),
        run_migrations=lambda: calls.append("run-migrations"),
        is_offline_mode=lambda: True,
        ),
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/pms_offline")

    module.run_migrations_offline()

    assert module.target_metadata is None
    assert (
        fake_config.main_options["sqlalchemy.url"]
        == "postgresql+psycopg://localhost/pms_offline"
    )
    assert calls[0] == (
        "configure",
        {
            "url": "postgresql+psycopg://localhost/pms_offline",
            "target_metadata": None,
            "literal_binds": True,
            "dialect_opts": {"paramstyle": "named"},
        },
    )
    assert "run-migrations" in calls
    assert calls[-2] == "run-migrations"
    assert calls[-1] == "transaction-exit"


def test_run_migrations_online_builds_engine_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_alembic_env(monkeypatch)
    calls: list[Any] = []
    fake_config = _FakeConfig()

    def fake_configure(**kwargs: object) -> None:
        calls.append(("configure", kwargs))

    setattr(
        module,
        "context",
        SimpleNamespace(
        config=fake_config,
        configure=fake_configure,
        begin_transaction=lambda: _FakeTransaction(calls),
        run_migrations=lambda: calls.append("run-migrations"),
        is_offline_mode=lambda: False,
        ),
    )

    def fake_engine_from_config(
        section: dict[str, Any],
        *,
        prefix: str,
        poolclass: type[object],
    ) -> _FakeConnectable:
        calls.append(("engine", section, prefix, poolclass))
        return _FakeConnectable(calls)

    setattr(module, "engine_from_config", fake_engine_from_config)
    monkeypatch.setenv("PMS_DATABASE_URL", "postgresql://localhost/pms_online")

    module.run_migrations_online()

    assert (
        fake_config.main_options["sqlalchemy.url"]
        == "postgresql+psycopg://localhost/pms_online"
    )
    assert calls[0][0] == "engine"
    assert calls[0][1]["sqlalchemy.url"] == "postgresql+psycopg://localhost/pms_online"
    assert calls[1] == "connect"
    assert calls[2] == "connection-enter"
    assert calls[3][0] == "configure"
    assert isinstance(calls[3][1]["connection"], _FakeConnection)
    assert calls[3][1]["target_metadata"] is None
    assert "run-migrations" in calls
    assert calls[-3] == "run-migrations"
    assert calls[-2] == "transaction-exit"
    assert calls[-1] == "connection-exit"


def test_configure_logging_uses_config_file_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_alembic_env(monkeypatch)
    calls: list[str] = []
    fake_config = _FakeConfig()
    fake_config.config_file_name = "alembic.ini"
    setattr(module, "context", SimpleNamespace(config=fake_config))
    setattr(module, "fileConfig", lambda path: calls.append(path))

    module._configure_logging()

    assert calls == ["alembic.ini"]


def test_run_migrations_dispatches_to_offline_and_online_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_alembic_env(monkeypatch)
    calls: list[str] = []
    fake_config = _FakeConfig()

    def fake_configure_logging() -> None:
        calls.append("logging")

    setattr(module, "_configure_logging", fake_configure_logging)
    setattr(module, "run_migrations_offline", lambda: calls.append("offline"))
    setattr(module, "run_migrations_online", lambda: calls.append("online"))

    setattr(
        module,
        "context",
        SimpleNamespace(
            config=fake_config,
            is_offline_mode=lambda: True,
        ),
    )
    module.run_migrations()

    setattr(
        module,
        "context",
        SimpleNamespace(
            config=fake_config,
            is_offline_mode=lambda: False,
        ),
    )
    module.run_migrations()

    assert calls == ["logging", "offline", "logging", "online"]
