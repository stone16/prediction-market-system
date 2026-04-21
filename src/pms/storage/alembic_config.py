from __future__ import annotations

import os
from collections.abc import Mapping


class AlembicDatabaseUrlError(RuntimeError):
    """Raised when Alembic cannot resolve a database URL from the environment."""


def _normalize_sqlalchemy_url(database_url: str) -> str:
    if database_url.startswith("postgresql+"):
        return database_url
    if database_url.startswith("postgresql://"):
        return "postgresql+psycopg://" + database_url.removeprefix("postgresql://")
    if database_url.startswith("postgres://"):
        return "postgresql+psycopg://" + database_url.removeprefix("postgres://")
    return database_url


def resolve_alembic_database_url(env: Mapping[str, str] | None = None) -> str:
    environment = os.environ if env is None else env
    for key in ("DATABASE_URL", "PMS_DATABASE_URL"):
        value = environment.get(key)
        if value is not None and value.strip():
            return _normalize_sqlalchemy_url(value)

    msg = (
        "DATABASE_URL or PMS_DATABASE_URL must be set before running Alembic "
        "commands"
    )
    raise AlembicDatabaseUrlError(msg)
