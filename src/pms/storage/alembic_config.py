from __future__ import annotations

import os
from collections.abc import Mapping


class AlembicDatabaseUrlError(RuntimeError):
    """Raised when Alembic cannot resolve a database URL from the environment."""


_ALEMBIC_DATABASE_URL_ENV_VARS = (
    "DATABASE_URL",
    "PMS_DATABASE__DSN",
    "PMS_DATABASE_URL",
)


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
    for key in _ALEMBIC_DATABASE_URL_ENV_VARS:
        value = environment.get(key)
        if value is not None and value.strip():
            return _normalize_sqlalchemy_url(value)

    msg = (
        "Database URL is not configured. Set one of DATABASE_URL, "
        "PMS_DATABASE__DSN, or PMS_DATABASE_URL before running Alembic commands."
    )
    raise AlembicDatabaseUrlError(msg)
