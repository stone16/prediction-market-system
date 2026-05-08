from __future__ import annotations

from collections.abc import Mapping
import os


def resolve_alembic_database_url(
    env: Mapping[str, str] | None = None,
) -> str:
    source = os.environ if env is None else env
    for variable_name in ("DATABASE_URL", "PMS_DATABASE__DSN", "PMS_DATABASE_URL"):
        database_url = source.get(variable_name)
        if database_url:
            return database_url

    msg = (
        "Database URL is not configured. Set one of DATABASE_URL, "
        "PMS_DATABASE__DSN, or PMS_DATABASE_URL before running alembic."
    )
    raise RuntimeError(msg)
