from __future__ import annotations

import pytest

from pms.storage.alembic_config import (
    AlembicDatabaseUrlError,
    resolve_alembic_database_url,
)


def test_resolve_alembic_database_url_prefers_database_url() -> None:
    resolved = resolve_alembic_database_url(
        {
            "DATABASE_URL": "postgresql://localhost/pms_database_url",
            "PMS_DATABASE_URL": "postgresql://localhost/pms_fallback",
        }
    )

    assert resolved == "postgresql+psycopg://localhost/pms_database_url"


def test_resolve_alembic_database_url_falls_back_to_pms_database_url() -> None:
    resolved = resolve_alembic_database_url(
        {
            "PMS_DATABASE_URL": "postgresql://localhost/pms_fallback",
        }
    )

    assert resolved == "postgresql+psycopg://localhost/pms_fallback"


def test_resolve_alembic_database_url_preserves_prefixed_sqlalchemy_url() -> None:
    resolved = resolve_alembic_database_url(
        {
            "DATABASE_URL": "postgresql+psycopg://localhost/pms_prefixed",
        }
    )

    assert resolved == "postgresql+psycopg://localhost/pms_prefixed"


def test_resolve_alembic_database_url_normalizes_postgres_alias() -> None:
    resolved = resolve_alembic_database_url(
        {
            "DATABASE_URL": "postgres://localhost/pms_alias",
        }
    )

    assert resolved == "postgresql+psycopg://localhost/pms_alias"


def test_resolve_alembic_database_url_requires_explicit_env_var() -> None:
    with pytest.raises(AlembicDatabaseUrlError) as exc_info:
        resolve_alembic_database_url({})

    assert "DATABASE_URL" in str(exc_info.value)
    assert "PMS_DATABASE_URL" in str(exc_info.value)
