from __future__ import annotations

import pytest

from pms.storage.alembic_env import resolve_alembic_database_url


def test_resolve_alembic_database_url_prefers_database_url() -> None:
    env = {
        "DATABASE_URL": "postgresql://primary/pms",
        "PMS_DATABASE__DSN": "postgresql://nested/pms",
        "PMS_DATABASE_URL": "postgresql://legacy/pms",
    }

    assert resolve_alembic_database_url(env) == "postgresql+psycopg://primary/pms"


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        (
            {
                "DATABASE_URL": "",
                "PMS_DATABASE__DSN": "postgresql://nested/pms",
            },
            "postgresql+psycopg://nested/pms",
        ),
        (
            {
                "DATABASE_URL": "",
                "PMS_DATABASE_URL": "postgresql://legacy/pms",
            },
            "postgresql+psycopg://legacy/pms",
        ),
        (
            {"PMS_DATABASE__DSN": "postgresql://nested/pms"},
            "postgresql+psycopg://nested/pms",
        ),
        (
            {"PMS_DATABASE_URL": "postgresql://legacy/pms"},
            "postgresql+psycopg://legacy/pms",
        ),
    ],
)
def test_resolve_alembic_database_url_uses_supported_fallbacks(
    env: dict[str, str],
    expected: str,
) -> None:
    assert resolve_alembic_database_url(env) == expected


def test_resolve_alembic_database_url_reports_supported_env_vars() -> None:
    with pytest.raises(RuntimeError) as exc_info:
        resolve_alembic_database_url({})

    assert str(exc_info.value) == (
        "Database URL is not configured. Set one of DATABASE_URL, "
        "PMS_DATABASE__DSN, or PMS_DATABASE_URL before running Alembic commands."
    )
