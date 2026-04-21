from __future__ import annotations

import os
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool

from pms.storage.alembic_config import resolve_alembic_database_url

target_metadata = None


def _config() -> Any:
    return context.config


def _configure_logging() -> None:
    config = _config()
    if config.config_file_name is not None:
        fileConfig(config.config_file_name)


def _configured_section() -> dict[str, Any]:
    database_url = resolve_alembic_database_url()
    config = _config()
    config.set_main_option("sqlalchemy.url", database_url)
    return config.get_section(config.config_ini_section, {}) or {}


def run_migrations_offline() -> None:
    database_url = resolve_alembic_database_url()
    config = _config()
    config.set_main_option("sqlalchemy.url", database_url)
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        _configured_section(),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


def run_migrations() -> None:
    _configure_logging()
    if context.is_offline_mode():
        run_migrations_offline()
    else:
        run_migrations_online()


if os.environ.get("PMS_SKIP_ALEMBIC_ENV_AUTO_RUN") != "1":
    run_migrations()
