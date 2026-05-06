from __future__ import annotations

import tomllib
from pathlib import Path


def test_fly_binds_api_host_via_env_and_checks_readiness() -> None:
    fly_config = tomllib.loads(Path("fly.toml").read_text(encoding="utf-8"))

    assert fly_config["env"]["PMS_API_HOST"] == "0.0.0.0"
    assert fly_config["http_service"]["checks"][0]["path"] == "/readiness"


def test_docker_cmd_uses_pms_api_host_env_not_deprecated_host_flag() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

    assert "--host" not in dockerfile
    assert "pms-api" in dockerfile
    assert "COPY alembic.ini ./alembic.ini" in dockerfile
    assert "COPY alembic ./alembic" in dockerfile
