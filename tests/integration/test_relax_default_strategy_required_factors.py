from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "relax_default_strategy_required_factors.py"

pytestmark = pytest.mark.integration

if not os.environ.get("PMS_RUN_INTEGRATION"):
    pytest.skip(
        "set PMS_RUN_INTEGRATION=1 to run integration tests",
        allow_module_level=True,
    )


def _dsn() -> str:
    return os.environ.get(
        "PMS_TEST_DATABASE_URL",
        "postgres://postgres:postgres@localhost:5432/pms_test",
    )


def _active_config(cur: psycopg.Cursor[Any]) -> tuple[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT s.active_version_id, sv.config_json
        FROM strategies s
        JOIN strategy_versions sv
          ON sv.strategy_version_id = s.active_version_id
        WHERE s.strategy_id = 'default'
        """
    )
    row = cur.fetchone()
    assert row is not None, "default strategy not seeded"
    return row[0], row[1]


def _required_flags(config: dict[str, Any]) -> dict[str, bool]:
    factors = config["config"]["factor_composition"]
    return {
        f["factor_id"]: f["required"]
        for f in factors
        if f["factor_id"] in {
            "metaculus_prior", "subset_pricing_violation"
        }
    }


def _run_script() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        capture_output=True,
        text=True,
        env={**os.environ, "DATABASE_URL": _dsn()},
        check=False,
    )


def test_relax_script_creates_new_version_and_flips_required_flags() -> None:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        old_id, old_cfg = _active_config(cur)
        old_flags = _required_flags(old_cfg)
        assert old_flags == {
            "metaculus_prior": True,
            "subset_pricing_violation": True,
        }

    proc = _run_script()
    assert proc.returncode == 0, proc.stderr

    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        new_id, new_cfg = _active_config(cur)
        assert new_id != old_id
        new_flags = _required_flags(new_cfg)
        assert new_flags == {
            "metaculus_prior": False,
            "subset_pricing_violation": False,
        }
        # old version row preserved (reversibility)
        cur.execute(
            "SELECT 1 FROM strategy_versions WHERE strategy_version_id = %s",
            (old_id,),
        )
        assert cur.fetchone() is not None


def test_relax_script_is_idempotent_on_rerun() -> None:
    # Assumes previous test ran first and the relaxation is in place.
    proc1 = _run_script()
    assert proc1.returncode == 0
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        first_id, _ = _active_config(cur)

    proc2 = _run_script()
    assert proc2.returncode == 0
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        second_id, _ = _active_config(cur)
    assert first_id == second_id, "re-run must not create a new version"
