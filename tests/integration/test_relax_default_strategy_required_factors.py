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


@pytest.fixture(autouse=True)
def reset_to_seed() -> None:
    """Reset strategies.active_version_id to a pre-relaxed seed version so
    each test starts from the same DB state. Skips the test if no seed
    version with both required factors set to true exists in the DB."""
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT strategy_version_id, config_json
            FROM strategy_versions
            WHERE strategy_id = 'default'
            """
        )
        seed_id: str | None = None
        for row in cur.fetchall():
            version_id: str = row[0]
            config: dict[str, Any] = row[1]
            flags = _required_flags(config)
            if flags.get("metaculus_prior") is True and flags.get(
                "subset_pricing_violation"
            ) is True:
                seed_id = version_id
                break
        if seed_id is None:
            pytest.skip(
                "no pre-relaxed seed version present; "
                "run alembic upgrade and seed the default strategy first"
            )
        cur.execute(
            "UPDATE strategies SET active_version_id = %s WHERE strategy_id = 'default'",
            (seed_id,),
        )
        conn.commit()


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
    """First call relaxes; second call should be a no-op printing 'already relaxed'."""
    proc1 = _run_script()
    assert proc1.returncode == 0, proc1.stderr
    # proc1 either relaxed (fresh seed) OR was already relaxed (post-relax DB).
    # In either case, proc2 must be the explicit no-op path.
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        first_id, _ = _active_config(cur)

    proc2 = _run_script()
    assert proc2.returncode == 0, proc2.stderr
    assert "already relaxed" in proc2.stdout, (
        f"second run should print 'already relaxed', got stdout: {proc2.stdout!r}"
    )
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        second_id, _ = _active_config(cur)
    assert first_id == second_id, "re-run must not create a new version"
