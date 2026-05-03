"""Create a new immutable strategy_version that relaxes two required factors.

Flips `metaculus_prior` and `subset_pricing_violation` in the `default`
strategy's `factor_composition` from `required: true` to `required: false`,
and points `strategies.active_version_id` at the new version. Idempotent.
The old version row is preserved so the change is reversible.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import sys
from typing import Any

import psycopg

TARGET_FACTORS: frozenset[str] = frozenset(
    {"metaculus_prior", "subset_pricing_violation"}
)


def _compute_version_id(config_payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        config_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _relax(config: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Return (relaxed config, list of factor_ids whose required flag flipped)."""
    relaxed = copy.deepcopy(config)
    flipped: list[str] = []
    factors = relaxed["config"]["factor_composition"]
    for factor in factors:
        if (
            factor.get("factor_id") in TARGET_FACTORS
            and factor.get("required", True)
        ):
            factor["required"] = False
            flipped.append(factor["factor_id"])
    return relaxed, flipped


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("error: DATABASE_URL is not set", file=sys.stderr)
        return 2

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
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
        if row is None:
            print(
                "error: default strategy not found", file=sys.stderr
            )
            return 3
        old_version_id: str
        config_json: dict[str, Any]
        old_version_id, config_json = row[0], row[1]

        # Verify the target factors exist (defends against future seed
        # changes that rename or remove them silently).
        present = {
            f["factor_id"]
            for f in config_json["config"]["factor_composition"]
        }
        missing = TARGET_FACTORS - present
        if missing:
            print(
                f"error: target factors missing from seed: {sorted(missing)}",
                file=sys.stderr,
            )
            return 4

        relaxed_config, flipped = _relax(config_json)
        if not flipped:
            print("already relaxed: no change applied")
            return 0

        new_version_id = _compute_version_id(relaxed_config)
        if new_version_id == old_version_id:
            # Belt-and-suspenders: the canonical JSON of the relaxed config
            # produced the same id, which would only happen if the helper's
            # hashing scheme drifts from the seed's. Refuse rather than
            # silently corrupt.
            print(
                "error: relaxed config hashes to the SAME id as the seed; "
                "hashing scheme drift suspected — aborting",
                file=sys.stderr,
            )
            return 5

        # INSERT new version + UPDATE active_version_id in one transaction.
        cur.execute(
            """
            INSERT INTO strategy_versions
                (strategy_version_id, strategy_id, config_json)
            VALUES (%s, 'default', %s::jsonb)
            ON CONFLICT (strategy_version_id) DO NOTHING
            """,
            (new_version_id, json.dumps(relaxed_config)),
        )
        cur.execute(
            """
            UPDATE strategies SET active_version_id = %s
            WHERE strategy_id = 'default'
            """,
            (new_version_id,),
        )
        conn.commit()

    print(f"old_version_id: {old_version_id}")
    print(f"new_version_id: {new_version_id}")
    print(f"flipped: {sorted(flipped)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
