from __future__ import annotations

from collections.abc import Sequence

import asyncpg


FACTOR_CATALOG_ROWS: tuple[tuple[str, str, str, str, str, str, str], ...] = (
    (
        "orderbook_imbalance",
        "Orderbook Imbalance",
        "Normalized bid-versus-ask depth imbalance from the current orderbook signal.",
        "97e885bf8b2edd8ce9fff149334dbe1706358eb4fb8b8c51a4b42561878c5963",
        "scalar",
        "neutral",
        "system",
    ),
    (
        "fair_value_spread",
        "Fair Value Spread",
        "Signed difference between external fair value and the current YES price.",
        "adb923abb80bbd30efa4db61ba846660317f138ef12c3ae521891df2831d64f9",
        "scalar",
        "neutral",
        "system",
    ),
    (
        "subset_pricing_violation",
        "Subset Pricing Violation",
        "Signed difference between subset and superset prices from external signals.",
        "c9e66b836e6fe6a9981ee6419aa38acb39de607e84fb1ff643b46bb9ac446891",
        "scalar",
        "neutral",
        "system",
    ),
    (
        "metaculus_prior",
        "Metaculus Prior",
        "Raw Metaculus probability from the external signal payload.",
        "4f62fec15fd5abaf2ff76810596268d1e14b46d346ff6e9f38b259c370a3ed71",
        "scalar",
        "neutral",
        "system",
    ),
    (
        "yes_count",
        "Yes Count",
        "Raw external yes_count observation count from the signal payload.",
        "afbc921285acc81f1289beca8dd64114c18f49068a8904c651a887c5ba8c178f",
        "scalar",
        "neutral",
        "system",
    ),
    (
        "no_count",
        "No Count",
        "Raw external no_count observation count from the signal payload.",
        "2871d6bf945e3ed4407b8b1f1beeb484cd8bd455a156e939094fbcc6a455c317",
        "scalar",
        "neutral",
        "system",
    ),
)


async def seed_factor_catalog(
    connection: asyncpg.Connection,
    *,
    factor_ids: Sequence[str] | None = None,
) -> None:
    available_rows = {row[0]: row for row in FACTOR_CATALOG_ROWS}
    selected_ids = tuple(factor_ids) if factor_ids is not None else tuple(available_rows)
    missing_ids = sorted(set(selected_ids) - set(available_rows))
    if missing_ids:
        msg = f"unknown factor catalog ids: {', '.join(missing_ids)}"
        raise ValueError(msg)

    insert_query = """
    INSERT INTO factors (
        factor_id,
        name,
        description,
        input_schema_hash,
        output_type,
        direction,
        owner
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (factor_id) DO NOTHING
    """
    for factor_id in selected_ids:
        await connection.execute(insert_query, *available_rows[factor_id])
