from __future__ import annotations

from typing import Any

import pytest

from pms.api.routes.share import ShareResponse, public_strategy_projection


class _ConnectionDouble:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.row = row
        self.queries: list[str] = []

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        del args
        self.queries.append(query)
        return self.row


@pytest.mark.asyncio
async def test_public_strategy_projection_returns_exact_allowlist_fields() -> None:
    connection = _ConnectionDouble(
        {
            "strategy_id": "alpha",
            "title": "Alpha Theory",
            "description": "Buy dislocations when liquidity is deep.",
            "brier_overall": 0.125,
            "trade_count": 3,
            "version_id_short": "alpha-v1",
            "api_key": "SECRET_AK",
            "private_key": "SECRET_PK",
            "metadata_json": {"owner": "ops"},
        }
    )

    payload = await public_strategy_projection(connection, "alpha")

    assert payload == ShareResponse(
        strategy_id="alpha",
        title="Alpha Theory",
        description="Buy dislocations when liquidity is deep.",
        brier_overall=0.125,
        trade_count=3,
        version_id_short="alpha-v1",
    )
    assert set(payload.model_dump(mode="json")) == {
        "strategy_id",
        "title",
        "description",
        "brier_overall",
        "trade_count",
        "version_id_short",
    }
    assert "metadata_json" not in connection.queries[0]
    assert "config_json" not in connection.queries[0]


@pytest.mark.asyncio
async def test_public_strategy_projection_returns_none_for_missing_strategy() -> None:
    connection = _ConnectionDouble(None)

    payload = await public_strategy_projection(connection, "missing")

    assert payload is None

