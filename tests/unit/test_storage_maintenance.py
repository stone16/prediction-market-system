from __future__ import annotations

from datetime import timedelta

from pms.storage.maintenance import (
    MarketHistoryPrunePolicy,
    build_market_history_prune_plan,
)


def test_market_history_prune_plan_builds_bounded_cleanup_sql() -> None:
    plan = build_market_history_prune_plan(
        MarketHistoryPrunePolicy(
            truncate_market_price_snapshots=True,
            price_changes_retention=timedelta(days=7),
            vacuum_full=True,
        )
    )

    assert [statement.description for statement in plan] == [
        "truncate market_price_snapshots",
        "delete old price_changes",
        "vacuum market_price_snapshots",
        "vacuum price_changes",
    ]
    assert plan[0].sql == "TRUNCATE TABLE market_price_snapshots"
    assert "DELETE FROM price_changes" in plan[1].sql
    assert plan[1].args == (timedelta(days=7),)
    assert plan[2].sql == "VACUUM FULL market_price_snapshots"
    assert plan[2].runs_in_transaction is False
