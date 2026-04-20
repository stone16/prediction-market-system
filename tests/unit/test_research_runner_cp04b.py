from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import re

from pms.research.entities import (
    PortfolioTarget,
    deserialize_portfolio_target_json,
    serialize_portfolio_target_json,
)


def test_portfolio_target_round_trips_through_json() -> None:
    target = PortfolioTarget(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        targets={
            ("market-a", "token-a", "buy_yes", datetime(2026, 4, 20, tzinfo=UTC)): 12.5,
            ("market-b", "token-b", "buy_no", datetime(2026, 4, 21, tzinfo=UTC)): 7.0,
        },
    )

    encoded = serialize_portfolio_target_json(target)
    decoded = deserialize_portfolio_target_json(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        raw_value=encoded,
    )

    assert decoded == target


def test_portfolio_target_round_trips_empty_targets() -> None:
    target = PortfolioTarget(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        targets={},
    )

    encoded = serialize_portfolio_target_json(target)
    decoded = deserialize_portfolio_target_json(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        raw_value=encoded,
    )

    assert decoded == target


def test_portfolio_target_remains_research_only() -> None:
    forbidden_pattern = re.compile(
        r"class PortfolioTarget\b|from pms\.research.*import.*PortfolioTarget"
    )

    for root in (
        Path("src/pms/controller"),
        Path("src/pms/actuator"),
        Path("src/pms/sensor"),
    ):
        for path in root.rglob("*.py"):
            assert forbidden_pattern.search(path.read_text(encoding="utf-8")) is None
