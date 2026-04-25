"""Tier 3 A — portfolio reconciliation on Runner.start.

Verifies the in-memory portfolio is rebuilt from persisted fills before
the runner accepts new decisions. Without this, restarting in LIVE mode
forgets all open Polymarket exposure.
"""

from __future__ import annotations

import logging
from typing import Any, cast

import pytest

from pms.core.enums import Venue
from pms.core.models import Position
from pms.runner import Runner
from pms.storage.fill_store import FillStore


class _FakeFillStore(FillStore):
    """FillStore subclass that returns a canned list of positions.

    Subclassing (not duck-typing) keeps `isinstance(fill_store, FillStore)`
    runtime checks passing without binding to a real PG pool.
    """

    def __init__(self, positions: list[Position]) -> None:
        super().__init__()
        self._positions = positions

    async def read_positions(self) -> list[Position]:
        return list(self._positions)


def _position(
    *,
    market_id: str = "m-1",
    token_id: str = "t-1",
    side: str = "BUY",
    shares_held: float = 100.0,
    avg_entry_price: float = 0.5,
    locked_usdc: float = 50.0,
) -> Position:
    return Position(
        market_id=market_id,
        token_id=token_id,
        venue=Venue.POLYMARKET.value,
        side=side,
        shares_held=shares_held,
        avg_entry_price=avg_entry_price,
        unrealized_pnl=0.0,
        locked_usdc=locked_usdc,
    )


def _runner_with_fake_pool(fill_store: FillStore) -> Runner:
    runner = Runner(fill_store=fill_store)
    # The reconciliation method bails out when `_pg_pool is None` (BACKTEST
    # without DB). Setting a sentinel keeps the path live; the FakeFillStore
    # never actually queries the pool.
    runner._pg_pool = cast(Any, object())  # noqa: SLF001
    return runner


@pytest.mark.asyncio
async def test_reconcile_rebuilds_portfolio_from_persisted_fills(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Persisted positions become the runner's in-memory portfolio."""
    positions = [
        _position(market_id="m-a", locked_usdc=100.0, shares_held=200.0),
        _position(market_id="m-b", locked_usdc=250.0, shares_held=500.0),
    ]
    runner = _runner_with_fake_pool(_FakeFillStore(positions))
    caplog.set_level(logging.INFO, logger="pms.runner")

    assert runner.portfolio.locked_usdc == 0.0
    assert runner.portfolio.free_usdc == 1000.0

    await runner._reconcile_portfolio_from_db()  # noqa: SLF001

    assert runner.portfolio.locked_usdc == pytest.approx(350.0)
    assert runner.portfolio.free_usdc == pytest.approx(650.0)
    assert runner.portfolio.total_usdc == 1000.0
    assert len(runner.portfolio.open_positions) == 2
    assert {p.market_id for p in runner.portfolio.open_positions} == {"m-a", "m-b"}
    assert any(
        "Reconciled portfolio from DB" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_reconcile_clamps_free_when_locked_exceeds_total_budget(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If reconciled exposure exceeds the budget, free is clamped to 0
    and the warning surfaces so operators tighten caps before resuming."""
    over_cap = [_position(market_id="m-over", locked_usdc=1500.0, shares_held=3000.0)]
    runner = _runner_with_fake_pool(_FakeFillStore(over_cap))
    caplog.set_level(logging.WARNING, logger="pms.runner")

    await runner._reconcile_portfolio_from_db()  # noqa: SLF001

    assert runner.portfolio.locked_usdc == pytest.approx(1500.0)
    assert runner.portfolio.free_usdc == 0.0
    assert any(
        "exceeds total budget" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_reconcile_filters_zero_share_positions() -> None:
    """Closed positions (shares_held=0) must NOT be carried into the
    in-memory portfolio. They typically appear when BUYs and SELLs
    cancel each other in the aggregation."""
    positions = [
        _position(market_id="m-open", locked_usdc=100.0, shares_held=200.0),
        _position(market_id="m-closed", locked_usdc=0.0, shares_held=0.0),
    ]
    runner = _runner_with_fake_pool(_FakeFillStore(positions))

    await runner._reconcile_portfolio_from_db()  # noqa: SLF001

    assert len(runner.portfolio.open_positions) == 1
    assert runner.portfolio.open_positions[0].market_id == "m-open"
    assert runner.portfolio.locked_usdc == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_reconcile_no_op_without_pg_pool() -> None:
    """BACKTEST mode without a DB must skip reconciliation cleanly."""
    runner = Runner(fill_store=_FakeFillStore([_position(locked_usdc=500.0)]))
    # Leave _pg_pool as None — the BACKTEST default.

    await runner._reconcile_portfolio_from_db()  # noqa: SLF001

    # Portfolio remains at its hardcoded default — no DB read happened.
    assert runner.portfolio.locked_usdc == 0.0
    assert runner.portfolio.free_usdc == 1000.0
    assert runner.portfolio.open_positions == []


@pytest.mark.asyncio
async def test_reconcile_swallows_fill_store_errors_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A flaky DB read at boot should not prevent the runner from
    starting — log a warning, leave portfolio at default, continue."""

    class _BrokenFillStore(FillStore):
        async def read_positions(self) -> list[Position]:
            raise RuntimeError("simulated PG error")

    runner = _runner_with_fake_pool(_BrokenFillStore())
    caplog.set_level(logging.WARNING, logger="pms.runner")

    await runner._reconcile_portfolio_from_db()  # noqa: SLF001

    assert runner.portfolio.locked_usdc == 0.0
    assert any(
        "portfolio reconciliation failed" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_reconcile_no_op_when_no_persisted_positions() -> None:
    """Empty fills table → no-op (preserves the hardcoded default)."""
    runner = _runner_with_fake_pool(_FakeFillStore([]))

    await runner._reconcile_portfolio_from_db()  # noqa: SLF001

    assert runner.portfolio.locked_usdc == 0.0
    assert runner.portfolio.free_usdc == 1000.0
    assert runner.portfolio.open_positions == []


@pytest.mark.asyncio
async def test_reconcile_fails_closed_in_live_mode_on_db_error() -> None:
    """SECURITY: in LIVE mode, swallowing DB errors and continuing with
    the default `$1000 free` portfolio recreates the exact restart-
    exposure bug this method exists to prevent. Codex round-1 finding f1.
    """
    from pms.config import PMSSettings
    from pms.core.enums import RunMode

    class _BrokenFillStore(FillStore):
        async def read_positions(self) -> list[Position]:
            raise RuntimeError("simulated PG error")

    runner = Runner(
        config=PMSSettings(mode=RunMode.LIVE),
        fill_store=_BrokenFillStore(),
    )
    runner._pg_pool = cast(Any, object())  # noqa: SLF001

    with pytest.raises(RuntimeError, match="LIVE portfolio reconciliation failed"):
        await runner._reconcile_portfolio_from_db()  # noqa: SLF001


@pytest.mark.asyncio
async def test_reconcile_paper_mode_still_swallows_errors() -> None:
    """In PAPER and BACKTEST, a transient read failure should not
    prevent the runner from starting — the in-memory portfolio simply
    stays at its default. Only LIVE fails closed."""
    from pms.config import PMSSettings
    from pms.core.enums import RunMode

    class _BrokenFillStore(FillStore):
        async def read_positions(self) -> list[Position]:
            raise RuntimeError("simulated PG error")

    runner = Runner(
        config=PMSSettings(mode=RunMode.PAPER),
        fill_store=_BrokenFillStore(),
    )
    runner._pg_pool = cast(Any, object())  # noqa: SLF001

    # Must not raise.
    await runner._reconcile_portfolio_from_db()  # noqa: SLF001
    assert runner.portfolio.locked_usdc == 0.0


@pytest.mark.asyncio
async def test_reconcile_clears_stale_portfolio_when_db_returns_empty(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Regression for CodeRabbit Major: a stop/start cycle on a runner
    that previously held positions must NOT preserve stale `locked_usdc`,
    `free_usdc`, or `open_positions` when reconciliation finds the DB
    is empty.

    Pre-fix: the empty-positions fast path returned early without
    clearing state, so a runner that held $50 locked / 1 position would
    still report that exposure on a subsequent restart even after every
    fill closed and the FillStore returned [].
    """
    from dataclasses import replace

    from pms.core.models import Portfolio

    runner = _runner_with_fake_pool(_FakeFillStore([]))
    # Simulate a runner that previously held positions: stale state from
    # a prior reconciliation cycle.
    stale_position = _position(market_id="m-stale", locked_usdc=50.0)
    runner.portfolio = replace(
        runner.portfolio,
        locked_usdc=50.0,
        free_usdc=950.0,
        open_positions=[stale_position],
    )
    caplog.set_level(logging.INFO, logger="pms.runner")

    await runner._reconcile_portfolio_from_db()  # noqa: SLF001

    assert runner.portfolio.locked_usdc == 0.0
    assert runner.portfolio.free_usdc == runner.portfolio.total_usdc
    assert runner.portfolio.open_positions == []
    # Single Portfolio invariant: total_usdc unchanged.
    assert isinstance(runner.portfolio, Portfolio)
    # Operator visibility: empty reconciliation should still log so a
    # human can see "the runner reconciled from DB and found nothing".
    assert any(
        "0 positions" in record.getMessage() for record in caplog.records
    )
