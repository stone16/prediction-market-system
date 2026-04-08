"""Tests for OrderExecutor (CP08).

Covers every CP08 executor acceptance criterion:

1. ``submit_order`` routes to the correct submit handler based on
   ``order.platform``.
2. Unknown platforms return an error ``OrderResult`` (not an exception).
3. Orders submitted with an empty ``order_id`` receive a generated
   client-side id prefixed with ``"pms-"``.
4. Successful submissions are tracked via ``submitted_order_ids()``.
5. Transient failures (``ConnectionError``, ``asyncio.TimeoutError``) are
   retried up to ``max_retries`` with exponential backoff.
6. Backoff delays grow multiplicatively — verified via injected ``sleep_fn``.
7. Idempotency: before retrying, the executor consults ``status_fn``; a
   filled status short-circuits and returns the existing fill.
8. Non-transient errors (``ValueError`` etc.) are not retried.
9. Max retries exhausted returns an error ``OrderResult``.
10. ``get_positions`` aggregates from every registered source and tolerates
    individual source failures.
11. ``cancel_order`` returns ``False`` in v1 (placeholder).

All financial math uses ``Decimal`` per CP01.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from pms.execution.executor import OrderExecutor
from pms.models import Order, OrderResult, Position


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _order(
    platform: str = "polymarket",
    order_id: str = "",
    size: Decimal = Decimal("10"),
    price: Decimal = Decimal("0.50"),
    market_id: str = "m-1",
) -> Order:
    return Order(
        order_id=order_id,
        platform=platform,
        market_id=market_id,
        outcome_id="yes",
        side="buy",
        price=price,
        size=size,
        order_type="limit",
    )


def _result(
    order_id: str,
    status: str = "filled",
    filled_size: Decimal = Decimal("10"),
    filled_price: Decimal = Decimal("0.50"),
    message: str = "ok",
) -> OrderResult:
    return OrderResult(
        order_id=order_id,
        status=status,  # type: ignore[arg-type]
        filled_size=filled_size,
        filled_price=filled_price,
        message=message,
        raw={},
    )


def _position(platform: str, market_id: str) -> Position:
    return Position(
        platform=platform,
        market_id=market_id,
        outcome_id="yes",
        size=Decimal("1"),
        avg_entry_price=Decimal("0.50"),
        unrealized_pnl=Decimal("0"),
    )


class _NoopSleep:
    """Records every sleep-call so we can assert exponential growth."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


# ---------------------------------------------------------------------------
# Routing + unknown platform
# ---------------------------------------------------------------------------


async def test_submit_order_routes_to_correct_platform() -> None:
    """submit_order dispatches to the registered submit_fn for the platform."""
    poly_calls: list[Order] = []
    kalshi_calls: list[Order] = []

    async def poly_submit(order: Order) -> OrderResult:
        poly_calls.append(order)
        return _result(order.order_id, message="poly")

    async def kalshi_submit(order: Order) -> OrderResult:
        kalshi_calls.append(order)
        return _result(order.order_id, message="kalshi")

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": poly_submit, "kalshi": kalshi_submit},
        sleep_fn=sleep,
    )

    poly_result = await executor.submit_order(_order(platform="polymarket"))
    kalshi_result = await executor.submit_order(_order(platform="kalshi"))

    assert poly_result.message == "poly"
    assert kalshi_result.message == "kalshi"
    assert len(poly_calls) == 1
    assert len(kalshi_calls) == 1


async def test_submit_order_unknown_platform_returns_error_result() -> None:
    """Submitting for an unregistered platform returns error without raising."""
    sleep = _NoopSleep()
    executor = OrderExecutor(submit_fns={}, sleep_fn=sleep)

    result = await executor.submit_order(_order(platform="nowhere"))

    assert result.status == "error"
    assert "nowhere" in result.message


# ---------------------------------------------------------------------------
# Client-side order id assignment + tracking
# ---------------------------------------------------------------------------


async def test_submit_order_assigns_client_side_id_when_missing() -> None:
    """An empty order_id is replaced by a generated id prefixed ``pms-``."""
    seen_ids: list[str] = []

    async def submit(order: Order) -> OrderResult:
        seen_ids.append(order.order_id)
        return _result(order.order_id)

    sleep = _NoopSleep()
    executor = OrderExecutor(submit_fns={"polymarket": submit}, sleep_fn=sleep)

    result = await executor.submit_order(_order(order_id=""))

    assert len(seen_ids) == 1
    assert seen_ids[0].startswith("pms-")
    assert result.order_id == seen_ids[0]
    # The generated id is surfaced via submitted_order_ids()
    assert result.order_id in executor.submitted_order_ids()


async def test_submit_order_preserves_existing_order_id() -> None:
    """A pre-populated order_id is respected and not overwritten."""
    seen_ids: list[str] = []

    async def submit(order: Order) -> OrderResult:
        seen_ids.append(order.order_id)
        return _result(order.order_id)

    sleep = _NoopSleep()
    executor = OrderExecutor(submit_fns={"polymarket": submit}, sleep_fn=sleep)

    result = await executor.submit_order(_order(order_id="caller-abc-123"))

    assert seen_ids == ["caller-abc-123"]
    assert result.order_id == "caller-abc-123"


async def test_successful_submission_tracked() -> None:
    """After a successful submit, the id is in submitted_order_ids()."""
    async def submit(order: Order) -> OrderResult:
        return _result(order.order_id)

    sleep = _NoopSleep()
    executor = OrderExecutor(submit_fns={"polymarket": submit}, sleep_fn=sleep)

    await executor.submit_order(_order(order_id="abc"))
    assert "abc" in executor.submitted_order_ids()


# ---------------------------------------------------------------------------
# Retry behaviour — transient failures
# ---------------------------------------------------------------------------


async def test_retry_on_connection_error_then_success() -> None:
    """Two ConnectionErrors → then success on the third attempt."""
    attempts = {"n": 0}

    async def flaky(order: Order) -> OrderResult:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("boom")
        return _result(order.order_id, status="filled")

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": flaky},
        max_retries=3,
        sleep_fn=sleep,
    )

    result = await executor.submit_order(_order(order_id="abc"))

    assert attempts["n"] == 3
    assert result.status == "filled"
    assert len(sleep.calls) == 2  # slept twice between the 3 attempts


async def test_retry_on_asyncio_timeout_error_then_success() -> None:
    """asyncio.TimeoutError is classified as transient and retried."""
    attempts = {"n": 0}

    async def flaky(order: Order) -> OrderResult:
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise asyncio.TimeoutError()
        return _result(order.order_id, status="filled")

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": flaky},
        max_retries=3,
        sleep_fn=sleep,
    )

    result = await executor.submit_order(_order(order_id="abc"))

    assert attempts["n"] == 2
    assert result.status == "filled"


async def test_exponential_backoff_delays() -> None:
    """Backoff grows multiplicatively between retries."""
    async def always_fail(order: Order) -> OrderResult:
        raise ConnectionError("nope")

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": always_fail},
        max_retries=4,
        initial_backoff=0.1,
        backoff_multiplier=2.0,
        sleep_fn=sleep,
    )

    result = await executor.submit_order(_order(order_id="abc"))

    assert result.status == "error"
    # 4 attempts → sleeps between them = 3 (no sleep after the last attempt).
    assert sleep.calls == pytest.approx([0.1, 0.2, 0.4])


async def test_max_retries_exhausted_returns_error_result() -> None:
    """All attempts fail transiently → final result is error, no raise."""
    call_count = {"n": 0}

    async def always_fail(order: Order) -> OrderResult:
        call_count["n"] += 1
        raise ConnectionError("always down")

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": always_fail},
        max_retries=3,
        sleep_fn=sleep,
    )

    result = await executor.submit_order(_order(order_id="abc"))

    assert result.status == "error"
    assert call_count["n"] == 3
    assert "abc" not in executor.submitted_order_ids()


async def test_unrecoverable_error_is_not_retried() -> None:
    """A non-transient exception breaks out of the retry loop immediately."""
    call_count = {"n": 0}

    async def explodes(order: Order) -> OrderResult:
        call_count["n"] += 1
        raise ValueError("programmer error")

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": explodes},
        max_retries=5,
        sleep_fn=sleep,
    )

    result = await executor.submit_order(_order(order_id="abc"))

    assert call_count["n"] == 1
    assert result.status == "error"
    assert sleep.calls == []  # never slept


# ---------------------------------------------------------------------------
# Idempotency — status check before retry
# ---------------------------------------------------------------------------


async def test_status_check_short_circuits_retry_when_order_already_filled() -> None:
    """First attempt times out; status_fn reports filled → no second submit."""
    submit_calls = {"n": 0}

    async def submit(order: Order) -> OrderResult:
        submit_calls["n"] += 1
        raise asyncio.TimeoutError()

    async def status(order_id: str) -> OrderResult | None:
        return _result(order_id, status="filled", message="already on exchange")

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": submit},
        status_fns={"polymarket": status},
        max_retries=3,
        sleep_fn=sleep,
    )

    result = await executor.submit_order(_order(order_id="abc"))

    # Only the first attempt ran — the status check intercepted the retry.
    assert submit_calls["n"] == 1
    assert result.status == "filled"
    assert result.message == "already on exchange"


async def test_status_check_returning_none_allows_retry() -> None:
    """status_fn returns None → retry proceeds as normal."""
    submit_calls = {"n": 0}

    async def submit(order: Order) -> OrderResult:
        submit_calls["n"] += 1
        if submit_calls["n"] == 1:
            raise asyncio.TimeoutError()
        return _result(order.order_id, status="filled")

    async def status(order_id: str) -> OrderResult | None:
        return None

    sleep = _NoopSleep()
    executor = OrderExecutor(
        submit_fns={"polymarket": submit},
        status_fns={"polymarket": status},
        max_retries=3,
        sleep_fn=sleep,
    )

    result = await executor.submit_order(_order(order_id="abc"))

    assert submit_calls["n"] == 2
    assert result.status == "filled"


# ---------------------------------------------------------------------------
# Positions aggregation + cancel placeholder
# ---------------------------------------------------------------------------


async def test_get_positions_aggregates_multiple_sources() -> None:
    """Positions from every registered source are concatenated."""
    async def poly_positions() -> list[Position]:
        return [_position("polymarket", "m-1"), _position("polymarket", "m-2")]

    async def kalshi_positions() -> list[Position]:
        return [_position("kalshi", "k-1")]

    executor = OrderExecutor(sleep_fn=_NoopSleep())
    executor.register_positions_source("polymarket", poly_positions)
    executor.register_positions_source("kalshi", kalshi_positions)

    positions = await executor.get_positions()

    assert len(positions) == 3
    platforms = {p.platform for p in positions}
    assert platforms == {"polymarket", "kalshi"}


async def test_get_positions_tolerates_source_failure() -> None:
    """A broken source is skipped; the others still contribute."""
    async def working() -> list[Position]:
        return [_position("polymarket", "m-1")]

    async def broken() -> list[Position]:
        raise ConnectionError("nope")

    executor = OrderExecutor(sleep_fn=_NoopSleep())
    executor.register_positions_source("polymarket", working)
    executor.register_positions_source("kalshi", broken)

    positions = await executor.get_positions()

    assert len(positions) == 1
    assert positions[0].platform == "polymarket"


async def test_cancel_order_returns_false_v1_placeholder() -> None:
    """v1 has no live cancel capability."""
    executor = OrderExecutor(sleep_fn=_NoopSleep())
    assert await executor.cancel_order("some-id") is False
