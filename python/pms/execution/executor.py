"""OrderExecutor — routes orders to platform submit callables (CP08).

This module implements the concrete
:class:`~pms.protocols.execution.ExecutorProtocol`. Because the CP04/CP05
connectors deliberately do not expose ``submit_order`` (live trading is
deferred), the executor accepts **injected submit/status callables** keyed
on platform. This keeps the routing logic unit-testable and lets integration
wiring be deferred to a later checkpoint.

Features implemented here per the CP08 acceptance criteria:

* platform-based routing via ``submit_fns``
* client-side order id assignment (``pms-<uuid>``) when the caller leaves
  ``order.order_id`` empty
* idempotent retry loop — before retrying the executor consults
  ``status_fns[platform]``; if the order is already filled / partial /
  rejected on the exchange the existing result is returned and no second
  submit is issued
* transient-failure retries (``asyncio.TimeoutError``, ``ConnectionError``)
  with exponential backoff bounded by ``max_retries``
* a pluggable ``sleep_fn`` so tests can skip real wall-clock delays
* positions aggregation across registered sources (with per-source failure
  tolerance)
* ``cancel_order`` placeholder that returns ``False`` in v1 — real cancel
  semantics are out of scope for this checkpoint

Phase 3B (positions ledger)
---------------------------

The executor also maintains an **in-memory positions ledger** derived
from every successful ``OrderResult`` it produces. This closes the
pms-v1 E2E gap where ``RiskManager`` could only see positions reported
by external connectors — long-only tracking, keyed by
``(platform, market_id, outcome_id)``:

* ``buy`` fills add to the position and weight-average the entry price
* ``sell`` fills reduce the position; selling more than is held clamps
  at zero (no short tracking — buying the opposite outcome is the
  intended way to express a short in this model)
* ``rejected`` / ``error`` results never touch the ledger
* ``get_positions`` returns the merged view (external sources are
  authoritative when they cover the same key; the ledger fills the
  gap for platforms with no registered source)
* ``internal_positions`` exposes just the ledger view for tests and
  for callers that need to distinguish synthetic from live positions

The ledger lives in process memory only — reload the executor and
you start with an empty book. Durable persistence is reserved for the
``StorageProtocol`` future work documented in the spec.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from decimal import Decimal
from typing import Final

from pms.models import Order, OrderResult, Position

logger = logging.getLogger(__name__)


# Type aliases for injected callables. Keeping them as module-level names
# makes the ``OrderExecutor`` signatures much more readable under mypy strict.
SubmitFn = Callable[[Order], Awaitable[OrderResult]]
StatusFn = Callable[[str], Awaitable[OrderResult | None]]
PositionsSourceFn = Callable[[], Awaitable[list[Position]]]
SleepFn = Callable[[float], Awaitable[None]]


# Statuses that mean "the exchange already has this order" and should
# short-circuit a retry.
_TERMINAL_STATUSES: Final[frozenset[str]] = frozenset(
    {"filled", "partial", "rejected"}
)


class OrderExecutor:
    """Routes orders, tracks client-side ids, and retries transient failures.

    All I/O is performed via injected callables so the executor remains
    deterministic under test. ``sleep_fn`` defaults to :func:`asyncio.sleep`
    in production but is replaced with a no-op in the CP08 test suite.
    """

    def __init__(
        self,
        submit_fns: dict[str, SubmitFn] | None = None,
        status_fns: dict[str, StatusFn] | None = None,
        max_retries: int = 3,
        initial_backoff: float = 0.1,
        backoff_multiplier: float = 2.0,
        sleep_fn: SleepFn = asyncio.sleep,
    ) -> None:
        self._submit_fns: dict[str, SubmitFn] = dict(submit_fns or {})
        self._status_fns: dict[str, StatusFn] = dict(status_fns or {})
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff
        self._backoff_multiplier = backoff_multiplier
        self._sleep: SleepFn = sleep_fn
        self._positions_sources: dict[str, PositionsSourceFn] = {}
        self._submitted_ids: set[str] = set()
        # Phase 3B: in-memory positions ledger keyed by
        # (platform, market_id, outcome_id). Mutated only by
        # ``_update_ledger`` after a successful ``submit_fn`` call.
        self._ledger: dict[tuple[str, str, str], Position] = {}

    # ------------------------------------------------------------------
    # Registration helpers
    # ------------------------------------------------------------------
    def register_positions_source(
        self, platform: str, fn: PositionsSourceFn
    ) -> None:
        """Register a callable that returns the positions for ``platform``."""
        self._positions_sources[platform] = fn

    # ------------------------------------------------------------------
    # Protocol surface
    # ------------------------------------------------------------------
    async def submit_order(self, order: Order) -> OrderResult:
        """Submit ``order`` with retries and idempotency checks.

        * Assigns a client-side id prefixed with ``"pms-"`` when none is set.
        * Dispatches to ``submit_fns[order.platform]``; unknown platforms
          return an error result (not an exception).
        * Retries on ``asyncio.TimeoutError`` / ``ConnectionError`` up to
          ``max_retries`` using exponential backoff.
        * Before every retry (``attempt > 0``) consults ``status_fns`` — if
          the exchange already has the order in a terminal state the
          existing fill is returned and no duplicate submit is issued.
        """
        # Ensure the order carries a client-side id for idempotency tracking.
        if not order.order_id:
            order = replace(order, order_id=f"pms-{uuid.uuid4()}")

        submit_fn = self._submit_fns.get(order.platform)
        if submit_fn is None:
            return self._error_result(
                order.order_id,
                f"No submit handler registered for platform {order.platform}",
            )

        status_fn = self._status_fns.get(order.platform)

        backoff = self._initial_backoff
        last_error = ""
        for attempt in range(self._max_retries):
            # Idempotency gate: before a retry (not the first attempt), ask
            # the exchange whether the order already exists. If it does, the
            # previous error was a "false negative" and we must not submit
            # again.
            if attempt > 0 and status_fn is not None:
                try:
                    status = await status_fn(order.order_id)
                except Exception:  # pragma: no cover - defensive
                    logger.exception(
                        "status_fn raised for order %s; proceeding with retry",
                        order.order_id,
                    )
                    status = None
                if status is not None and status.status in _TERMINAL_STATUSES:
                    logger.info(
                        "Order %s resolved via status check after retry",
                        order.order_id,
                    )
                    self._submitted_ids.add(order.order_id)
                    self._update_ledger(order, status)
                    return status

            try:
                result = await submit_fn(order)
            except (asyncio.TimeoutError, ConnectionError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "Transient error on attempt %d for order %s: %s",
                    attempt + 1,
                    order.order_id,
                    last_error,
                )
                if attempt < self._max_retries - 1:
                    await self._sleep(backoff)
                    backoff *= self._backoff_multiplier
                continue
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.exception(
                    "Unrecoverable error submitting order %s", order.order_id
                )
                break

            self._submitted_ids.add(order.order_id)
            self._update_ledger(order, result)
            return result

        return self._error_result(
            order.order_id,
            f"Failed after {self._max_retries} attempts: {last_error}",
        )

    async def cancel_order(self, order_id: str) -> bool:
        """v1 placeholder — real cancellation is deferred."""
        return False

    async def get_positions(self) -> list[Position]:
        """Aggregate positions across every registered source + the ledger.

        External (live API) sources are authoritative when they cover the
        same ``(platform, market_id, outcome_id)`` key — they reflect the
        venue's ground truth, while the ledger is a synthetic
        reconstruction from local fill history. Internal ledger entries
        only fill the gap for keys the external sources do not report.

        Individual source failures are logged and skipped so a single
        broken connector never blanks out the whole portfolio view.
        """
        merged: dict[tuple[str, str, str], Position] = {}
        for platform, fn in self._positions_sources.items():
            try:
                positions = await fn()
            except Exception as exc:
                logger.warning(
                    "Failed to fetch positions from %s: %s", platform, exc
                )
                continue
            for pos in positions:
                merged[(pos.platform, pos.market_id, pos.outcome_id)] = pos

        # Internal ledger fills any gap not covered by an external source.
        for key, pos in self._ledger.items():
            merged.setdefault(key, pos)

        return list(merged.values())

    # ------------------------------------------------------------------
    # Phase 3B: in-memory positions ledger
    # ------------------------------------------------------------------
    def internal_positions(self) -> list[Position]:
        """Return the executor's synthetic positions ledger as a list.

        Snapshot only — mutating the returned list does **not** alter
        the executor's internal state. Use this when callers need to
        distinguish positions the executor inferred from its own fill
        stream from positions reported by an external source.
        """
        return list(self._ledger.values())

    def clear_internal_positions(self) -> None:
        """Reset the in-memory ledger.

        Test helper / operational reset. Real production callers should
        rarely need this; it's primarily here so a mid-session reset is
        possible without re-instantiating the executor (which would
        also drop ``_submitted_ids`` and the registered submit handlers).
        """
        self._ledger.clear()

    def _update_ledger(self, order: Order, result: OrderResult) -> None:
        """Apply ``result`` to the in-memory positions ledger.

        Skips if the result is not a fill (``filled`` / ``partial``) or
        if ``filled_size`` is zero. ``buy`` adds and re-averages;
        ``sell`` reduces and clamps at zero (long-only — see module
        docstring for the rationale).
        """
        if result.status not in ("filled", "partial"):
            return
        if result.filled_size <= 0:
            return

        key = (order.platform, order.market_id, order.outcome_id)
        existing = self._ledger.get(key)

        if order.side == "buy":
            if existing is None:
                self._ledger[key] = Position(
                    platform=order.platform,
                    market_id=order.market_id,
                    outcome_id=order.outcome_id,
                    size=result.filled_size,
                    avg_entry_price=result.filled_price,
                    unrealized_pnl=Decimal("0"),
                )
                return
            new_size = existing.size + result.filled_size
            # Weighted average — Decimal arithmetic, not float, so the
            # cost basis is exact across many partial fills.
            new_avg = (
                existing.size * existing.avg_entry_price
                + result.filled_size * result.filled_price
            ) / new_size
            self._ledger[key] = replace(
                existing, size=new_size, avg_entry_price=new_avg
            )
            return

        # order.side == "sell"
        if existing is None or existing.size <= 0:
            # Long-only: nothing to sell. Silently no-op rather than
            # raise so a stray sell from a strategy never crashes the
            # executor — the risk manager is the layer that should
            # reject naked shorts.
            return
        new_size = existing.size - result.filled_size
        if new_size <= 0:
            # Position fully closed — drop it from the ledger entirely
            # so callers don't see a zero-size ghost row.
            del self._ledger[key]
            return
        # Sells do not change cost basis (avg_entry_price stays put);
        # only realised P&L would, and the ledger does not track that.
        self._ledger[key] = replace(existing, size=new_size)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    def submitted_order_ids(self) -> set[str]:
        """Return a copy of the set of successfully submitted order ids."""
        return set(self._submitted_ids)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _error_result(order_id: str, message: str) -> OrderResult:
        return OrderResult(
            order_id=order_id,
            status="error",
            filled_size=Decimal("0"),
            filled_price=Decimal("0"),
            message=message,
            raw={},
        )
