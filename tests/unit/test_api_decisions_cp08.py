from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import httpx
import pytest

from pms.actuator.risk import RiskDecision
from pms.api.app import create_app
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import RunMode, Side, TimeInForce
from pms.core.models import Opportunity, Portfolio, TradeDecision
from pms.runner import Runner


@dataclass(frozen=True)
class _StoredDecisionRow:
    decision: TradeDecision
    status: str
    factor_snapshot_hash: str | None
    created_at: datetime
    updated_at: datetime
    expires_at: datetime
    opportunity: Opportunity | None = None


class _DecisionStoreDouble:
    def __init__(self, row: _StoredDecisionRow | None) -> None:
        self.row = row
        self.read_calls: list[tuple[int, str | None, bool]] = []
        self.get_calls: list[tuple[str, bool]] = []
        self.update_calls: list[tuple[str, str, str]] = []

    async def read_decisions(
        self,
        *,
        limit: int,
        status: str | None = None,
        include_opportunity: bool = False,
    ) -> list[_StoredDecisionRow]:
        self.read_calls.append((limit, status, include_opportunity))
        if self.row is None:
            return []
        if status is not None and self.row.status != status:
            return []
        if include_opportunity:
            return [self.row]
        return [replace(self.row, opportunity=None)]

    async def get_decision(
        self,
        decision_id: str,
        *,
        include_opportunity: bool = False,
    ) -> _StoredDecisionRow | None:
        self.get_calls.append((decision_id, include_opportunity))
        if self.row is None or self.row.decision.decision_id != decision_id:
            return None
        if include_opportunity:
            return self.row
        return replace(self.row, opportunity=None)

    async def update_status(
        self,
        decision_id: str,
        *,
        current_status: str,
        next_status: str,
        updated_at: datetime,
    ) -> bool:
        del updated_at
        self.update_calls.append((decision_id, current_status, next_status))
        if self.row is None or self.row.decision.decision_id != decision_id:
            return False
        if self.row.status != current_status:
            return False
        self.row = replace(self.row, status=next_status)
        return True


class _DedupStoreDouble:
    def __init__(self, *, acquire_allowed: bool = True) -> None:
        self.acquire_allowed = acquire_allowed
        self.acquire_calls: list[str] = []
        self.release_calls: list[tuple[str, str]] = []

    async def acquire(self, decision: TradeDecision) -> bool:
        self.acquire_calls.append(decision.decision_id)
        return self.acquire_allowed

    async def release(self, decision_id: str, outcome: str) -> None:
        self.release_calls.append((decision_id, outcome))


class _RiskManagerDouble:
    def __init__(self, reason: str = "approved") -> None:
        self.reason = reason
        self.calls: list[str] = []

    def check(self, decision: TradeDecision, portfolio: Portfolio) -> RiskDecision:
        del portfolio
        self.calls.append(decision.decision_id)
        return RiskDecision(self.reason == "approved", self.reason)


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cp08",
        market_id="market-cp08",
        token_id="token-cp08-yes",
        venue="polymarket",
        side=Side.BUY.value,
        notional_usdc=25.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["cp08"],
        prob_estimate=0.67,
        expected_edge=0.18,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-cp08",
        strategy_id="default",
        strategy_version_id="default-v1",
        limit_price=0.41,
        action=Side.BUY.value,
        model_id="model-cp08",
    )


def _opportunity() -> Opportunity:
    created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    return Opportunity(
        opportunity_id="opportunity-cp08",
        market_id="market-cp08",
        token_id="token-cp08-yes",
        side="yes",
        selected_factor_values={"edge": 0.18, "liquidity": 0.04},
        expected_edge=0.18,
        rationale="cp08 rationale",
        target_size_usdc=25.0,
        expiry=created_at + timedelta(minutes=15),
        staleness_policy="cp08",
        strategy_id="default",
        strategy_version_id="default-v1",
        created_at=created_at,
        factor_snapshot_hash="snapshot-cp08",
        composition_trace={"kind": "unit"},
    )


def _stored_decision_row(
    *,
    status: str = "pending",
    factor_snapshot_hash: str | None = "snapshot-cp08",
) -> _StoredDecisionRow:
    created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    return _StoredDecisionRow(
        decision=_decision(),
        status=status,
        factor_snapshot_hash=factor_snapshot_hash,
        created_at=created_at,
        updated_at=created_at,
        expires_at=created_at + timedelta(minutes=15),
        opportunity=_opportunity(),
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _runner(
    store: _DecisionStoreDouble,
    *,
    dedup_allowed: bool = True,
    risk_reason: str = "approved",
) -> tuple[Runner, _DedupStoreDouble, AsyncMock]:
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.PAPER,
            auto_migrate_default_v2=False,
            risk=RiskSettings(
                max_position_per_market=1_000.0,
                max_total_exposure=10_000.0,
            ),
        )
    )
    dedup_store = _DedupStoreDouble(acquire_allowed=dedup_allowed)
    enqueue = AsyncMock()
    runner.decision_store = cast(Any, store)
    runner.portfolio = _portfolio()
    runner.enqueue_accepted_decision = enqueue  # type: ignore[attr-defined]
    runner.actuator_executor = cast(
        Any,
        SimpleNamespace(
            risk=_RiskManagerDouble(reason=risk_reason),
            dedup_store=dedup_store,
        ),
    )
    return runner, dedup_store, enqueue


@pytest.mark.asyncio
async def test_accept_decision_returns_404_when_unknown_id() -> None:
    store = _DecisionStoreDouble(row=None)
    runner, dedup_store, enqueue = _runner(store)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/decisions/missing/accept",
            json={"factor_snapshot_hash": "snapshot-cp08"},
        )

    assert response.status_code == 404
    assert response.json() == {"detail": "Decision not found"}
    assert dedup_store.acquire_calls == []
    assert store.update_calls == []
    enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_accept_decision_returns_404_when_decision_is_not_pending() -> None:
    store = _DecisionStoreDouble(_stored_decision_row(status="expired"))
    runner, dedup_store, enqueue = _runner(store)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/decisions/decision-cp08/accept",
            json={"factor_snapshot_hash": "snapshot-cp08"},
        )

    assert response.status_code == 404
    assert response.json() == {"detail": "Decision not found"}
    assert dedup_store.acquire_calls == []
    assert store.update_calls == []
    enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_accept_decision_returns_409_with_current_hash_on_snapshot_mismatch() -> None:
    store = _DecisionStoreDouble(
        _stored_decision_row(factor_snapshot_hash="snapshot-current"),
    )
    runner, dedup_store, enqueue = _runner(store)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/decisions/decision-cp08/accept",
            json={"factor_snapshot_hash": "snapshot-stale"},
        )

    assert response.status_code == 409
    assert response.json() == {
        "detail": "market_changed",
        "current_factor_snapshot_hash": "snapshot-current",
    }
    assert dedup_store.acquire_calls == []
    assert store.update_calls == []
    enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_accept_decision_returns_422_with_risk_rule_name() -> None:
    store = _DecisionStoreDouble(_stored_decision_row())
    runner, dedup_store, enqueue = _runner(
        store,
        dedup_allowed=True,
        risk_reason="max_position_per_market",
    )
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/decisions/decision-cp08/accept",
            json={"factor_snapshot_hash": "snapshot-cp08"},
        )

    assert response.status_code == 422
    assert response.json() == {"detail": "max_position_per_market"}
    assert dedup_store.acquire_calls == ["decision-cp08"]
    assert dedup_store.release_calls == [("decision-cp08", "invalid")]
    assert store.update_calls == [("decision-cp08", "pending", "rejected")]
    enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_get_decisions_include_opportunity_embeds_factor_payload() -> None:
    store = _DecisionStoreDouble(_stored_decision_row())
    runner, _, _ = _runner(store)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/decisions?include=opportunity&limit=1")

    assert response.status_code == 200
    assert response.json() == [
        {
            "decision_id": "decision-cp08",
            "market_id": "market-cp08",
            "token_id": "token-cp08-yes",
            "venue": "polymarket",
            "side": "BUY",
            "notional_usdc": 25.0,
            "order_type": "limit",
            "max_slippage_bps": 50,
            "stop_conditions": ["cp08"],
            "prob_estimate": 0.67,
            "expected_edge": 0.18,
            "time_in_force": "GTC",
            "opportunity_id": "opportunity-cp08",
            "strategy_id": "default",
            "strategy_version_id": "default-v1",
            "limit_price": 0.41,
            "action": "BUY",
            "outcome": "YES",
            "model_id": "model-cp08",
            "status": "pending",
            "factor_snapshot_hash": "snapshot-cp08",
            "created_at": "2026-04-23T10:00:00+00:00",
            "updated_at": "2026-04-23T10:00:00+00:00",
            "expires_at": "2026-04-23T10:15:00+00:00",
            "forecaster": "model-cp08",
            "kelly_size": 25.0,
            "opportunity": {
                "opportunity_id": "opportunity-cp08",
                "market_id": "market-cp08",
                "token_id": "token-cp08-yes",
                "side": "yes",
                "selected_factor_values": {"edge": 0.18, "liquidity": 0.04},
                "expected_edge": 0.18,
                "rationale": "cp08 rationale",
                "target_size_usdc": 25.0,
                "expiry": "2026-04-23T10:15:00+00:00",
                "staleness_policy": "cp08",
                "strategy_id": "default",
                "strategy_version_id": "default-v1",
                "created_at": "2026-04-23T10:00:00+00:00",
                "factor_snapshot_hash": "snapshot-cp08",
                "composition_trace": {"kind": "unit"},
            },
        }
    ]
    assert store.read_calls == [(1, None, True)]
