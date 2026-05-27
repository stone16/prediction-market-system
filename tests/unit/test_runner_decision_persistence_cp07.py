from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from math import inf, nan
from pathlib import Path
from typing import Any, cast

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode, Side, TimeInForce
from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision
from pms.runner import (
    Runner,
    StrategyControllerRuntime,
    _decision_evidence_from_signal,
    _decision_expires_at,
)


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.BACKTEST,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _runner() -> Runner:
    return Runner(
        config=_settings(),
        historical_data_path=FIXTURE_PATH,
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="market-cp07",
        token_id="token-cp07-yes",
        venue="polymarket",
        title="Will CP07 persist decisions?",
        yes_price=0.41,
        volume_24h=1200.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.40, "size": 20.0}],
            "asks": [{"price": 0.42, "size": 25.0}],
        },
        external_signal={"resolved_outcome": 1.0, "book_age_ms": 75.0},
        fetched_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _opportunity() -> Opportunity:
    return Opportunity(
        opportunity_id="opportunity-cp07",
        market_id="market-cp07",
        token_id="token-cp07-yes",
        side="yes",
        selected_factor_values={"edge": 0.21},
        expected_edge=0.21,
        rationale="persist the decision",
        target_size_usdc=25.0,
        expiry=datetime(2026, 4, 23, 10, 15, tzinfo=UTC),
        staleness_policy="cp07",
        strategy_id="default",
        strategy_version_id="default-v1",
        created_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        factor_snapshot_hash="snapshot-cp07",
    )


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cp07",
        market_id="market-cp07",
        token_id="token-cp07-yes",
        venue="polymarket",
        side=Side.BUY.value,
        notional_usdc=25.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["cp07"],
        prob_estimate=0.68,
        expected_edge=0.21,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-cp07",
        strategy_id="default",
        strategy_version_id="default-v1",
        limit_price=0.41,
        action=Side.BUY.value,
        model_id="model-cp07",
    )


class _OpportunityAwareControllerDouble:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        del portfolio
        self.calls.append(signal.market_id)
        return (
            replace(
                _opportunity(),
                created_at=datetime(2026, 4, 23, 10, 0, 1, 250000, tzinfo=UTC),
            ),
            _decision(),
        )


class _RecordingOpportunityStore:
    def __init__(self, runner: Runner) -> None:
        self.runner = runner
        self.calls: list[str] = []

    async def insert(self, opportunity: Opportunity) -> None:
        assert self.runner.state.opportunities == []
        self.calls.append(opportunity.opportunity_id)


class _RecordingDecisionStore:
    def __init__(self, runner: Runner) -> None:
        self.runner = runner
        self.calls: list[tuple[str, str | None, str]] = []
        self.decision_evidence: dict[str, object] | None = None
        self.transitions: list[tuple[str, str, str, datetime]] = []
        self.created_at: datetime | None = None
        self.expires_at: datetime | None = None

    async def insert(
        self,
        decision: TradeDecision,
        *,
        factor_snapshot_hash: str | None,
        created_at: datetime,
        expires_at: datetime,
        status: str = "pending",
        decision_evidence: dict[str, object] | None = None,
    ) -> None:
        assert self.runner.state.decisions == []
        assert self.runner._decision_queue.empty()  # noqa: SLF001
        self.calls.append((decision.decision_id, factor_snapshot_hash, status))
        self.decision_evidence = decision_evidence
        self.created_at = created_at
        self.expires_at = expires_at

    async def update_status(
        self,
        decision_id: str,
        *,
        current_status: str,
        next_status: str,
        updated_at: datetime,
    ) -> bool:
        if current_status == "accepted" and next_status == "queued":
            assert self.runner._decision_queue.empty()  # noqa: SLF001
        self.transitions.append(
            (decision_id, current_status, next_status, updated_at)
        )
        return True


class _RecordingSweepStore:
    def __init__(self) -> None:
        self.calls: list[datetime] = []
        self.prune_calls: list[datetime] = []

    async def expire_pending(self, *, before: datetime) -> int:
        self.calls.append(before)
        return 2

    async def prune_expired(self, *, before: datetime) -> object:
        self.prune_calls.append(before)
        return object()


@pytest.mark.asyncio
async def test_controller_pipeline_persists_decision_before_enqueuing_it() -> None:
    runner = _runner()
    controller = _OpportunityAwareControllerDouble()
    queue: asyncio.Queue[MarketSignal] = asyncio.Queue()
    runner._controller_runtimes["default"] = StrategyControllerRuntime(
        strategy_id="default",
        strategy_version_id="default-v1",
        controller=cast(Any, controller),
        asset_ids=None,
    )
    runner._controller_signal_queues["default"] = queue
    runner.opportunity_store = cast(Any, _RecordingOpportunityStore(runner))
    runner.decision_store = cast(Any, _RecordingDecisionStore(runner))
    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001
    await runner._controller_task
    await queue.put(_signal())

    await asyncio.wait_for(runner._controller_pipeline_loop("default"), timeout=1.0)  # noqa: SLF001

    assert controller.calls == ["market-cp07"]
    assert [item.opportunity_id for item in runner.state.opportunities] == [
        "opportunity-cp07"
    ]
    assert [item.decision_id for item in runner.state.decisions] == ["decision-cp07"]
    work_item = runner._decision_queue.get_nowait()  # noqa: SLF001
    assert work_item.decision.decision_id == "decision-cp07"
    assert work_item.signal is not None
    assert work_item.signal.market_id == "market-cp07"

    decision_store = cast(_RecordingDecisionStore, runner.decision_store)
    assert decision_store.calls == [("decision-cp07", "snapshot-cp07", "pending")]
    assert decision_store.decision_evidence is not None
    assert decision_store.decision_evidence["factor_snapshot_hash"] == "snapshot-cp07"
    assert decision_store.decision_evidence["book_age_ms"] == 75.0
    assert decision_store.decision_evidence["decision_latency_ms"] == 1250.0
    assert decision_store.decision_evidence["quote_source"] == "postgres_snapshot"
    assert decision_store.decision_evidence["book_hash"]
    assert decision_store.decision_evidence["book_top_levels"] == {
        "bids": [{"price": 0.4, "size": 20.0}],
        "asks": [{"price": 0.42, "size": 25.0}],
    }
    assert decision_store.transitions == [
        (
            "decision-cp07",
            "pending",
            "accepted",
            datetime(2026, 4, 23, 10, 0, 1, 250000, tzinfo=UTC),
        ),
        (
            "decision-cp07",
            "accepted",
            "queued",
            datetime(2026, 4, 23, 10, 0, 1, 250000, tzinfo=UTC),
        ),
    ]
    assert decision_store.created_at == datetime(
        2026, 4, 23, 10, 0, 1, 250000, tzinfo=UTC
    )
    assert decision_store.expires_at == datetime(2026, 4, 23, 10, 15, tzinfo=UTC)


@pytest.mark.asyncio
async def test_runner_sweep_expired_decisions_once_delegates_to_store() -> None:
    runner = _runner()
    runner.decision_store = cast(Any, _RecordingSweepStore())
    now = datetime(2026, 4, 23, 11, 0, tzinfo=UTC)

    expired = await runner._sweep_expired_decisions_once(now=now)  # noqa: SLF001

    assert expired == 2
    store = cast(_RecordingSweepStore, runner.decision_store)
    assert store.calls == [now]
    assert store.prune_calls == [now - timedelta(hours=24)]


def test_decision_expiry_uses_signal_resolution_when_opportunity_has_no_expiry() -> None:
    created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    opportunity = _opportunity()
    signal = _signal()
    object.__setattr__(opportunity, "expiry", None)
    object.__setattr__(signal, "resolves_at", datetime(2026, 4, 23, 10, 5, tzinfo=UTC))

    expires_at = _decision_expires_at(signal, opportunity, created_at=created_at)

    assert expires_at == datetime(2026, 4, 23, 10, 5, tzinfo=UTC)


def test_decision_evidence_drops_non_finite_orderbook_values() -> None:
    signal = replace(
        _signal(),
        orderbook={
            "bids": [
                {"price": nan, "size": 10.0},
                {"price": 0.39, "size": inf},
                {"price": 0.38, "size": 7.0},
            ],
            "asks": [
                {"price": "Inf", "size": 8.0},
                {"price": 0.42, "size": "-Infinity"},
                {"price": 0.43, "size": 9.0},
            ],
        },
        external_signal={"book_age_ms": "NaN"},
    )

    evidence = _decision_evidence_from_signal(
        signal,
        decision=_decision(),
        factor_snapshot_hash="snapshot-cp07",
        quote_source="postgres_snapshot",
        decision_created_at=signal.fetched_at,
    )

    assert evidence["book_age_ms"] is None
    assert evidence["book_top_levels"] == {
        "bids": [{"price": 0.38, "size": 7.0}],
        "asks": [{"price": 0.43, "size": 9.0}],
    }
    json.dumps(evidence, allow_nan=False)


def test_decision_evidence_records_submitted_token_when_buy_no_uses_yes_book() -> None:
    signal = replace(_signal(), token_id="token-cp07-yes")
    decision = replace(
        _decision(),
        token_id="token-cp07-no",
        outcome="NO",
        spread_bps_at_decision=500,
    )

    evidence = _decision_evidence_from_signal(
        signal,
        decision=decision,
        factor_snapshot_hash="snapshot-cp07",
        quote_source="postgres_snapshot",
        decision_created_at=signal.fetched_at,
    )

    assert evidence["book_token_id"] == "token-cp07-yes"
    assert evidence["decision_token_id"] == "token-cp07-no"
    assert evidence["decision_outcome"] == "NO"
    assert evidence["decision_side"] == "BUY"
    assert evidence["spread_bps_at_decision"] == 500
    assert evidence["book_hash"]


def test_decision_evidence_records_secondary_baseline_probabilities() -> None:
    signal = replace(
        _signal(),
        external_signal={
            "resolved_outcome": 1.0,
            "book_age_ms": 75.0,
            "last_trade_price": 0.43,
            "category_prior_baseline_prob_estimate": 0.52,
        },
    )
    decision = replace(
        _decision(),
        token_id="token-cp07-no",
        outcome="NO",
        limit_price=0.39,
    )

    evidence = _decision_evidence_from_signal(
        signal,
        decision=decision,
        factor_snapshot_hash="snapshot-cp07",
        quote_source="postgres_snapshot",
        decision_created_at=signal.fetched_at,
    )

    assert evidence["market_implied_baseline_prob_estimate"] == pytest.approx(0.61)
    assert evidence["mid_quote_baseline_prob_estimate"] == pytest.approx(0.41)
    assert evidence["last_trade_baseline_prob_estimate"] == pytest.approx(0.43)
    assert evidence["category_prior_baseline_prob_estimate"] == pytest.approx(0.52)
