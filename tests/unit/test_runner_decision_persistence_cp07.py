from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode, Side, TimeInForce
from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision
from pms.runner import Runner, StrategyControllerRuntime


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
        orderbook={"bids": [], "asks": []},
        external_signal={"resolved_outcome": 1.0},
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
        return _opportunity(), _decision()


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
    ) -> None:
        assert self.runner.state.decisions == []
        assert self.runner._decision_queue.empty()  # noqa: SLF001
        self.calls.append((decision.decision_id, factor_snapshot_hash, status))
        self.created_at = created_at
        self.expires_at = expires_at


class _RecordingSweepStore:
    def __init__(self) -> None:
        self.calls: list[datetime] = []

    async def expire_pending(self, *, before: datetime) -> int:
        self.calls.append(before)
        return 2


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
    decision, signal = runner._decision_queue.get_nowait()  # noqa: SLF001
    assert decision.decision_id == "decision-cp07"
    assert signal.market_id == "market-cp07"

    decision_store = cast(_RecordingDecisionStore, runner.decision_store)
    assert decision_store.calls == [("decision-cp07", "snapshot-cp07", "pending")]
    assert decision_store.created_at == datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    assert decision_store.expires_at == datetime(2026, 4, 23, 10, 15, tzinfo=UTC)


@pytest.mark.asyncio
async def test_runner_sweep_expired_decisions_once_delegates_to_store() -> None:
    runner = _runner()
    runner.decision_store = cast(Any, _RecordingSweepStore())
    now = datetime(2026, 4, 23, 11, 0, tzinfo=UTC)

    expired = await runner._sweep_expired_decisions_once(now=now)  # noqa: SLF001

    assert expired == 2
    assert cast(_RecordingSweepStore, runner.decision_store).calls == [now]
