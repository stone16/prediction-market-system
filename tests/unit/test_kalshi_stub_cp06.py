from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock

import httpx
import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import RiskSettings
from pms.core.enums import Venue
from pms.core.models import MarketSignal, Portfolio, TradeDecision
from pms.research.execution import BacktestExecutionSimulator
from pms.research.specs import ExecutionModel
from pms.storage.feedback_store import FeedbackStore
from pms.strategies.projections import MarketSelectionSpec
from tests.support.fake_stores import InMemoryFeedbackStore


REPO_ROOT = Path(__file__).resolve().parents[2]
DISPATCHER_FILES = (
    REPO_ROOT / "src/pms/market_selection/selector.py",
    REPO_ROOT / "src/pms/sensor/adapters/historical.py",
    REPO_ROOT / "src/pms/sensor/adapters/market_discovery.py",
    REPO_ROOT / "src/pms/actuator/executor.py",
    REPO_ROOT / "src/pms/actuator/adapters/paper.py",
    REPO_ROOT / "src/pms/actuator/adapters/backtest.py",
    REPO_ROOT / "src/pms/research/execution.py",
)


def _decision(*, venue: str = "polymarket") -> TradeDecision:
    return TradeDecision(
        decision_id="decision-kalshi-cp06",
        market_id="market-kalshi-cp06",
        token_id="token-kalshi-cp06",
        venue=cast(Any, venue),
        side="BUY",
        price=0.4,
        size=10.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["unit-test"],
        prob_estimate=0.6,
        expected_edge=0.2,
        time_in_force="GTC",
        opportunity_id="opp-kalshi-cp06",
        strategy_id="default",
        strategy_version_id="default-v1",
    )


def _signal(*, venue: str = "polymarket") -> MarketSignal:
    return MarketSignal(
        market_id="market-kalshi-cp06",
        token_id="token-kalshi-cp06",
        venue=cast(Any, venue),
        title="Will CP06 fail fast on kalshi?",
        yes_price=0.4,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 5, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={},
        fetched_at=datetime(2026, 4, 21, 12, 0, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def test_venue_enum_retains_kalshi_member() -> None:
    assert Venue.KALSHI.value == "kalshi"


def test_dispatcher_files_define_explicit_kalshi_stub_branches() -> None:
    for path in DISPATCHER_FILES:
        source = path.read_text(encoding="utf-8")
        assert "Venue.KALSHI" in source, f"{path} is missing the explicit Kalshi branch"
        assert "KalshiStubError" in source, f"{path} is missing the Kalshi stub raise"


@pytest.mark.asyncio
async def test_market_selector_rejects_kalshi_before_hitting_store() -> None:
    from pms.core.exceptions import KalshiStubError
    from pms.market_selection.merge import UnionMergePolicy
    from pms.market_selection.selector import MarketSelector

    class FakeRegistry:
        async def list_market_selections(
            self,
        ) -> list[tuple[str, str, MarketSelectionSpec]]:
            return [
                (
                    "strategy-kalshi",
                    "strategy-kalshi-v1",
                    MarketSelectionSpec(
                        venue="kalshi",
                        resolution_time_max_horizon_days=7,
                        volume_min_usdc=100.0,
                    ),
                )
            ]

    class FakeStore:
        async def read_eligible_markets(
            self,
            venue: str,
            max_horizon_days: int | None,
            min_volume_usdc: float,
        ) -> list[object]:
            raise AssertionError(
                "selector should reject Kalshi before calling read_eligible_markets"
            )

    selector = MarketSelector(
        store=cast(Any, FakeStore()),
        registry=cast(Any, FakeRegistry()),
        merge_policy=UnionMergePolicy(),
    )

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await selector.select_per_strategy()


@pytest.mark.asyncio
async def test_historical_sensor_rejects_kalshi_rows(tmp_path: Path) -> None:
    from pms.core.exceptions import KalshiStubError
    from pms.sensor.adapters.historical import HistoricalSensor

    fixture = tmp_path / "kalshi-signals.jsonl"
    fixture.write_text(
        json.dumps(
            {
                "market_id": "kalshi-market",
                "token_id": "kalshi-token",
                "venue": "kalshi",
                "title": "Will CP06 catch Kalshi historical rows?",
                "yes_price": 0.5,
                "volume_24h": 1000.0,
                "resolves_at": "2026-05-01T00:00:00Z",
                "orderbook": {"bids": [], "asks": []},
                "external_signal": {},
                "fetched_at": "2026-04-21T00:00:00Z",
                "market_status": "open",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    sensor = HistoricalSensor(fixture)

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await anext(sensor.__aiter__())


@dataclass
class _StoreMock:
    write_market_mock: AsyncMock = field(default_factory=AsyncMock)
    write_token_mock: AsyncMock = field(default_factory=AsyncMock)

    async def write_market(self, market: Any) -> None:
        await self.write_market_mock(market)

    async def write_token(self, token: Any) -> None:
        await self.write_token_mock(token)


@pytest.mark.asyncio
async def test_market_discovery_sensor_rejects_kalshi_rows() -> None:
    from pms.core.exceptions import KalshiStubError
    from pms.sensor.adapters.market_discovery import MarketDiscoverySensor

    store = _StoreMock()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(
            200,
            json=[
                {
                    "id": "kalshi-market",
                    "conditionId": "kalshi-market",
                    "venue": "kalshi",
                    "slug": "kalshi-market",
                    "question": "Will CP06 catch Kalshi discovery rows?",
                    "endDateIso": "2026-07-31",
                    "createdAt": "2025-05-02T15:03:10.397014Z",
                    "clobTokenIds": json.dumps(["yes-token", "no-token"]),
                    "outcomes": json.dumps(["Yes", "No"]),
                }
            ],
        )

    sensor = MarketDiscoverySensor(
        store=cast(Any, store),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await sensor.poll_once()
    await sensor.aclose()

    assert store.write_market_mock.await_count == 0
    assert store.write_token_mock.await_count == 0


@pytest.mark.asyncio
async def test_paper_actuator_rejects_kalshi_decision() -> None:
    from pms.core.exceptions import KalshiStubError

    actuator = PaperActuator(
        orderbooks={
            "market-kalshi-cp06": {
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [{"price": 0.41, "size": 100.0}],
            }
        }
    )

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await actuator.execute(_decision(venue="kalshi"))


@pytest.mark.asyncio
async def test_backtest_actuator_rejects_kalshi_decision(tmp_path: Path) -> None:
    from pms.core.exceptions import KalshiStubError

    fixture = tmp_path / "orderbooks.jsonl"
    fixture.write_text(
        json.dumps(
            {
                "market_id": "market-kalshi-cp06",
                "orderbook": {
                    "bids": [{"price": 0.39, "size": 100.0}],
                    "asks": [{"price": 0.41, "size": 100.0}],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    actuator = BacktestActuator(fixture)

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await actuator.execute(_decision(venue="kalshi"))


@pytest.mark.asyncio
async def test_actuator_executor_rejects_kalshi_before_adapter_execution() -> None:
    from pms.core.exceptions import KalshiStubError

    @dataclass
    class RecordingAdapter:
        calls: int = 0

        async def execute(
            self,
            decision: TradeDecision,
            portfolio: Portfolio | None = None,
        ) -> object:
            del decision, portfolio
            self.calls += 1
            raise AssertionError("adapter should not run for Kalshi")

    adapter = RecordingAdapter()
    feedback_store = cast(FeedbackStore, InMemoryFeedbackStore())
    executor = ActuatorExecutor(
        adapter=cast(Any, adapter),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1_000.0)
        ),
        feedback=ActuatorFeedback(feedback_store),
    )

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await executor.execute(_decision(venue="kalshi"), _portfolio())

    assert adapter.calls == 0
    assert executor.dedup_tokens.contains("decision-kalshi-cp06") is False
    assert await cast(InMemoryFeedbackStore, feedback_store).all() == []


@pytest.mark.asyncio
async def test_backtest_execution_simulator_rejects_kalshi_runtime_use() -> None:
    from pms.core.exceptions import KalshiStubError

    simulator = BacktestExecutionSimulator()

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await simulator.execute(
            signal=_signal(venue="kalshi"),
            decision=_decision(venue="kalshi"),
            portfolio=_portfolio(),
            execution_model=ExecutionModel(
                fee_rate=0.0,
                slippage_bps=0.0,
                latency_ms=0.0,
                staleness_ms=1_000.0,
                fill_policy="immediate_or_cancel",
            ),
        )
