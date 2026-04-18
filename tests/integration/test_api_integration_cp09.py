from __future__ import annotations

from pathlib import Path
from typing import cast

import httpx
import pytest

from pms.api.app import create_app
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import RunMode
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


@pytest.mark.asyncio
async def test_api_backtest_runner_get_routes(tmp_path: Path) -> None:
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            risk=RiskSettings(
                max_position_per_market=1000.0,
                max_total_exposure=10_000.0,
            ),
        ),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )

    await runner.start()
    await runner.wait_until_idle()
    app = create_app(runner)
    transport = httpx.ASGITransport(app=app)

    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            status = await client.get("/status")
            signals = await client.get("/signals?limit=50")
            decisions = await client.get("/decisions?limit=50")
            metrics = await client.get("/metrics")
            feedback = await client.get("/feedback")
    finally:
        await runner.stop()

    assert status.status_code == 200
    assert signals.status_code == 200
    assert decisions.status_code == 200
    assert metrics.status_code == 200
    assert feedback.status_code == 200
    assert status.json()["evaluator"]["eval_records_total"] >= 5
    assert len(signals.json()) == 50
    assert len(decisions.json()) >= 10
    assert metrics.json()["brier_overall"] is not None
    assert isinstance(feedback.json(), list)
