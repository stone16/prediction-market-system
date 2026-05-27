from __future__ import annotations

import os
from pathlib import Path

import pytest

from pms.config import PMSSettings
from pms.live_preflight import run_live_preflight


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
    ),
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_LIVE_PREFLIGHT") != "1",
        reason=(
            "set PMS_RUN_LIVE_PREFLIGHT=1 to run credentialed read-only "
            "LIVE preflight"
        ),
    ),
]


@pytest.mark.asyncio
async def test_credentialed_live_preflight_passes_without_skipping_venue() -> None:
    config_path = Path(os.environ.get("PMS_LIVE_PREFLIGHT_CONFIG", "config.live.yaml"))
    settings = PMSSettings.load(config_path)

    result = await run_live_preflight(settings, skip_venue=False)

    assert result.ok, result.as_dict()
    assert result.require_check("venue_reconciliation").ok is True
