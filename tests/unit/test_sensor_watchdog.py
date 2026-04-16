from __future__ import annotations

import asyncio

import pytest

from pms.config import PMSSettings, SensorSettings
from pms.sensor.watchdog import SensorWatchdog


@pytest.mark.asyncio
async def test_watchdog_rearms_after_activity() -> None:
    fallback_calls = 0

    async def fallback() -> None:
        nonlocal fallback_calls
        fallback_calls += 1

    watchdog = SensorWatchdog(timeout_s=0.01, fallback=fallback)
    await watchdog.start()

    await asyncio.sleep(0.03)
    watchdog.notify_message()
    await asyncio.sleep(0.03)
    await watchdog.stop()

    assert fallback_calls == 2


def test_config_exposes_sensor_poll_interval() -> None:
    settings = PMSSettings(
        sensor=SensorSettings(
            poll_interval_s=7.5,
            max_reconnect_interval_s=42.0,
        )
    )

    assert settings.sensor.poll_interval_s == 7.5
    assert settings.sensor.max_reconnect_interval_s == 42.0
