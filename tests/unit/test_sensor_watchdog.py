from __future__ import annotations

import asyncio

import pytest

from pms.config import PMSSettings, SensorSettings
from pms.sensor.watchdog import SensorWatchdog


@pytest.mark.asyncio
async def test_watchdog_rearms_after_activity() -> None:
    fallback_calls = 0
    first_timeout = asyncio.Event()
    second_timeout = asyncio.Event()

    async def fallback() -> None:
        nonlocal fallback_calls
        fallback_calls += 1
        if fallback_calls == 1:
            first_timeout.set()
        if fallback_calls == 2:
            second_timeout.set()

    watchdog = SensorWatchdog(timeout_s=0.01, fallback=fallback)
    await watchdog.start()

    await asyncio.wait_for(first_timeout.wait(), timeout=0.5)
    watchdog.notify_message()
    await asyncio.wait_for(second_timeout.wait(), timeout=0.5)
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
