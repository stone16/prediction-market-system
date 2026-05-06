from __future__ import annotations

import os

import pytest

from tests.unit.test_executor_publishes_halt import (
    test_executor_publishes_halt_event_from_real_auto_halt_path,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
    ),
]


async def test_executor_halt_publish_integration() -> None:
    await test_executor_publishes_halt_event_from_real_auto_halt_path()
