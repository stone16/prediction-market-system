from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import importlib
from typing import Any

import pytest


def _load_symbol(module_name: str, symbol_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in red phase
        pytest.fail(f"{module_name} is missing: {exc}")
    return getattr(module, symbol_name)


@pytest.mark.asyncio
async def test_subscription_controller_updates_sink_only_when_asset_ids_change() -> None:
    subscription_controller_cls = _load_symbol(
        "pms.market_selection.subscription_controller",
        "SensorSubscriptionController",
    )

    class RecordingSink:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        async def update_subscription(self, asset_ids: list[str]) -> None:
            self.calls.append(list(asset_ids))

    sink = RecordingSink()
    controller = subscription_controller_cls(sink)

    changed = await controller.update(["asset-a", "asset-b"])

    assert changed is True
    assert sink.calls == [["asset-a", "asset-b"]]
    assert controller.current_asset_ids == frozenset({"asset-a", "asset-b"})
    assert isinstance(controller.last_updated_at, datetime)
    assert controller.last_updated_at.tzinfo is UTC


@pytest.mark.asyncio
async def test_subscription_controller_is_noop_for_same_asset_set() -> None:
    subscription_controller_cls = _load_symbol(
        "pms.market_selection.subscription_controller",
        "SensorSubscriptionController",
    )

    class RecordingSink:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        async def update_subscription(self, asset_ids: list[str]) -> None:
            self.calls.append(list(asset_ids))

    sink = RecordingSink()
    controller = subscription_controller_cls(sink)

    first_changed = await controller.update(["asset-a", "asset-b"])
    first_updated_at = controller.last_updated_at
    second_changed = await controller.update(["asset-b", "asset-a"])

    assert first_changed is True
    assert second_changed is False
    assert sink.calls == [["asset-a", "asset-b"]]
    assert controller.current_asset_ids == frozenset({"asset-a", "asset-b"})
    assert controller.last_updated_at == first_updated_at


@pytest.mark.asyncio
async def test_subscription_controller_serializes_concurrent_updates() -> None:
    subscription_controller_cls = _load_symbol(
        "pms.market_selection.subscription_controller",
        "SensorSubscriptionController",
    )

    class BlockingSink:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.current_concurrency = 0
            self.max_concurrency = 0

        async def update_subscription(self, asset_ids: list[str]) -> None:
            self.current_concurrency += 1
            self.max_concurrency = max(self.max_concurrency, self.current_concurrency)
            self.calls.append(list(asset_ids))
            if len(self.calls) == 1:
                self.started.set()
                await self.release.wait()
            self.current_concurrency -= 1

    sink = BlockingSink()
    controller = subscription_controller_cls(sink)

    first_update = asyncio.create_task(controller.update(["asset-a"]))
    await sink.started.wait()
    second_update = asyncio.create_task(controller.update(["asset-b"]))
    await asyncio.sleep(0)
    sink.release.set()

    first_changed, second_changed = await asyncio.gather(first_update, second_update)

    assert first_changed is True
    assert second_changed is True
    assert sink.max_concurrency == 1
    assert sink.calls == [["asset-a"], ["asset-b"]]
    assert controller.current_asset_ids == frozenset({"asset-b"})
    assert controller.last_updated_at is not None
