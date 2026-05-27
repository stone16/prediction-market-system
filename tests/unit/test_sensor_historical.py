from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
import json
import os
from pathlib import Path

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.sensor.adapters.historical import HistoricalSensor
from pms.sensor.stream import SensorStream


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


async def _collect(sensor: HistoricalSensor) -> list[MarketSignal]:
    return [signal async for signal in sensor]


def _write_jsonl_signal(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "external_signal": {},
                "fetched_at": "2026-04-13T00:00:01Z",
                "market_id": "m1",
                "market_status": "open",
                "orderbook": {"asks": [], "bids": []},
                "resolves_at": "2026-04-15T00:00:00Z",
                "title": "First",
                "token_id": "t1",
                "venue": "polymarket",
                "volume_24h": 1.0,
                "yes_price": 0.5,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_csv_signals(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "market_id,token_id,venue,title,yes_price,volume_24h,resolves_at,orderbook,external_signal,fetched_at,market_status",
                'm1,t1,polymarket,First,0.5,1.0,2026-04-15T00:00:00Z,"{""bids"":[],""asks"":[]}","{}",2026-04-13T00:00:01Z,open',
            ]
        ),
        encoding="utf-8",
    )


@pytest.mark.asyncio
async def test_historical_sensor_reads_jsonl_fixture_in_timestamp_order() -> None:
    assert FIXTURE_PATH.exists()

    signals = await _collect(HistoricalSensor(FIXTURE_PATH))

    assert len(signals) == 100
    assert [signal.fetched_at for signal in signals] == sorted(
        signal.fetched_at for signal in signals
    )
    assert signals[0].market_id == "pm-synthetic-000"
    assert signals[-1].market_id == "pm-synthetic-099"
    assert signals[0].market_status == MarketStatus.OPEN.value
    assert {
        signal.external_signal.get("resolved_outcome") for signal in signals
    } == {0.0, 1.0}


@pytest.mark.asyncio
async def test_historical_sensor_reads_csv_in_timestamp_order(tmp_path: Path) -> None:
    csv_path = tmp_path / "signals.csv"
    csv_path.write_text(
        "\n".join(
            [
                "market_id,token_id,venue,title,yes_price,volume_24h,resolves_at,orderbook,external_signal,fetched_at,market_status",
                'm2,t2,polymarket,Second,0.6,2.0,2026-04-15T00:00:00Z,"{""bids"":[],""asks"":[]}","{}",2026-04-13T00:00:02Z,open',
                'm1,t1,polymarket,First,0.5,1.0,2026-04-15T00:00:00Z,"{""bids"":[],""asks"":[]}","{}",2026-04-13T00:00:01Z,open',
            ]
        ),
        encoding="utf-8",
    )

    signals = await _collect(HistoricalSensor(csv_path))

    assert [signal.market_id for signal in signals] == ["m1", "m2"]


@pytest.mark.asyncio
async def test_historical_sensor_rejects_symlink_jsonl_fixture(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-signals.jsonl"
    _write_jsonl_signal(target_path)
    path = tmp_path / "signals.jsonl"
    path.symlink_to(target_path)

    with pytest.raises(ValueError, match="historical data cannot be read safely"):
        await _collect(HistoricalSensor(path))


@pytest.mark.asyncio
async def test_historical_sensor_opens_csv_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    path = tmp_path / "signals.csv"
    _write_csv_signals(path)
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    signals = await _collect(HistoricalSensor(path))

    observed_by_path = {observed_path: flags for observed_path, flags in observed}
    assert [signal.market_id for signal in signals] == ["m1"]
    assert observed_by_path[path] & no_follow_flag


@pytest.mark.asyncio
async def test_historical_sensor_rejects_hardlink_swap_during_jsonl_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "signals.jsonl"
    _write_jsonl_signal(path)
    replacement_source = tmp_path / "replacement-signals.jsonl"
    _write_jsonl_signal(replacement_source)
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == path and not swapped:
            swapped = True
            path.unlink()
            os.link(replacement_source, path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="historical data cannot be read safely"):
        await _collect(HistoricalSensor(path))

    assert swapped is True


@pytest.mark.asyncio
async def test_historical_sensor_replay_speed_scales_sleep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jsonl_path = tmp_path / "signals.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                '{"market_id":"m1","token_id":"t1","venue":"polymarket","title":"First","yes_price":0.5,"volume_24h":1.0,"resolves_at":"2026-04-15T00:00:00Z","orderbook":{"bids":[],"asks":[]},"external_signal":{},"fetched_at":"2026-04-13T00:00:01Z","market_status":"open"}',
                '{"market_id":"m2","token_id":"t2","venue":"polymarket","title":"Second","yes_price":0.6,"volume_24h":2.0,"resolves_at":"2026-04-15T00:00:00Z","orderbook":{"bids":[],"asks":[]},"external_signal":{},"fetched_at":"2026-04-13T00:00:05Z","market_status":"open"}',
            ]
        ),
        encoding="utf-8",
    )
    sleep_durations: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_durations.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    signals = await _collect(HistoricalSensor(jsonl_path, replay_speed=2.0))

    assert [signal.market_id for signal in signals] == ["m1", "m2"]
    assert sleep_durations == [2.0]


@pytest.mark.asyncio
async def test_sensor_stream_fans_historical_sensor_into_queue() -> None:
    stream = SensorStream()
    await stream.start([HistoricalSensor(FIXTURE_PATH)])

    signals = [
        await asyncio.wait_for(stream.queue.get(), timeout=1.0) for _ in range(100)
    ]
    await asyncio.wait_for(stream.stop(), timeout=5.0)

    assert len(signals) == 100
    assert [signal.fetched_at for signal in signals] == sorted(
        signal.fetched_at for signal in signals
    )


@pytest.mark.asyncio
async def test_sensor_stream_stop_cancels_tasks_within_timeout() -> None:
    class NeverEndingSensor:
        def __aiter__(self) -> AsyncIterator[MarketSignal]:
            return self

        async def __anext__(self) -> MarketSignal:
            await asyncio.sleep(60.0)
            raise StopAsyncIteration

    stream = SensorStream()
    await stream.start([NeverEndingSensor()])

    await asyncio.wait_for(stream.stop(), timeout=5.0)

    assert stream.tasks == ()
