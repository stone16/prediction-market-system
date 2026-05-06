from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from pms.alerting.scheduler import next_trigger


def test_next_trigger_same_day_before_2200_asia_shanghai() -> None:
    tz = ZoneInfo("Asia/Shanghai")

    assert next_trigger(tz, datetime(2026, 5, 6, 13, 0, tzinfo=tz)) == datetime(
        2026,
        5,
        6,
        22,
        0,
        tzinfo=tz,
    )


def test_next_trigger_exactly_at_2200_moves_to_next_day() -> None:
    tz = ZoneInfo("Asia/Shanghai")

    assert next_trigger(tz, datetime(2026, 5, 6, 22, 0, tzinfo=tz)) == datetime(
        2026,
        5,
        7,
        22,
        0,
        tzinfo=tz,
    )


def test_next_trigger_after_2200_moves_to_next_day() -> None:
    tz = ZoneInfo("Asia/Shanghai")

    assert next_trigger(tz, datetime(2026, 5, 6, 23, 0, tzinfo=tz)) == datetime(
        2026,
        5,
        7,
        22,
        0,
        tzinfo=tz,
    )
