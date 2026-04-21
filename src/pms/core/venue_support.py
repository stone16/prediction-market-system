from __future__ import annotations

from pms.core.enums import Venue
from pms.core.exceptions import KalshiStubError


def kalshi_stub_error(context: str) -> KalshiStubError:
    return KalshiStubError(
        "Kalshi adapter is not implemented in v1 — see "
        "`pms-correctness-bundle-v1` Out of Scope §3"
        f" ({context})"
    )


def normalize_venue(value: object, *, context: str) -> str:
    if value is None:
        msg = f"{context} is missing venue"
        raise ValueError(msg)

    venue = str(value).strip().lower()
    if venue in {Venue.POLYMARKET.value, Venue.KALSHI.value}:
        return venue

    msg = f"{context} received unsupported venue {value!r}"
    raise ValueError(msg)
