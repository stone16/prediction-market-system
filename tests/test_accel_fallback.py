"""Tests for the optional Rust acceleration layer (Phase 3E).

These tests run on a stock ``uv sync`` install — they do **not**
require any Rust crate to be built. The contract being tested is
that ``pms._accel`` imports cleanly with the Python fallback active,
exposes the right shape, and that production code can safely branch
on the ``HAS_*_RS`` flags without crashing when the crates are absent.

Once a future task wires real Rust crates, the same tests should
keep passing — they assert on the *contract*, not on which side
(Rust or Python) is currently active.
"""

from __future__ import annotations

import importlib

import pms._accel as accel


def test_accel_module_imports_cleanly() -> None:
    """Importing ``pms._accel`` must succeed even when no Rust crate is built."""
    assert accel is not None


def test_all_has_flags_are_bool() -> None:
    """The ``HAS_*_RS`` flags must always be plain ``bool`` (not None / object)."""
    assert isinstance(accel.HAS_DATAFEED_RS, bool)
    assert isinstance(accel.HAS_EMBEDDINGS_RS, bool)
    assert isinstance(accel.HAS_EXECUTOR_RS, bool)


def test_module_references_are_none_when_flag_is_false() -> None:
    """When a flag is False the corresponding module reference must be ``None``.

    Half-loaded stubs are forbidden by the contract — production code
    that branches on ``HAS_*_RS`` and reaches for the module would
    crash on a None attribute access otherwise.
    """
    if not accel.HAS_DATAFEED_RS:
        assert accel.datafeed_rs is None
    if not accel.HAS_EMBEDDINGS_RS:
        assert accel.embeddings_rs is None
    if not accel.HAS_EXECUTOR_RS:
        assert accel.executor_rs is None


def test_accel_status_returns_full_snapshot() -> None:
    status = accel.accel_status()
    assert isinstance(status, dict)
    assert set(status.keys()) == {"datafeed_rs", "embeddings_rs", "executor_rs"}
    assert all(isinstance(v, bool) for v in status.values())


def test_accel_status_returns_fresh_dict() -> None:
    """``accel_status`` must hand back a copy callers can mutate safely."""
    a = accel.accel_status()
    b = accel.accel_status()
    a["datafeed_rs"] = not a["datafeed_rs"]
    assert b["datafeed_rs"] == accel.HAS_DATAFEED_RS  # b is unaffected


def test_re_importing_accel_keeps_consistent_flags() -> None:
    """The ``HAS_*_RS`` flags must not flicker across re-imports.

    Module-level state should be set once at import time and stay
    stable for the process lifetime — re-importing the module
    (e.g. via ``importlib.reload``) must produce the same flag
    values, otherwise downstream caching keyed on the flag would
    become unreliable.
    """
    snapshot_before = accel.accel_status()
    importlib.reload(accel)
    snapshot_after = accel.accel_status()
    assert snapshot_before == snapshot_after


def test_default_install_has_no_rust_crates() -> None:
    """Smoke test for the documented default-install state.

    The Phase 3E commit explicitly does not build the Rust crates as
    part of ``uv sync``; this test pins that behaviour so a future
    accidental wiring of the crates into the default install would
    surface as a test diff. When a future task lands a built crate,
    update this test to assert the new expected default.
    """
    status = accel.accel_status()
    assert status == {
        "datafeed_rs": False,
        "embeddings_rs": False,
        "executor_rs": False,
    }, (
        f"unexpected default-install accel status: {status} — if a "
        f"crate has been wired into uv sync, update this test."
    )
