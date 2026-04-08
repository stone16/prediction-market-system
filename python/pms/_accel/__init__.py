"""Optional Rust acceleration layer (Phase 3E).

This module is the lazy detection layer between the Python codebase and
the three optional Rust crates under ``rust/crates/``. Production code
that wants to use an accelerated implementation imports the
``HAS_*_RS`` flag plus the (possibly ``None``) module reference from
here, and falls back to the canonical pure-Python implementation when
the flag is ``False``.

The contract this module locks in:

* Importing ``pms._accel`` **never** raises, even when none of the
  Rust crates are built. The detection is wrapped in
  ``try/except ImportError`` per crate.
* The ``HAS_*_RS`` flags are always plain ``bool`` so callers can
  branch on them without worrying about truthy-but-not-true semantics.
* When a Rust crate is missing, the corresponding module reference is
  ``None`` (not a half-built stub). Callers MUST check the flag before
  reaching for the module.

Build instructions live in ``rust/README.md``. The default
``uv sync`` install does not pull or build the Rust crates — they are
opt-in performance overrides that require a Rust toolchain + maturin
to install. See the README for why three independent crates instead
of one extension module.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Crate detection — each crate gets its own try/except so a build
# failure on one does not mask another. Order is alphabetical for
# stability of the import side effects.
# ---------------------------------------------------------------------------


datafeed_rs: Any | None
try:
    import pms_datafeed_rs as datafeed_rs  # type: ignore[import-not-found, no-redef]

    HAS_DATAFEED_RS: bool = True
except ImportError:
    datafeed_rs = None
    HAS_DATAFEED_RS = False


embeddings_rs: Any | None
try:
    import pms_embeddings_rs as embeddings_rs  # type: ignore[import-not-found, no-redef]

    HAS_EMBEDDINGS_RS: bool = True
except ImportError:
    embeddings_rs = None
    HAS_EMBEDDINGS_RS = False


executor_rs: Any | None
try:
    import pms_executor_rs as executor_rs  # type: ignore[import-not-found, no-redef]

    HAS_EXECUTOR_RS: bool = True
except ImportError:
    executor_rs = None
    HAS_EXECUTOR_RS = False


def accel_status() -> dict[str, bool]:
    """Return a snapshot of which Rust accel crates are loadable.

    Useful for diagnostics, version-banner output, or test reporting.
    The returned dict is fresh on every call so callers can mutate it
    safely.
    """
    return {
        "datafeed_rs": HAS_DATAFEED_RS,
        "embeddings_rs": HAS_EMBEDDINGS_RS,
        "executor_rs": HAS_EXECUTOR_RS,
    }


__all__ = [
    "HAS_DATAFEED_RS",
    "HAS_EMBEDDINGS_RS",
    "HAS_EXECUTOR_RS",
    "accel_status",
    "datafeed_rs",
    "embeddings_rs",
    "executor_rs",
]
