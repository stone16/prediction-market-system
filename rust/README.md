# Rust Acceleration Scaffolding

Phase 3E ships the Rust workspace structure for three performance-
critical paths the v1 spec reserves for compiled acceleration. The
canonical implementations stay in pure Python under `python/pms/`;
the Rust crates exist as **optional** drop-in replacements that the
Python package detects at import time and uses transparently when
they are installed.

## Status

| Crate                  | Status     | Hot path                                                  | Python source of truth                                       |
|------------------------|------------|-----------------------------------------------------------|--------------------------------------------------------------|
| `pms_datafeed_rs`      | scaffolded | Polymarket / Kalshi raw payload → normalized `Market`     | `python/pms/connectors/polymarket.py`, `kalshi.py`            |
| `pms_executor_rs`      | scaffolded | Exponential backoff + idempotency state for retry loop    | `python/pms/execution/executor.py`                           |
| `pms_embeddings_rs`    | scaffolded | All-pairs cosine similarity over a flat `(N, D)` matrix   | `python/pms/embeddings/engine.py`                            |

"Scaffolded" means: the crate compiles end-to-end under PyO3 0.22,
exports a `version()` smoke function the Python accel layer probes,
and declares each hot-path entry point with an `unimplemented!()`
body so the path forward is concrete. Filling in the actual
implementations is reserved for a future task — the Python fallback
remains the canonical reference until then.

## Build

The Rust crates are not built by `uv sync`. Building them requires
two extra tools that the default install does **not** install:

```bash
# 1. Install a recent Rust toolchain (1.78+).
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable

# 2. Install maturin (the PyO3 build frontend).
uv pip install maturin

# 3. Build each crate in turn. ``maturin develop`` builds the
#    extension and installs it into the active venv so the
#    ``import pms_*_rs`` line in the accel layer succeeds.
cd rust
maturin develop --release -m crates/embeddings/Cargo.toml
maturin develop --release -m crates/datafeed/Cargo.toml
maturin develop --release -m crates/executor/Cargo.toml
```

After a successful `maturin develop` round, restart the Python
process and verify the accel layer detected the build:

```python
>>> from pms._accel import HAS_EMBEDDINGS_RS, HAS_DATAFEED_RS, HAS_EXECUTOR_RS
>>> HAS_EMBEDDINGS_RS, HAS_DATAFEED_RS, HAS_EXECUTOR_RS
(True, True, True)
```

## Python fallback wiring pattern

The accel layer at `python/pms/_accel/__init__.py` does the lazy
detection. Each accel module is wrapped in a `try/except ImportError`
that sets a `HAS_*_RS` flag and stashes the module reference (or
`None`) on import:

```python
# python/pms/_accel/__init__.py
try:
    import pms_embeddings_rs as _embeddings_rs
    HAS_EMBEDDINGS_RS = True
except ImportError:
    _embeddings_rs = None
    HAS_EMBEDDINGS_RS = False
```

Production code paths that want to use the accelerated implementation
look like this:

```python
from pms._accel import HAS_EMBEDDINGS_RS, embeddings_rs

def find_similar_pairs(embeddings, threshold):
    if HAS_EMBEDDINGS_RS:
        return embeddings_rs.find_similar_pairs(embeddings, threshold)
    # Pure-Python fallback — the canonical reference implementation
    # in pms.embeddings.engine.EmbeddingEngine.
    return _python_find_similar_pairs(embeddings, threshold)
```

The accel layer's tests
(`tests/test_accel_fallback.py`) lock in the contract:

* All `HAS_*_RS` flags are bool — never raise on import even when
  the Rust crates are not built.
* When a Rust crate is missing, the corresponding accel attribute
  is `None` (not a partial / broken stub).
* The Python fallback path is always callable.

## Why three crates instead of one

Three independent crates means each hot path can be:

1. Built independently — partial wins ship before the slowest crate
   is finished.
2. Excluded individually — a build failure on one crate (toolchain
   mismatch, OS-specific bug) does not block the others.
3. Versioned independently — the embedding crate's API surface is
   completely unrelated to the executor crate's, so coupling them
   into one extension would be a layering mistake.

The Cargo workspace shares dependency versions and `[workspace.package]`
metadata so the three crates stay aligned without duplicating
boilerplate.

## What "implemented" looks like

For each hot-path placeholder, the future task should:

1. Replace the `unimplemented!()` body with the real Rust
   implementation, mirroring the Python reference.
2. Add a unit test on the Rust side under `rust/crates/<name>/tests/`
   exercising the hot path against a small fixture.
3. Update `python/pms/_accel/__init__.py` to expose the new entry
   point.
4. Wire the production code path to prefer the accelerated version
   via the `HAS_*_RS` flag.
5. Add a Python-side benchmark comparing the two implementations on
   a realistic input shape and document the speedup in this README.

The Python implementation must remain available and tested as the
fallback — every accel path must have a Python equivalent so the
project remains installable on platforms where Rust acceleration
is unavailable (CI without rustc, locked-down corporate Pythons, …).
