//! Rust acceleration scaffold for the PMS executor inner retry loop.
//!
//! Phase 3E ships this crate as a buildable PyO3 stub. The eventual
//! responsibility is the per-order retry math: exponential backoff
//! computation, idempotency-key dedup, and the in-memory positions
//! ledger update. The pure-Python implementation in
//! `python/pms/execution/executor.py` is the canonical reference and
//! stays the source of truth until this is filled in.
//!
//! The current module exports a single `version()` function so the
//! crate compiles end-to-end and the Python accel layer can detect
//! it. Real ledger / backoff implementations are deliberately left
//! `unimplemented!()` — see `rust/README.md` for the design.

use pyo3::prelude::*;

/// Returns the crate's package version.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Hot-path placeholder: compute the next exponential backoff delay
/// for the executor's retry loop, given the attempt index and the
/// (initial, multiplier) tuple.
///
/// Currently `unimplemented!()` — the Python fallback in
/// `pms.execution.executor.OrderExecutor.submit_order` stays the
/// source of truth until this is filled in.
#[pyfunction]
#[allow(unused_variables)]
fn compute_backoff(attempt: usize, initial: f64, multiplier: f64) -> PyResult<f64> {
    Err(pyo3::exceptions::PyNotImplementedError::new_err(
        "pms_executor_rs::compute_backoff is scaffolded but not yet implemented",
    ))
}

#[pymodule]
fn pms_executor_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(compute_backoff, m)?)?;
    Ok(())
}
