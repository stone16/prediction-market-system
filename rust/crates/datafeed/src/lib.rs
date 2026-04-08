//! Rust acceleration scaffold for the PMS data-feed normalization hot path.
//!
//! Phase 3E ships this crate as a buildable PyO3 stub. The eventual
//! responsibility is parsing raw venue payloads (Polymarket Gamma JSON,
//! Kalshi REST envelopes) into the normalized `Market` shape that lives
//! in `python/pms/models/market.py`. The pure-Python implementation in
//! `python/pms/connectors/polymarket.py` and `python/pms/connectors/kalshi.py`
//! is the canonical reference; this crate exists to provide a faster path
//! when the wire-volume warrants it.
//!
//! The current module exports a single `version()` function so the
//! crate compiles end-to-end and `python/pms/_accel/__init__.py` can
//! detect that it's installed. Real implementations of the hot paths
//! are intentionally left as `unimplemented!()` until a future task
//! explicitly takes them on — see `rust/README.md` for the design.

use pyo3::prelude::*;

/// Returns the crate's package version.
///
/// This is the smoke function the Python accel layer uses to confirm
/// the compiled extension is loadable. Tests assert it returns a
/// non-empty string; the value itself is the Cargo package version.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Hot-path placeholder: parse a raw Polymarket Gamma `/markets`
/// response payload into a list of normalized records.
///
/// Currently `unimplemented!()` — the Python fallback in
/// `pms.connectors.polymarket.PolymarketConnector._normalize_market`
/// stays the source of truth until this is filled in.
#[pyfunction]
#[allow(unused_variables)]
fn parse_polymarket_markets(_payload: &str) -> PyResult<Vec<String>> {
    Err(pyo3::exceptions::PyNotImplementedError::new_err(
        "pms_datafeed_rs::parse_polymarket_markets is scaffolded but not yet implemented",
    ))
}

#[pymodule]
fn pms_datafeed_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(parse_polymarket_markets, m)?)?;
    Ok(())
}
