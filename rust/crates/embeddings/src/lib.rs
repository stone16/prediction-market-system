//! Rust acceleration scaffold for the PMS embedding cosine-similarity batch.
//!
//! Phase 3E ships this crate as a buildable PyO3 stub. The eventual
//! responsibility is the O(N²) all-pairs cosine similarity scan that
//! `pms.embeddings.engine.EmbeddingEngine.find_similar_pairs` performs
//! today in pure NumPy. For market corpora >10k titles the NumPy
//! implementation becomes the dominant cost in the correlation
//! pipeline; a SIMD-friendly Rust loop is the natural acceleration
//! target.
//!
//! The current module exports a single `version()` function so the
//! crate compiles end-to-end and the Python accel layer can detect
//! it. The real cosine batch is deliberately `unimplemented!()` —
//! see `rust/README.md` for the design and the Python fallback that
//! remains canonical until this is filled in.

use pyo3::prelude::*;

/// Returns the crate's package version.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Hot-path placeholder: compute pairwise cosine similarity for a
/// flat `(N, D)` matrix of normalized vectors and return every pair
/// whose similarity exceeds `threshold`.
///
/// Currently `unimplemented!()` — the Python fallback in
/// `pms.embeddings.engine.EmbeddingEngine.find_similar_pairs` stays
/// the source of truth until this is filled in.
#[pyfunction]
#[allow(unused_variables)]
fn find_similar_pairs(
    embeddings: Vec<Vec<f32>>,
    threshold: f32,
) -> PyResult<Vec<(usize, usize, f32)>> {
    Err(pyo3::exceptions::PyNotImplementedError::new_err(
        "pms_embeddings_rs::find_similar_pairs is scaffolded but not yet implemented",
    ))
}

#[pymodule]
fn pms_embeddings_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(find_similar_pairs, m)?)?;
    Ok(())
}
