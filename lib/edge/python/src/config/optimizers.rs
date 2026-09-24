//! Python wrapper for edge optimizer config (optional in EdgeConfig).

use edge::EdgeOptimizersConfig;
use pyo3::prelude::*;

use crate::repr::*;

/// Optimizer-related configuration for EdgeConfig.
///
/// Args:
///     deleted_threshold: Min fraction of deleted vectors to run vacuum (default 0.2).
///     vacuum_min_vector_number: Min vectors in segment to run vacuum (default 1000).
///     default_segment_number: Target number of segments (0 = auto).
///     max_segment_size: Max segment size in KB.
///     indexing_threshold: Indexing threshold in KB.
///     prevent_unoptimized: If enabled, points written to segments larger than the indexing threshold
///         become deferred (excluded from read/search until those segments are optimized).
///         Updates with `wait=true` will only return after the deferred points become visible.
#[pyclass(name = "EdgeOptimizersConfig", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyEdgeOptimizersConfig(pub EdgeOptimizersConfig);

#[pyclass_repr]
#[pymethods]
impl PyEdgeOptimizersConfig {
    #[new]
    #[pyo3(signature = (deleted_threshold=None, vacuum_min_vector_number=None, default_segment_number=None, max_segment_size=None, indexing_threshold=None, prevent_unoptimized=None))]
    pub fn new(
        deleted_threshold: Option<f64>,
        vacuum_min_vector_number: Option<usize>,
        default_segment_number: Option<usize>,
        max_segment_size: Option<usize>,
        indexing_threshold: Option<usize>,
        prevent_unoptimized: Option<bool>,
    ) -> Self {
        Self(EdgeOptimizersConfig {
            deleted_threshold,
            vacuum_min_vector_number,
            default_segment_number,
            max_segment_size,
            indexing_threshold,
            prevent_unoptimized,
        })
    }

    /// Deleted threshold.
    #[getter]
    pub fn deleted_threshold(&self) -> Option<f64> {
        self.0.deleted_threshold
    }

    /// Vacuum min vector number.
    #[getter]
    pub fn vacuum_min_vector_number(&self) -> Option<usize> {
        self.0.vacuum_min_vector_number
    }

    /// Default segment number.
    #[getter]
    pub fn default_segment_number(&self) -> Option<usize> {
        self.0.default_segment_number
    }

    /// Max segment size in KB.
    #[getter]
    pub fn max_segment_size(&self) -> Option<usize> {
        self.0.max_segment_size
    }

    /// Indexing threshold in KB.
    #[getter]
    pub fn indexing_threshold(&self) -> Option<usize> {
        self.0.indexing_threshold
    }

    /// Prevent unoptimized flag.
    #[getter]
    pub fn prevent_unoptimized(&self) -> Option<bool> {
        self.0.prevent_unoptimized
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}
