//! Python wrapper for edge optimizer config (optional in EdgeConfig).

use edge::EdgeOptimizersConfig;
use pyo3::prelude::*;

use crate::repr::*;

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

    #[getter]
    pub fn deleted_threshold(&self) -> Option<f64> {
        self.0.deleted_threshold
    }

    #[getter]
    pub fn vacuum_min_vector_number(&self) -> Option<usize> {
        self.0.vacuum_min_vector_number
    }

    #[getter]
    pub fn default_segment_number(&self) -> Option<usize> {
        self.0.default_segment_number
    }

    #[getter]
    pub fn max_segment_size(&self) -> Option<usize> {
        self.0.max_segment_size
    }

    #[getter]
    pub fn indexing_threshold(&self) -> Option<usize> {
        self.0.indexing_threshold
    }

    #[getter]
    pub fn prevent_unoptimized(&self) -> Option<bool> {
        self.0.prevent_unoptimized
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}
