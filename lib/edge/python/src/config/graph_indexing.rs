use std::fmt;

use pyo3::prelude::*;
use segment::types::{IndexingAlgorithm, IndexingEffort};

use crate::repr::*;

/// Algorithm that builds the HNSW graph.
#[pyclass(name = "IndexingAlgorithm", from_py_object, eq, eq_int)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PyIndexingAlgorithm {
    /// PiPNN for the main graph of large segments with vectors it supports, HNSW otherwise.
    Auto,
    /// Insert the points one by one into the graph. `ef_construct` controls the quality.
    Hnsw,
}

#[pymethods]
impl PyIndexingAlgorithm {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyIndexingAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Auto => "Auto",
            Self::Hnsw => "Hnsw",
        };
        f.simple_enum::<Self>(repr)
    }
}

impl From<IndexingAlgorithm> for PyIndexingAlgorithm {
    fn from(algorithm: IndexingAlgorithm) -> Self {
        match algorithm {
            IndexingAlgorithm::Auto => PyIndexingAlgorithm::Auto,
            IndexingAlgorithm::Hnsw => PyIndexingAlgorithm::Hnsw,
        }
    }
}

impl From<PyIndexingAlgorithm> for IndexingAlgorithm {
    fn from(algorithm: PyIndexingAlgorithm) -> Self {
        match algorithm {
            PyIndexingAlgorithm::Auto => IndexingAlgorithm::Auto,
            PyIndexingAlgorithm::Hnsw => IndexingAlgorithm::Hnsw,
        }
    }
}

/// How much work the graph build may spend on quality, for graphs built by PiPNN.
#[pyclass(name = "IndexingEffort", from_py_object, eq, eq_int)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PyIndexingEffort {
    Low,
    Medium,
    High,
}

#[pymethods]
impl PyIndexingEffort {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyIndexingEffort {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Low => "Low",
            Self::Medium => "Medium",
            Self::High => "High",
        };
        f.simple_enum::<Self>(repr)
    }
}

impl From<IndexingEffort> for PyIndexingEffort {
    fn from(effort: IndexingEffort) -> Self {
        match effort {
            IndexingEffort::Low => PyIndexingEffort::Low,
            IndexingEffort::Medium => PyIndexingEffort::Medium,
            IndexingEffort::High => PyIndexingEffort::High,
        }
    }
}

impl From<PyIndexingEffort> for IndexingEffort {
    fn from(effort: PyIndexingEffort) -> Self {
        match effort {
            PyIndexingEffort::Low => IndexingEffort::Low,
            PyIndexingEffort::Medium => IndexingEffort::Medium,
            PyIndexingEffort::High => IndexingEffort::High,
        }
    }
}
