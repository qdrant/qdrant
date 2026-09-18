pub mod optimizers;
pub mod quantization;
pub mod sparse_vector_data;
pub mod vector_data;

use std::collections::HashMap;

use derive_more::Into;
use edge::EdgeConfig;
use pyo3::prelude::*;
use segment::types::{QuantizationConfig, VectorNameBuf};

pub use self::optimizers::*;
pub use self::quantization::*;
pub use self::sparse_vector_data::*;
pub use self::vector_data::*;
use crate::repr::*;

/// Configuration for creating a new Qdrant Edge shard.
#[pyclass(name = "EdgeConfig", from_py_object)]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyEdgeConfig(pub EdgeConfig);

#[pyclass_repr]
#[pymethods]
impl PyEdgeConfig {
    /// Create an EdgeConfig.
    ///
    /// Parameters left as None are "not specified": when loading an existing shard each
    /// one resolves through provided -> persisted -> derived from segments -> default,
    /// so an unspecified parameter keeps the shard as it is. vectors and sparse_vectors
    /// define the stored data: if provided they are validated for compatibility against
    /// the existing segments, if omitted they are inherited from the shard.
    ///
    /// Args:
    ///     vectors: Dense vector configuration. Can be a single EdgeVectorParams for
    ///              the default vector (name "") or a dict of name -> EdgeVectorParams.
    ///              Optional if sparse_vectors is provided (sparse-only config).
    ///     sparse_vectors: Optional sparse vector configurations.
    ///     on_disk_payload: If True, store payload on disk (mmap); otherwise in RAM.
    ///                      None keeps the shard's current value (defaults to on-disk).
    ///     hnsw_config: Optional global HNSW config (used when building HNSW index).
    ///     quantization_config: Optional global quantization config.
    ///     optimizers: Optional optimizer settings.
    ///     max_search_threads: Number of threads in the shard's search thread pool, which
    ///                         runs per-segment reads in parallel and loads segments in
    ///                         parallel. None (the default) derives the count from the number
    ///                         of CPUs, matching the core search runtime.
    ///     search_pool_core: Pin every search pool thread to this CPU core (best-effort),
    ///                       bounding search compute to one core. None = OS scheduling.
    #[new]
    #[pyo3(signature = (vectors=None, sparse_vectors=None, on_disk_payload=None, hnsw_config=None, quantization_config=None, optimizers=None, max_search_threads=None, search_pool_core=None))]
    // Python-facing keyword arguments mirror EdgeConfig's fields one-to-one.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vectors: Option<PyEdgeVectors>,
        sparse_vectors: Option<HashMap<String, PyEdgeSparseVectorParams>>,
        on_disk_payload: Option<bool>,
        hnsw_config: Option<PyHnswIndexConfig>,
        quantization_config: Option<PyQuantizationConfig>,
        optimizers: Option<PyEdgeOptimizersConfig>,
        max_search_threads: Option<usize>,
        search_pool_core: Option<usize>,
    ) -> PyResult<Self> {
        let vectors = match vectors {
            Some(PyEdgeVectors::Default(default)) => HashMap::from([(String::new(), default)]),
            Some(PyEdgeVectors::Explicit(map)) => map,
            None => HashMap::new(),
        };
        let sparse_vectors = sparse_vectors.unwrap_or_default();
        if vectors.is_empty() && sparse_vectors.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "EdgeConfig requires at least one of vectors or sparse_vectors to be non-empty",
            ));
        }
        let vectors = PyEdgeVectorParams::peel_map(vectors);
        let sparse_vectors = PyEdgeSparseVectorParams::peel_map(sparse_vectors);
        let vectors: HashMap<VectorNameBuf, _> = vectors.into_iter().collect();
        let sparse_vectors: HashMap<VectorNameBuf, _> = sparse_vectors.into_iter().collect();
        Ok(Self(EdgeConfig {
            on_disk_payload,
            vectors,
            sparse_vectors,
            hnsw_config: hnsw_config.map(|h| h.0),
            quantization_config: quantization_config.map(QuantizationConfig::from),
            optimizers: optimizers.map(|o| o.0),
            wal_options: None,
            max_search_threads,
            search_pool_core,
        }))
    }

    /// Dense vector configurations.
    #[getter]
    pub fn vectors(&self) -> HashMap<String, PyEdgeVectorParams> {
        PyEdgeVectorParams::wrap_map(&self.0.vectors)
    }

    /// Sparse vector configurations.
    #[getter]
    pub fn sparse_vectors(&self) -> HashMap<String, PyEdgeSparseVectorParams> {
        PyEdgeSparseVectorParams::wrap_map(&self.0.sparse_vectors)
    }

    /// Whether payload is stored on disk, or None if not specified.
    #[getter]
    pub fn on_disk_payload(&self) -> Option<bool> {
        self.0.on_disk_payload
    }

    /// Global HNSW config, or None if not specified.
    #[getter]
    pub fn hnsw_config(&self) -> Option<PyHnswIndexConfig> {
        self.0.hnsw_config.map(PyHnswIndexConfig)
    }

    /// Global quantization config.
    #[getter]
    pub fn quantization_config(&self) -> Option<PyQuantizationConfig> {
        self.0.quantization_config.clone().map(PyQuantizationConfig)
    }

    /// Optimizer settings, or None if not specified.
    #[getter]
    pub fn optimizers(&self) -> Option<PyEdgeOptimizersConfig> {
        self.0.optimizers.clone().map(PyEdgeOptimizersConfig)
    }

    /// Number of threads in the search thread pool, or None for the CPU-derived default.
    #[getter]
    pub fn max_search_threads(&self) -> Option<usize> {
        self.0.max_search_threads
    }

    /// CPU core the search pool is pinned to, or None for OS scheduling.
    #[getter]
    pub fn search_pool_core(&self) -> Option<usize> {
        self.0.search_pool_core
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyEdgeConfig {
    fn _getters(self) {
        let EdgeConfig {
            on_disk_payload: _,
            vectors: _,
            sparse_vectors: _,
            hnsw_config: _,
            quantization_config: _,
            optimizers: _,
            wal_options: _,
            max_search_threads: _,
            search_pool_core: _,
        } = self.0;
    }
}

#[derive(Clone, Debug, FromPyObject)]
pub enum PyEdgeVectors {
    Default(PyEdgeVectorParams),
    Explicit(HashMap<String, PyEdgeVectorParams>),
}
