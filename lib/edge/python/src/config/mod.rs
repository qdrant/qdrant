pub mod quantization;
pub mod sparse_vector_data;
pub mod vector_data;

use std::collections::HashMap;
use std::fmt;

use derive_more::Into;
use edge::config::{EdgeOptimizersConfig, EdgeShardConfig};
use pyo3::prelude::*;
use segment::types::{PayloadStorageType, QuantizationConfig, VectorNameBuf};

pub use self::quantization::*;
pub use self::sparse_vector_data::*;
pub use self::vector_data::*;
use crate::repr::*;

#[pyclass(name = "EdgeConfig", from_py_object)]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyEdgeConfig(pub EdgeShardConfig);

#[pyclass_repr]
#[pymethods]
impl PyEdgeConfig {
    #[new]
    #[pyo3(signature = (vectors, sparse_vectors=None, on_disk_payload=true, hnsw_config=None, quantization_config=None, optimizers=None))]
    pub fn new(
        #[pyo3(from_py_with = edge_vectors_helper)] vectors: HashMap<String, PyEdgeVectorParams>,
        sparse_vectors: Option<HashMap<String, PyEdgeSparseVectorParams>>,
        on_disk_payload: bool,
        hnsw_config: Option<PyHnswIndexConfig>,
        quantization_config: Option<PyQuantizationConfig>,
        optimizers: Option<PyEdgeOptimizersConfig>,
    ) -> Self {
        let vectors = PyEdgeVectorParams::peel_map(vectors);
        let sparse_vectors = sparse_vectors
            .map(PyEdgeSparseVectorParams::peel_map)
            .unwrap_or_default();
        let vectors: HashMap<VectorNameBuf, _> = vectors.into_iter().collect();
        let sparse_vectors: HashMap<VectorNameBuf, _> = sparse_vectors.into_iter().collect();
        Self(EdgeShardConfig {
            on_disk_payload,
            vectors,
            sparse_vectors,
            hnsw_config: hnsw_config.map(|h| h.0).unwrap_or_default(),
            quantization_config: quantization_config.map(QuantizationConfig::from),
            optimizers: optimizers.map(|o| o.0).unwrap_or_default(),
        })
    }

    #[getter]
    pub fn vectors(&self) -> HashMap<String, PyEdgeVectorParams> {
        PyEdgeVectorParams::wrap_map(&self.0.vectors)
    }

    #[getter]
    pub fn sparse_vectors(&self) -> HashMap<String, PyEdgeSparseVectorParams> {
        PyEdgeSparseVectorParams::wrap_map(&self.0.sparse_vectors)
    }

    #[getter]
    pub fn on_disk_payload(&self) -> bool {
        self.0.on_disk_payload
    }

    #[getter]
    pub fn hnsw_config(&self) -> PyHnswIndexConfig {
        PyHnswIndexConfig(self.0.hnsw_config)
    }

    #[getter]
    pub fn quantization_config(&self) -> Option<PyQuantizationConfig> {
        self.0.quantization_config.clone().map(PyQuantizationConfig)
    }

    #[getter]
    pub fn optimizers(&self) -> PyEdgeOptimizersConfig {
        PyEdgeOptimizersConfig(self.0.optimizers.clone())
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyEdgeConfig {
    fn _getters(self) {
        let EdgeShardConfig {
            on_disk_payload: _,
            vectors: _,
            sparse_vectors: _,
            hnsw_config: _,
            quantization_config: _,
            optimizers: _,
        } = self.0;
    }
}

/// Python wrapper for optimizer-related config (optional in EdgeConfig).
#[pyclass(name = "EdgeOptimizersConfig", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyEdgeOptimizersConfig(pub EdgeOptimizersConfig);

#[pyclass_repr]
#[pymethods]
impl PyEdgeOptimizersConfig {
    #[new]
    #[pyo3(signature = (deleted_threshold=None, vacuum_min_vector_number=None, default_segment_number=0, max_segment_size=None, indexing_threshold=None, prevent_unoptimized=None))]
    pub fn new(
        deleted_threshold: Option<f64>,
        vacuum_min_vector_number: Option<usize>,
        default_segment_number: usize,
        max_segment_size: Option<usize>,
        indexing_threshold: Option<usize>,
        prevent_unoptimized: Option<bool>,
    ) -> Self {
        Self(EdgeOptimizersConfig {
            deleted_threshold: deleted_threshold.unwrap_or(0.2),
            vacuum_min_vector_number: vacuum_min_vector_number.unwrap_or(1000),
            default_segment_number,
            max_segment_size,
            indexing_threshold,
            prevent_unoptimized,
        })
    }

    #[getter]
    pub fn deleted_threshold(&self) -> f64 {
        self.0.deleted_threshold
    }

    #[getter]
    pub fn vacuum_min_vector_number(&self) -> usize {
        self.0.vacuum_min_vector_number
    }

    #[getter]
    pub fn default_segment_number(&self) -> usize {
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

#[pyclass(name = "PayloadStorageType", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyPayloadStorageType {
    Mmap,
    InRamMmap,
}

#[pymethods]
impl PyPayloadStorageType {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyPayloadStorageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            PyPayloadStorageType::Mmap => "Mmap",
            PyPayloadStorageType::InRamMmap => "InRamMmap",
        };

        f.simple_enum::<Self>(repr)
    }
}

impl From<PayloadStorageType> for PyPayloadStorageType {
    fn from(storage_type: PayloadStorageType) -> Self {
        #[allow(unreachable_patterns)]
        match storage_type {
            PayloadStorageType::Mmap => PyPayloadStorageType::Mmap,
            PayloadStorageType::InRamMmap => PyPayloadStorageType::InRamMmap,
            _ => unimplemented!("RocksDB-backed storage types are not supported by Qdrant Edge"),
        }
    }
}

impl From<PyPayloadStorageType> for PayloadStorageType {
    fn from(storage_type: PyPayloadStorageType) -> Self {
        match storage_type {
            PyPayloadStorageType::Mmap => PayloadStorageType::Mmap,
            PyPayloadStorageType::InRamMmap => PayloadStorageType::InRamMmap,
        }
    }
}

fn edge_vectors_helper(config: &Bound<'_, PyAny>) -> PyResult<HashMap<String, PyEdgeVectorParams>> {
    #[derive(FromPyObject)]
    enum Helper {
        Default(PyEdgeVectorParams),
        Explicit(HashMap<String, PyEdgeVectorParams>),
    }

    let config = match config.extract()? {
        Helper::Default(default) => {
            let mut map = HashMap::new();
            map.insert("".to_string(), default);
            map
        }
        Helper::Explicit(map) => map,
    };

    Ok(config)
}
