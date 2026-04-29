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

#[pyclass(name = "EdgeConfig", from_py_object)]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyEdgeConfig(pub EdgeConfig);

#[pyclass_repr]
#[pymethods]
impl PyEdgeConfig {
    #[new]
    #[pyo3(signature = (vectors=None, sparse_vectors=None, on_disk_payload=true, hnsw_config=None, quantization_config=None, optimizers=None))]
    pub fn new(
        #[pyo3(from_py_with = option_edge_vectors_helper)] vectors: Option<
            HashMap<String, PyEdgeVectorParams>,
        >,
        sparse_vectors: Option<HashMap<String, PyEdgeSparseVectorParams>>,
        on_disk_payload: bool,
        hnsw_config: Option<PyHnswIndexConfig>,
        quantization_config: Option<PyQuantizationConfig>,
        optimizers: Option<PyEdgeOptimizersConfig>,
    ) -> PyResult<Self> {
        let vectors = vectors.unwrap_or_default();
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
            hnsw_config: hnsw_config.map(|h| h.0).unwrap_or_default(),
            quantization_config: quantization_config.map(QuantizationConfig::from),
            optimizers: optimizers.map(|o| o.0).unwrap_or_default(),
        }))
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
        let EdgeConfig {
            on_disk_payload: _,
            vectors: _,
            sparse_vectors: _,
            hnsw_config: _,
            quantization_config: _,
            optimizers: _,
        } = self.0;
    }
}

fn option_edge_vectors_helper(
    config: &Bound<'_, PyAny>,
) -> PyResult<Option<HashMap<String, PyEdgeVectorParams>>> {
    if config.is_none() {
        return Ok(None);
    }
    edge_vectors_helper(config).map(Some)
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
