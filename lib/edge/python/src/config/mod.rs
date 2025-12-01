pub mod quantization;
pub mod sparse_vector_data;
pub mod vector_data;

use std::collections::HashMap;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

pub use self::quantization::*;
pub use self::sparse_vector_data::*;
pub use self::vector_data::*;

#[pyclass(name = "SegmentConfig")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySegmentConfig(SegmentConfig);

#[pymethods]
impl PySegmentConfig {
    #[new]
    pub fn new(
        vector_data: HashMap<String, PyVectorDataConfig>,
        sparse_vector_data: HashMap<String, PySparseVectorDataConfig>,
        payload_storage_type: PyPayloadStorageType,
    ) -> Self {
        Self(SegmentConfig {
            vector_data: PyVectorDataConfig::peel_map(vector_data),
            sparse_vector_data: PySparseVectorDataConfig::peel_map(sparse_vector_data),
            payload_storage_type: PayloadStorageType::from(payload_storage_type),
        })
    }

    #[getter]
    pub fn vector_data(&self) -> &HashMap<String, PyVectorDataConfig> {
        PyVectorDataConfig::wrap_map_ref(&self.0.vector_data)
    }

    #[getter]
    pub fn sparse_vector_data(&self) -> &HashMap<String, PySparseVectorDataConfig> {
        PySparseVectorDataConfig::wrap_map_ref(&self.0.sparse_vector_data)
    }

    #[getter]
    pub fn payload_storage_type(&self) -> PyPayloadStorageType {
        PyPayloadStorageType::from(self.0.payload_storage_type)
    }
}

#[pyclass(name = "PayloadStorageType")]
#[derive(Copy, Clone, Debug)]
pub enum PyPayloadStorageType {
    Mmap,
    InRamMmap,
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
