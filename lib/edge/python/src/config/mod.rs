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
        // TODO: Transmute!?
        let vector_data = vector_data
            .into_iter()
            .map(|(vector, config)| (vector, VectorDataConfig::from(config)))
            .collect();

        // TODO: Transmute!?
        let sparse_vector_data = sparse_vector_data
            .into_iter()
            .map(|(vector, config)| (vector, SparseVectorDataConfig::from(config)))
            .collect();

        Self(SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type: PayloadStorageType::from(payload_storage_type),
        })
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
