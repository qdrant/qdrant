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
#[derive(Copy, Clone, Debug, Into)]
pub struct PyPayloadStorageType(PayloadStorageType);

#[pymethods]
impl PyPayloadStorageType {
    #[classattr]
    pub const MMAP: Self = Self(PayloadStorageType::Mmap);

    #[classattr]
    pub const IN_RAM_MMAP: Self = Self(PayloadStorageType::InRamMmap);
}

impl PyPayloadStorageType {
    fn _variants(storage_type: PayloadStorageType) {
        #[allow(unreachable_patterns)]
        match storage_type {
            PayloadStorageType::Mmap => (),
            PayloadStorageType::InRamMmap => (),
            _ => todo!(), // TODO: Ignore RocksDB storage types
        }
    }
}
