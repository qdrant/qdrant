pub mod quantization;
pub mod sparse_vector_data;
pub mod vector_data;

use std::collections::HashMap;
use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

pub use self::quantization::*;
pub use self::sparse_vector_data::*;
pub use self::vector_data::*;
use crate::repr::*;

#[pyclass(name = "SegmentConfig")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySegmentConfig(SegmentConfig);

#[pyclass_repr]
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

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PySegmentConfig {
    fn _getters(self) {
        // Every field should have a getter method
        let SegmentConfig {
            vector_data: _,
            sparse_vector_data: _,
            payload_storage_type: _,
        } = self.0;
    }
}

#[pyclass(name = "PayloadStorageType")]
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
