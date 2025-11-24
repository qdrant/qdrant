use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

use super::quantization::*;

#[pyclass(name = "VectorDataConfig")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyVectorDataConfig(VectorDataConfig);

#[pymethods]
impl PyVectorDataConfig {
    #[new]
    pub fn new(
        size: usize,
        distance: PyDistance,
        storage_type: PyVectorStorageType,
        index: PyIndexes,
        quantization_config: Option<PyQuantizationConfig>,
        multivector_config: Option<PyMultiVectorConfig>,
        datatype: Option<PyVectorStorageDatatype>,
    ) -> Self {
        Self(VectorDataConfig {
            size,
            distance: Distance::from(distance),
            storage_type: VectorStorageType::from(storage_type),
            index: Indexes::from(index),
            quantization_config: quantization_config.map(QuantizationConfig::from),
            multivector_config: multivector_config.map(MultiVectorConfig::from),
            datatype: datatype.map(VectorStorageDatatype::from),
        })
    }
}

#[pyclass(name = "Distance")]
#[derive(Copy, Clone, Debug)]
pub enum PyDistance {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
}

impl From<Distance> for PyDistance {
    fn from(distance: Distance) -> Self {
        match distance {
            Distance::Cosine => PyDistance::Cosine,
            Distance::Euclid => PyDistance::Euclid,
            Distance::Dot => PyDistance::Dot,
            Distance::Manhattan => PyDistance::Manhattan,
        }
    }
}

impl From<PyDistance> for Distance {
    fn from(distance: PyDistance) -> Self {
        match distance {
            PyDistance::Cosine => Distance::Cosine,
            PyDistance::Euclid => Distance::Euclid,
            PyDistance::Dot => Distance::Dot,
            PyDistance::Manhattan => Distance::Manhattan,
        }
    }
}

#[pyclass(name = "VectorStorageType")]
#[derive(Copy, Clone, Debug)]
pub enum PyVectorStorageType {
    Memory,
    Mmap,
    ChunkedMmap,
    InRamChunkedMmap,
}

impl From<VectorStorageType> for PyVectorStorageType {
    fn from(storage_type: VectorStorageType) -> Self {
        match storage_type {
            VectorStorageType::Memory => PyVectorStorageType::Memory,
            VectorStorageType::Mmap => PyVectorStorageType::Mmap,
            VectorStorageType::ChunkedMmap => PyVectorStorageType::ChunkedMmap,
            VectorStorageType::InRamChunkedMmap => PyVectorStorageType::InRamChunkedMmap,
        }
    }
}

impl From<PyVectorStorageType> for VectorStorageType {
    fn from(storage_type: PyVectorStorageType) -> Self {
        match storage_type {
            PyVectorStorageType::Memory => VectorStorageType::Memory,
            PyVectorStorageType::Mmap => VectorStorageType::Mmap,
            PyVectorStorageType::ChunkedMmap => VectorStorageType::ChunkedMmap,
            PyVectorStorageType::InRamChunkedMmap => VectorStorageType::InRamChunkedMmap,
        }
    }
}

#[pyclass(name = "Indexes")]
#[derive(Clone, Debug, Into)]
pub struct PyIndexes(Indexes);

#[pymethods]
impl PyIndexes {
    #[classattr]
    pub const PLAIN: Self = Self(Indexes::Plain {});

    // TODO: HNSW
}

impl PyIndexes {
    fn _variants(indexes: Indexes) {
        match indexes {
            Indexes::Plain {} => (),
            Indexes::Hnsw(_) => (), // TODO
        }
    }
}

#[pyclass(name = "MultiVectorConfig")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyMultiVectorConfig(MultiVectorConfig);

#[pymethods]
impl PyMultiVectorConfig {
    #[new]
    pub fn new(comparator: PyMultiVectorComparator) -> Self {
        Self(MultiVectorConfig {
            comparator: MultiVectorComparator::from(comparator),
        })
    }
}

#[pyclass(name = "MultiVectorComparator")]
#[derive(Copy, Clone, Debug)]
pub enum PyMultiVectorComparator {
    MaxSim,
}

impl From<MultiVectorComparator> for PyMultiVectorComparator {
    fn from(comparator: MultiVectorComparator) -> Self {
        match comparator {
            MultiVectorComparator::MaxSim => PyMultiVectorComparator::MaxSim,
        }
    }
}

impl From<PyMultiVectorComparator> for MultiVectorComparator {
    fn from(comparator: PyMultiVectorComparator) -> Self {
        match comparator {
            PyMultiVectorComparator::MaxSim => MultiVectorComparator::MaxSim,
        }
    }
}

#[pyclass(name = "VectorStorageDatatype")]
#[derive(Copy, Clone, Debug)]
pub enum PyVectorStorageDatatype {
    Float32,
    Float16,
    Uint8,
}

impl From<VectorStorageDatatype> for PyVectorStorageDatatype {
    fn from(datatype: VectorStorageDatatype) -> Self {
        match datatype {
            VectorStorageDatatype::Float32 => PyVectorStorageDatatype::Float32,
            VectorStorageDatatype::Float16 => PyVectorStorageDatatype::Float16,
            VectorStorageDatatype::Uint8 => PyVectorStorageDatatype::Uint8,
        }
    }
}

impl From<PyVectorStorageDatatype> for VectorStorageDatatype {
    fn from(datatype: PyVectorStorageDatatype) -> Self {
        match datatype {
            PyVectorStorageDatatype::Float32 => VectorStorageDatatype::Float32,
            PyVectorStorageDatatype::Float16 => VectorStorageDatatype::Float16,
            PyVectorStorageDatatype::Uint8 => VectorStorageDatatype::Uint8,
        }
    }
}
