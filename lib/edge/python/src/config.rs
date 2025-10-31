use std::collections::HashMap;

use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

#[pyclass(name = "SegmentConfig")]
#[derive(Clone, Debug, Into)]
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

#[pyclass(name = "VectorDataConfig")]
#[derive(Clone, Debug, Into)]
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
#[derive(Copy, Clone, Debug, Into)]
pub struct PyDistance(Distance);

#[pymethods]
impl PyDistance {
    #[classattr]
    pub const COSINE: Self = Self(Distance::Cosine);

    #[classattr]
    pub const EUCLID: Self = Self(Distance::Euclid);

    #[classattr]
    pub const DOT: Self = Self(Distance::Dot);

    #[classattr]
    pub const MANHATTAN: Self = Self(Distance::Manhattan);
}

impl PyDistance {
    fn _variants(distance: Distance) {
        match distance {
            Distance::Cosine => (),
            Distance::Euclid => (),
            Distance::Dot => (),
            Distance::Manhattan => (),
        }
    }
}

#[pyclass(name = "VectorStorageType")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyVectorStorageType(VectorStorageType);

#[pymethods]
impl PyVectorStorageType {
    #[classattr]
    pub const MEMORY: Self = Self(VectorStorageType::Memory);

    #[classattr]
    pub const MMAP: Self = Self(VectorStorageType::Mmap);

    #[classattr]
    pub const CHUNKED_MMAP: Self = Self(VectorStorageType::ChunkedMmap);

    #[classattr]
    pub const IN_RAM_CHUNKED_MMAP: Self = Self(VectorStorageType::InRamChunkedMmap);
}

impl PyVectorStorageType {
    fn _variants(storage_type: VectorStorageType) {
        match storage_type {
            VectorStorageType::Memory => (),
            VectorStorageType::Mmap => (),
            VectorStorageType::ChunkedMmap => (),
            VectorStorageType::InRamChunkedMmap => (),
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

    // TODO: HNSW!?
}

impl PyIndexes {
    fn _variants(indexes: Indexes) {
        match indexes {
            Indexes::Plain {} => (),
            Indexes::Hnsw(_) => (), // TODO!?
        }
    }
}

#[pyclass(name = "QuantizationConfig")]
#[derive(Clone, Debug, Into)]
pub struct PyQuantizationConfig(QuantizationConfig); // TODO!?

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
#[derive(Copy, Clone, Debug, Into)]
pub struct PyMultiVectorComparator(MultiVectorComparator);

#[pymethods]
impl PyMultiVectorComparator {
    #[classattr]
    pub const MAX_SIM: Self = Self(MultiVectorComparator::MaxSim);
}

impl PyMultiVectorComparator {
    fn _variants(comparator: MultiVectorComparator) {
        match comparator {
            MultiVectorComparator::MaxSim => (),
        }
    }
}

#[pyclass(name = "VectorStorageDatatype")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyVectorStorageDatatype(VectorStorageDatatype);

#[pymethods]
impl PyVectorStorageDatatype {
    #[classattr]
    pub const FLOAT_32: Self = Self(VectorStorageDatatype::Float32);

    #[classattr]
    pub const FLOAT_16: Self = Self(VectorStorageDatatype::Float16);

    #[classattr]
    pub const UINT_8: Self = Self(VectorStorageDatatype::Uint8);
}

impl PyVectorStorageDatatype {
    fn _variants(storage_datatype: VectorStorageDatatype) {
        match storage_datatype {
            VectorStorageDatatype::Float32 => (),
            VectorStorageDatatype::Float16 => (),
            VectorStorageDatatype::Uint8 => (),
        }
    }
}

#[pyclass(name = "SparseVectorDataConfig")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PySparseVectorDataConfig(SparseVectorDataConfig);

#[pymethods]
impl PySparseVectorDataConfig {
    // TODO!?
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
