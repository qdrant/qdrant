use std::collections::HashMap;
use std::mem;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::types::*;

use super::quantization::*;

#[pyclass(name = "VectorDataConfig")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyVectorDataConfig(pub VectorDataConfig);

impl PyVectorDataConfig {
    pub fn peel_map(map: HashMap<String, Self>) -> HashMap<String, VectorDataConfig>
    where
        Self: TransparentWrapper<VectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }

    pub fn wrap_map_ref(map: &HashMap<String, VectorDataConfig>) -> &HashMap<String, Self>
    where
        Self: TransparentWrapper<VectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }
}

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

    #[getter]
    pub fn size(&self) -> usize {
        self.0.size
    }

    #[getter]
    pub fn distance(&self) -> PyDistance {
        PyDistance::from(self.0.distance)
    }

    #[getter]
    pub fn storage_type(&self) -> PyVectorStorageType {
        PyVectorStorageType::from(self.0.storage_type)
    }

    #[getter]
    pub fn index(&self) -> PyIndexes {
        PyIndexes(self.0.index.clone())
    }

    #[getter]
    pub fn quantization_config(&self) -> Option<PyQuantizationConfig> {
        self.0.quantization_config.clone().map(PyQuantizationConfig)
    }

    #[getter]
    pub fn multivector_config(&self) -> Option<PyMultiVectorConfig> {
        self.0.multivector_config.map(PyMultiVectorConfig)
    }

    #[getter]
    pub fn datatype(&self) -> Option<PyVectorStorageDatatype> {
        self.0.datatype.map(PyVectorStorageDatatype::from)
    }
}

impl<'py> IntoPyObject<'py> for &PyVectorDataConfig {
    type Target = PyVectorDataConfig;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        Bound::new(py, self.clone())
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

#[derive(Clone, Debug, Into)]
pub struct PyIndexes(Indexes);

impl FromPyObject<'_, '_> for PyIndexes {
    type Error = PyErr;

    fn extract(indexes: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Plain(PyPlainIndexConfig),
            Hnsw(PyHnswIndexConfig),
        }

        fn _variants(indexes: Indexes) {
            match indexes {
                Indexes::Plain {} => (),
                Indexes::Hnsw(_) => (),
            }
        }

        let indexes = match indexes.extract()? {
            Helper::Plain(_) => Indexes::Plain {},
            Helper::Hnsw(hnsw) => Indexes::Hnsw(HnswConfig::from(hnsw)),
        };

        Ok(Self(indexes))
    }
}

impl<'py> IntoPyObject<'py> for PyIndexes {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Indexes::Plain {} => PyPlainIndexConfig.into_bound_py_any(py),
            Indexes::Hnsw(hnsw) => PyHnswIndexConfig(hnsw).into_bound_py_any(py),
        }
    }
}

#[pyclass(name = "PlainIndexConfig")]
#[derive(Copy, Clone, Debug, Default, Into)]
pub struct PyPlainIndexConfig;

#[pymethods]
impl PyPlainIndexConfig {
    #[new]
    pub fn new() -> Self {
        Self
    }
}

#[pyclass(name = "HnswIndexConfig")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyHnswIndexConfig(HnswConfig);

#[pymethods]
impl PyHnswIndexConfig {
    #[new]
    #[pyo3(signature = (m, ef_construct, full_scan_threshold, on_disk=None, payload_m=None, inline_storage=None))]
    pub fn new(
        m: usize,
        ef_construct: usize,
        full_scan_threshold: usize,
        on_disk: Option<bool>,
        payload_m: Option<usize>,
        inline_storage: Option<bool>,
    ) -> Self {
        Self(HnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads: 0,
            on_disk,
            payload_m,
            inline_storage,
        })
    }

    #[getter]
    pub fn m(&self) -> usize {
        self.0.m
    }

    #[getter]
    pub fn ef_construct(&self) -> usize {
        self.0.ef_construct
    }

    #[getter]
    pub fn full_scan_threshold(&self) -> usize {
        self.0.full_scan_threshold
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn payload_m(&self) -> Option<usize> {
        self.0.payload_m
    }

    #[getter]
    pub fn inline_storage(&self) -> Option<bool> {
        self.0.inline_storage
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

    #[getter]
    pub fn comparator(&self) -> PyMultiVectorComparator {
        PyMultiVectorComparator::from(self.0.comparator)
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
