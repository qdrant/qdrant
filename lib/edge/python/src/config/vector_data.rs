use std::collections::HashMap;
use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::types::*;

use super::quantization::*;
use crate::repr::*;

#[pyclass(name = "Distance", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyDistance {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
}

#[pymethods]
impl PyDistance {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyDistance {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Cosine => "Cosine",
            Self::Euclid => "Euclid",
            Self::Dot => "Dot",
            Self::Manhattan => "Manhattan",
        };

        f.simple_enum::<Self>(repr)
    }
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

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
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

impl Repr for PyIndexes {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Indexes::Plain {} => PyPlainIndexConfig.fmt(f),
            Indexes::Hnsw(hnsw) => PyHnswIndexConfig::wrap_ref(hnsw).fmt(f),
        }
    }
}

#[pyclass(name = "PlainIndexConfig", from_py_object)]
#[derive(Copy, Clone, Debug, Default, Into)]
pub struct PyPlainIndexConfig;

#[pyclass_repr]
#[pymethods]
impl PyPlainIndexConfig {
    #[new]
    pub fn new() -> Self {
        Self
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

#[pyclass(name = "HnswIndexConfig", from_py_object)]
#[derive(Copy, Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyHnswIndexConfig(pub HnswConfig);

#[pyclass_repr]
#[pymethods]
impl PyHnswIndexConfig {
    #[new]
    #[pyo3(signature = (m, ef_construct, full_scan_threshold, max_indexing_threads=0, on_disk=None, payload_m=None, inline_storage=None))]
    pub fn new(
        m: usize,
        ef_construct: usize,
        full_scan_threshold: usize,
        max_indexing_threads: usize,
        on_disk: Option<bool>,
        payload_m: Option<usize>,
        inline_storage: Option<bool>,
    ) -> Self {
        Self(HnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
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
    pub fn max_indexing_threads(&self) -> usize {
        self.0.max_indexing_threads
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

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyHnswIndexConfig {
    fn _getters(self) {
        // Every field should have a getter method
        let HnswConfig {
            m: _,
            ef_construct: _,
            full_scan_threshold: _,
            max_indexing_threads: _, // not relevant for Qdrant Edge
            on_disk: _,
            payload_m: _,
            inline_storage: _,
        } = self.0;
    }
}

#[pyclass(name = "MultiVectorConfig", from_py_object)]
#[derive(Copy, Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMultiVectorConfig(MultiVectorConfig);

#[pyclass_repr]
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

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMultiVectorConfig {
    fn _getters(self) {
        // Every field should have a getter method
        let MultiVectorConfig { comparator: _ } = self.0;
    }
}

#[pyclass(name = "MultiVectorComparator", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyMultiVectorComparator {
    MaxSim,
}

#[pymethods]
impl PyMultiVectorComparator {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyMultiVectorComparator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::MaxSim => "MaxSim",
        };

        f.simple_enum::<Self>(repr)
    }
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

#[pyclass(name = "VectorStorageDatatype", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyVectorStorageDatatype {
    Float32,
    Float16,
    Uint8,
}

#[pymethods]
impl PyVectorStorageDatatype {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyVectorStorageDatatype {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Float32 => "Float32",
            Self::Float16 => "Float16",
            Self::Uint8 => "Uint8",
        };

        f.simple_enum::<Self>(repr)
    }
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

// --- EdgeVectorParams (user-facing config for EdgeConfig) ---

use edge::EdgeVectorParams;

#[pyclass(name = "EdgeVectorParams", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyEdgeVectorParams(pub EdgeVectorParams);

impl PyEdgeVectorParams {
    pub fn peel_map(map: HashMap<String, Self>) -> HashMap<String, EdgeVectorParams> {
        map.into_iter().map(|(k, v)| (k, v.0)).collect()
    }

    pub fn wrap_map(
        map: &HashMap<String, EdgeVectorParams>,
    ) -> HashMap<String, PyEdgeVectorParams> {
        map.iter()
            .map(|(k, v)| (k.clone(), PyEdgeVectorParams(v.clone())))
            .collect()
    }
}

#[pyclass_repr]
#[pymethods]
impl PyEdgeVectorParams {
    #[new]
    #[pyo3(signature = (size, distance, on_disk=None, multivector_config=None, datatype=None, quantization_config=None, hnsw_config=None))]
    pub fn new(
        size: usize,
        distance: PyDistance,
        on_disk: Option<bool>,
        multivector_config: Option<PyMultiVectorConfig>,
        datatype: Option<PyVectorStorageDatatype>,
        quantization_config: Option<PyQuantizationConfig>,
        hnsw_config: Option<PyHnswIndexConfig>,
    ) -> Self {
        Self(EdgeVectorParams {
            size,
            distance: Distance::from(distance),
            on_disk,
            multivector_config: multivector_config.map(MultiVectorConfig::from),
            datatype: datatype.map(VectorStorageDatatype::from),
            quantization_config: quantization_config.map(QuantizationConfig::from),
            hnsw_config: hnsw_config.map(|h| h.0),
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
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn multivector_config(&self) -> Option<PyMultiVectorConfig> {
        self.0.multivector_config.map(PyMultiVectorConfig)
    }

    #[getter]
    pub fn datatype(&self) -> Option<PyVectorStorageDatatype> {
        self.0.datatype.map(PyVectorStorageDatatype::from)
    }

    #[getter]
    pub fn quantization_config(&self) -> Option<PyQuantizationConfig> {
        self.0.quantization_config.clone().map(PyQuantizationConfig)
    }

    #[getter]
    pub fn hnsw_config(&self) -> Option<PyHnswIndexConfig> {
        self.0.hnsw_config.map(PyHnswIndexConfig)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl<'py> IntoPyObject<'py> for &PyEdgeVectorParams {
    type Target = PyEdgeVectorParams;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}
