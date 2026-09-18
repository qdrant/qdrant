// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::HashMap;
use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use pyo3::{IntoPyObjectExt as _, PyTypeInfo};
use segment::types::*;

use super::quantization::*;
use crate::repr::*;
use crate::type_hint::Alias;

/// Distance metrics for vector comparison.
#[pyclass(name = "Distance", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyDistance {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
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

pub const INDEXES: Alias = Alias {
    name: "IndexType",
    definition: IndexesHelper::INPUT_TYPE,
};

#[derive(FromPyObject, IntoPyObject)]
enum IndexesHelper {
    Plain(PyPlainIndexConfig),
    Hnsw(PyHnswIndexConfig),
}

impl FromPyObject<'_, '_> for PyIndexes {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = INDEXES.hint();

    fn extract(indexes: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(indexes: Indexes) {
            match indexes {
                Indexes::Plain {} => (),
                Indexes::Hnsw(_) => (),
            }
        }

        let indexes = match indexes.extract()? {
            IndexesHelper::Plain(_) => Indexes::Plain {},
            IndexesHelper::Hnsw(hnsw) => Indexes::Hnsw(HnswConfig::from(hnsw)),
        };

        Ok(Self(indexes))
    }
}

impl<'py> IntoPyObject<'py> for PyIndexes {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = INDEXES.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Indexes::Plain {} => IndexesHelper::Plain(PyPlainIndexConfig),
            Indexes::Hnsw(hnsw) => IndexesHelper::Hnsw(PyHnswIndexConfig(hnsw)),
        }
        .into_bound_py_any(py)
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

/// Configuration for plain (brute-force) index.
///
/// Create a PlainIndexConfig.
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

/// Configuration for HNSW index.
///
/// Create an HnswIndexConfig.
///
/// Args:
///     m: Number of edges per node.
///     ef_construct: Number of candidates during index construction.
///     full_scan_threshold: Threshold for full scan.
///     max_indexing_threads: Max threads for HNSW indexing (0 = auto).
///     on_disk: Whether to store on disk.
///     payload_m: Payload index m value.
///     inline_storage: Whether to use inline storage.
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
            memory: None,
            payload_m,
            inline_storage,
        })
    }

    /// Number of edges per node.
    #[getter]
    pub fn m(&self) -> usize {
        self.0.m
    }

    /// ef_construct value.
    #[getter]
    pub fn ef_construct(&self) -> usize {
        self.0.ef_construct
    }

    /// Full scan threshold.
    #[getter]
    pub fn full_scan_threshold(&self) -> usize {
        self.0.full_scan_threshold
    }

    /// Max indexing threads (0 = auto).
    #[getter]
    pub fn max_indexing_threads(&self) -> usize {
        self.0.max_indexing_threads
    }

    /// On-disk flag.
    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    /// Payload m value.
    #[getter]
    pub fn payload_m(&self) -> Option<usize> {
        self.0.payload_m
    }

    /// Inline storage flag.
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
            memory: _,
            payload_m: _,
            inline_storage: _,
        } = self.0;
    }
}

/// Configuration for multi-vector storage.
///
/// Create a MultiVectorConfig.
///
/// Args:
///     comparator: Multi-vector comparator.
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

    /// Comparator.
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

/// Multi-vector comparison methods.
#[pyclass(name = "MultiVectorComparator", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyMultiVectorComparator {
    MaxSim,
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

/// Vector storage data types.
#[pyclass(name = "VectorStorageDatatype", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyVectorStorageDatatype {
    Float32,
    Float16,
    Uint8,
    Turbo4,
}

impl Repr for PyVectorStorageDatatype {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Float32 => "Float32",
            Self::Float16 => "Float16",
            Self::Uint8 => "Uint8",
            Self::Turbo4 => "Turbo4",
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
            VectorStorageDatatype::Turbo4 => PyVectorStorageDatatype::Turbo4,
        }
    }
}

impl From<PyVectorStorageDatatype> for VectorStorageDatatype {
    fn from(datatype: PyVectorStorageDatatype) -> Self {
        match datatype {
            PyVectorStorageDatatype::Float32 => VectorStorageDatatype::Float32,
            PyVectorStorageDatatype::Float16 => VectorStorageDatatype::Float16,
            PyVectorStorageDatatype::Uint8 => VectorStorageDatatype::Uint8,
            PyVectorStorageDatatype::Turbo4 => VectorStorageDatatype::Turbo4,
        }
    }
}

// --- EdgeVectorParams (user-facing config for EdgeConfig) ---

use edge::EdgeVectorParams;

/// Dense vector parameters for EdgeConfig.
///
/// Create EdgeVectorParams.
///
/// Args:
///     size: Dimension of vectors.
///     distance: Distance metric.
///     on_disk: If True, store vectors on disk (mmap); otherwise in RAM.
///     multivector_config: Optional multi-vector configuration.
///     datatype: Optional storage datatype.
///     quantization_config: Optional per-vector quantization override.
///     hnsw_config: Optional per-vector HNSW config override.
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

    /// Vector dimension.
    #[getter]
    pub fn size(&self) -> usize {
        self.0.size
    }

    /// Distance metric.
    #[getter]
    pub fn distance(&self) -> PyDistance {
        PyDistance::from(self.0.distance)
    }

    /// Whether vector storage is on disk.
    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    /// Multi-vector configuration.
    #[getter]
    pub fn multivector_config(&self) -> Option<PyMultiVectorConfig> {
        self.0.multivector_config.map(PyMultiVectorConfig)
    }

    /// Storage datatype.
    #[getter]
    pub fn datatype(&self) -> Option<PyVectorStorageDatatype> {
        self.0.datatype.map(PyVectorStorageDatatype::from)
    }

    /// Quantization configuration.
    #[getter]
    pub fn quantization_config(&self) -> Option<PyQuantizationConfig> {
        self.0.quantization_config.clone().map(PyQuantizationConfig)
    }

    /// HNSW config override.
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
    const OUTPUT_TYPE: PyStaticExpr = PyEdgeVectorParams::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}
