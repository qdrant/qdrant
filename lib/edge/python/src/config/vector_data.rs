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

/// Memory placement of a component (vectors, HNSW graph, indexes, …).
///
/// Data is always persisted on disk regardless of this setting; it only
/// controls how the data is held in RAM. Prefer this over the deprecated
/// `on_disk` / `always_ram` / `on_disk_payload` flags.
#[pyclass(name = "Memory", from_py_object, eq, eq_int)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PyMemory {
    /// Not pre-loaded from disk; paged in on demand.
    Cold,
    /// Pre-loaded into the page cache on open; evictable under pressure.
    Cached,
    /// Loaded onto the heap and never evicted by cache pressure.
    Pinned,
}

#[pymethods]
impl PyMemory {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Cold => "Cold",
            Self::Cached => "Cached",
            Self::Pinned => "Pinned",
        };
        f.simple_enum::<Self>(repr)
    }
}

impl From<Memory> for PyMemory {
    fn from(memory: Memory) -> Self {
        match memory {
            Memory::Cold => PyMemory::Cold,
            Memory::Cached => PyMemory::Cached,
            Memory::Pinned => PyMemory::Pinned,
        }
    }
}

impl From<PyMemory> for Memory {
    fn from(memory: PyMemory) -> Self {
        match memory {
            PyMemory::Cold => Memory::Cold,
            PyMemory::Cached => Memory::Cached,
            PyMemory::Pinned => Memory::Pinned,
        }
    }
}

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
/// Args:
///     m: Number of edges per node.
///     ef_construct: Number of candidates during index construction.
///     full_scan_threshold: Threshold for full scan.
///     max_indexing_threads: Max threads for HNSW indexing (0 = auto).
///     on_disk: Deprecated, use memory instead. Whether to store on disk.
///     payload_m: Payload index m value.
///     inline_storage: Whether to use inline storage.
///     memory: Memory placement of the HNSW graph. Overrides the deprecated on_disk
///             flag if both are set.
#[pyclass(name = "HnswIndexConfig", from_py_object)]
#[derive(Copy, Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyHnswIndexConfig(pub HnswConfig);

#[pyclass_repr]
#[pymethods]
impl PyHnswIndexConfig {
    #[new]
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (m, ef_construct, full_scan_threshold, max_indexing_threads=0, on_disk=None, payload_m=None, inline_storage=None, memory=None))]
    pub fn new(
        m: usize,
        ef_construct: usize,
        full_scan_threshold: usize,
        max_indexing_threads: usize,
        on_disk: Option<bool>,
        payload_m: Option<usize>,
        inline_storage: Option<bool>,
        memory: Option<PyMemory>,
    ) -> Self {
        Self(HnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            memory: memory.map(Memory::from),
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
        #[allow(deprecated)]
        {
            self.0.on_disk
        }
    }

    /// Memory placement of the HNSW graph, or None if not specified.
    #[getter]
    pub fn memory(&self) -> Option<PyMemory> {
        self.0.memory.map(PyMemory::from)
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
    Turbo8,
    Turbo16,
}

impl Repr for PyVectorStorageDatatype {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Float32 => "Float32",
            Self::Float16 => "Float16",
            Self::Uint8 => "Uint8",
            Self::Turbo4 => "Turbo4",
            Self::Turbo8 => "Turbo8",
            Self::Turbo16 => "Turbo16",
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
            VectorStorageDatatype::Turbo8 => PyVectorStorageDatatype::Turbo8,
            VectorStorageDatatype::Turbo16 => PyVectorStorageDatatype::Turbo16,
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
            PyVectorStorageDatatype::Turbo8 => VectorStorageDatatype::Turbo8,
            PyVectorStorageDatatype::Turbo16 => VectorStorageDatatype::Turbo16,
        }
    }
}

// --- EdgeVectorParams (user-facing config for EdgeConfig) ---

use edge::EdgeVectorParams;

/// Dense vector parameters for EdgeConfig.
///
/// Args:
///     size: Dimension of vectors.
///     distance: Distance metric.
///     on_disk: Deprecated, use memory instead. If True, store vectors on disk
///              (mmap); otherwise in RAM.
///     multivector_config: Optional multi-vector configuration.
///     datatype: Optional storage datatype.
///     quantization_config: Optional per-vector quantization override.
///     hnsw_config: Optional per-vector HNSW config override.
///     memory: Memory placement of the original vector storage. Overrides the
///             deprecated on_disk flag if both are set.
///             Pinned is not supported (treated as Cached). Defaults to Cached.
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
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (size, distance, on_disk=None, multivector_config=None, datatype=None, quantization_config=None, hnsw_config=None, memory=None))]
    pub fn new(
        size: usize,
        distance: PyDistance,
        on_disk: Option<bool>,
        multivector_config: Option<PyMultiVectorConfig>,
        datatype: Option<PyVectorStorageDatatype>,
        quantization_config: Option<PyQuantizationConfig>,
        hnsw_config: Option<PyHnswIndexConfig>,
        memory: Option<PyMemory>,
    ) -> Self {
        #[allow(deprecated)]
        Self(EdgeVectorParams {
            size,
            distance: Distance::from(distance),
            on_disk,
            memory: memory.map(Memory::from),
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
        #[allow(deprecated)]
        {
            self.0.on_disk
        }
    }

    /// Memory placement of the original vector storage, or None if not specified.
    #[getter]
    pub fn memory(&self) -> Option<PyMemory> {
        self.0.memory.map(PyMemory::from)
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
