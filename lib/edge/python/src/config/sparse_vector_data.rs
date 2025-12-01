use std::collections::HashMap;
use std::mem;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::types::*;

use super::vector_data::*;

#[pyclass(name = "SparseVectorDataConfig")]
#[derive(Copy, Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySparseVectorDataConfig(pub SparseVectorDataConfig);

impl PySparseVectorDataConfig {
    pub fn peel_map(map: HashMap<String, Self>) -> HashMap<String, SparseVectorDataConfig>
    where
        Self: TransparentWrapper<SparseVectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }

    pub fn wrap_map_ref(map: &HashMap<String, SparseVectorDataConfig>) -> &HashMap<String, Self>
    where
        Self: TransparentWrapper<SparseVectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }
}

#[pymethods]
impl PySparseVectorDataConfig {
    #[new]
    pub fn new(
        index: PySparseIndexConfig,
        storage_type: PySparseVectorStorageType,
        modifier: Option<PyModifier>,
    ) -> Self {
        Self(SparseVectorDataConfig {
            index: SparseIndexConfig::from(index),
            storage_type: SparseVectorStorageType::from(storage_type),
            modifier: modifier.map(Modifier::from),
        })
    }

    #[getter]
    pub fn index(&self) -> PySparseIndexConfig {
        PySparseIndexConfig(self.0.index)
    }

    #[getter]
    pub fn storage_type(&self) -> PySparseVectorStorageType {
        PySparseVectorStorageType::from(self.0.storage_type)
    }

    #[getter]
    pub fn modifier(&self) -> Option<PyModifier> {
        self.0.modifier.map(PyModifier::from)
    }
}

impl<'py> IntoPyObject<'py> for &PySparseVectorDataConfig {
    type Target = PySparseVectorDataConfig;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(*self, py)
    }
}

#[pyclass(name = "SparseIndexConfig")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PySparseIndexConfig(SparseIndexConfig);

#[pymethods]
impl PySparseIndexConfig {
    #[new]
    pub fn new(
        full_scan_threshold: Option<usize>,
        index_type: PySparseIndexType,
        datatype: Option<PyVectorStorageDatatype>,
    ) -> Self {
        Self(SparseIndexConfig {
            full_scan_threshold,
            index_type: SparseIndexType::from(index_type),
            datatype: datatype.map(VectorStorageDatatype::from),
        })
    }

    #[getter]
    pub fn full_scan_threshold(&self) -> Option<usize> {
        self.0.full_scan_threshold
    }

    #[getter]
    pub fn index_type(&self) -> PySparseIndexType {
        PySparseIndexType::from(self.0.index_type)
    }

    #[getter]
    pub fn datatype(&self) -> Option<PyVectorStorageDatatype> {
        self.0.datatype.map(PyVectorStorageDatatype::from)
    }
}

#[pyclass(name = "SparseIndexType")]
#[derive(Copy, Clone, Debug)]
pub enum PySparseIndexType {
    MutableRam,
    ImmutableRam,
    Mmap,
}

impl From<SparseIndexType> for PySparseIndexType {
    fn from(index_type: SparseIndexType) -> Self {
        match index_type {
            SparseIndexType::MutableRam => PySparseIndexType::MutableRam,
            SparseIndexType::ImmutableRam => PySparseIndexType::ImmutableRam,
            SparseIndexType::Mmap => PySparseIndexType::Mmap,
        }
    }
}

impl From<PySparseIndexType> for SparseIndexType {
    fn from(index_type: PySparseIndexType) -> Self {
        match index_type {
            PySparseIndexType::MutableRam => SparseIndexType::MutableRam,
            PySparseIndexType::ImmutableRam => SparseIndexType::ImmutableRam,
            PySparseIndexType::Mmap => SparseIndexType::Mmap,
        }
    }
}

#[pyclass(name = "SparseVectorStorageType")]
#[derive(Copy, Clone, Debug)]
pub enum PySparseVectorStorageType {
    Mmap,
}

impl From<SparseVectorStorageType> for PySparseVectorStorageType {
    fn from(storage_type: SparseVectorStorageType) -> Self {
        #[allow(unreachable_patterns)]
        #[allow(clippy::match_wildcard_for_single_variants)]
        match storage_type {
            SparseVectorStorageType::Mmap => PySparseVectorStorageType::Mmap,
            _ => unimplemented!("RocksDB-backed storage types are not supported by Qdrant Edge"),
        }
    }
}

impl From<PySparseVectorStorageType> for SparseVectorStorageType {
    fn from(storage_type: PySparseVectorStorageType) -> Self {
        match storage_type {
            PySparseVectorStorageType::Mmap => SparseVectorStorageType::Mmap,
        }
    }
}

#[pyclass(name = "Modifier")]
#[derive(Copy, Clone, Debug)]
pub enum PyModifier {
    None,
    Idf,
}

impl From<Modifier> for PyModifier {
    fn from(modifier: Modifier) -> Self {
        match modifier {
            Modifier::None => PyModifier::None,
            Modifier::Idf => PyModifier::Idf,
        }
    }
}

impl From<PyModifier> for Modifier {
    fn from(modifier: PyModifier) -> Self {
        match modifier {
            PyModifier::None => Modifier::None,
            PyModifier::Idf => Modifier::Idf,
        }
    }
}
