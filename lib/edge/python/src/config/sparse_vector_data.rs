use std::collections::HashMap;
use std::fmt;

use edge::EdgeSparseVectorParams;
use pyo3::PyTypeInfo;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use segment::data_types::modifier::Modifier;
use segment::types::VectorStorageDatatype;

use super::vector_data::*;
use crate::repr::*;

/// Sparse vector parameters for EdgeConfig.
#[pyclass(name = "EdgeSparseVectorParams", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyEdgeSparseVectorParams(pub EdgeSparseVectorParams);

impl PyEdgeSparseVectorParams {
    pub fn peel_map(map: HashMap<String, Self>) -> HashMap<String, EdgeSparseVectorParams> {
        map.into_iter().map(|(k, v)| (k, v.0)).collect()
    }

    pub fn wrap_map(
        map: &HashMap<String, EdgeSparseVectorParams>,
    ) -> HashMap<String, PyEdgeSparseVectorParams> {
        map.iter()
            .map(|(k, v)| (k.clone(), PyEdgeSparseVectorParams(v.clone())))
            .collect()
    }
}

#[pyclass_repr]
#[pymethods]
impl PyEdgeSparseVectorParams {
    /// Create EdgeSparseVectorParams.
    ///
    /// Args:
    ///     full_scan_threshold: Threshold for full scan vs index search.
    ///     on_disk: If True, sparse index on disk; otherwise in RAM.
    ///     modifier: Optional modifier (e.g., IDF).
    ///     datatype: Storage datatype.
    #[new]
    #[pyo3(signature = (full_scan_threshold=None, on_disk=None, modifier=None, datatype=None))]
    pub fn new(
        full_scan_threshold: Option<usize>,
        on_disk: Option<bool>,
        modifier: Option<PyModifier>,
        datatype: Option<PyVectorStorageDatatype>,
    ) -> Self {
        Self(EdgeSparseVectorParams {
            full_scan_threshold,
            on_disk,
            modifier: modifier.map(Modifier::from),
            datatype: datatype.map(VectorStorageDatatype::from),
        })
    }

    /// Full scan threshold.
    #[getter]
    pub fn full_scan_threshold(&self) -> Option<usize> {
        self.0.full_scan_threshold
    }

    /// Whether sparse index is on disk.
    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    /// Modifier.
    #[getter]
    pub fn modifier(&self) -> Option<PyModifier> {
        self.0.modifier.map(PyModifier::from)
    }

    /// Storage datatype.
    #[getter]
    pub fn datatype(&self) -> Option<PyVectorStorageDatatype> {
        self.0.datatype.map(PyVectorStorageDatatype::from)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl<'py> IntoPyObject<'py> for &PyEdgeSparseVectorParams {
    type Target = PyEdgeSparseVectorParams;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = PyEdgeSparseVectorParams::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

/// Sparse vector modifiers.
#[pyclass(name = "Modifier", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyModifier {
    #[pyo3(name = "None_")]
    None,
    Idf,
}

impl Repr for PyModifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::None => "None_",
            Self::Idf => "Idf",
        };

        f.simple_enum::<Self>(repr)
    }
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
