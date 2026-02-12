use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::data_types::facets::{FacetResponse, FacetValue, FacetValueHit};
use segment::types::Filter;
use shard::facet::FacetRequestInternal;

use crate::repr::*;
use crate::types::{PyFilter, PyJsonPath};

#[pyclass(name = "FacetRequest", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyFacetRequest(FacetRequestInternal);

#[pyclass_repr]
#[pymethods]
impl PyFacetRequest {
    #[new]
    #[pyo3(signature = (key, limit = 10, exact = false, filter = None))]
    pub fn new(key: PyJsonPath, limit: usize, exact: bool, filter: Option<PyFilter>) -> Self {
        Self(FacetRequestInternal {
            key: key.into(),
            limit,
            filter: filter.map(Filter::from),
            exact,
        })
    }

    #[getter]
    pub fn key(&self) -> PyJsonPath {
        PyJsonPath(self.0.key.clone())
    }

    #[getter]
    pub fn limit(&self) -> usize {
        self.0.limit
    }

    #[getter]
    pub fn exact(&self) -> bool {
        self.0.exact
    }

    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }
}

#[pyclass(name = "FacetHit", from_py_object)]
#[derive(Clone, Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFacetHit(FacetValueHit);

#[pymethods]
impl PyFacetHit {
    #[getter]
    pub fn value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        facet_value_into_py(&self.0.value, py)
    }

    #[getter]
    pub fn count(&self) -> usize {
        self.0.count
    }

    pub fn __repr__(&self) -> String {
        format!("FacetHit(value={:?}, count={})", self.0.value, self.0.count)
    }
}

impl Repr for PyFacetHit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FacetHit(value={:?}, count={})",
            self.0.value, self.0.count
        )
    }
}

#[pyclass(name = "FacetResponse", from_py_object)]
#[derive(Clone, Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFacetResponse(FacetResponse);

impl PyFacetResponse {
    pub fn new(response: FacetResponse) -> Self {
        Self(response)
    }
}

#[pymethods]
impl PyFacetResponse {
    #[getter]
    pub fn hits(&self) -> Vec<PyFacetHit> {
        PyFacetHit::wrap_vec(self.0.hits.clone())
    }

    fn __len__(&self) -> usize {
        self.0.hits.len()
    }

    fn __iter__(&self) -> PyFacetHitIter {
        PyFacetHitIter {
            inner: self.0.hits.clone().into_iter(),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("FacetResponse(hits={})", self.0.hits.len())
    }
}

impl Repr for PyFacetResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FacetResponse(hits={})", self.0.hits.len())
    }
}

#[pyclass]
pub struct PyFacetHitIter {
    inner: std::vec::IntoIter<FacetValueHit>,
}

#[pymethods]
impl PyFacetHitIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyFacetHit> {
        slf.inner.next().map(PyFacetHit)
    }
}

fn facet_value_into_py<'py>(value: &FacetValue, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    match value {
        FacetValue::Keyword(s) => s.into_bound_py_any(py),
        FacetValue::Int(i) => i.into_bound_py_any(py),
        FacetValue::Uuid(uuid) => uuid::Uuid::from_u128(*uuid)
            .to_string()
            .into_bound_py_any(py),
        FacetValue::Bool(b) => b.into_bound_py_any(py),
    }
}
