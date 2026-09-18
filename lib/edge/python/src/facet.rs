use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use edge::FacetRequest;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use pyo3::{PyTypeInfo, type_hint_identifier, type_hint_subscript};
use segment::data_types::facets::{FacetResponse, FacetValue, FacetValueHit};
use segment::types::{Filter, ValueVariants};

use crate::repr::*;
use crate::types::{PyFilter, PyJsonPath, PyValueVariants};

/// Request for facet operation.
///
/// Create a FacetRequest.
///
/// Args:
///     key: Payload field key to facet on.
///     limit: Maximum number of facet hits to return.
///     exact: Whether to count exactly or estimate.
///     filter: Filter conditions.
#[pyclass(name = "FacetRequest", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyFacetRequest(FacetRequest);

#[pyclass_repr]
#[pymethods]
impl PyFacetRequest {
    #[new]
    #[pyo3(signature = (key, limit = 10, exact = false, filter = None))]
    pub fn new(key: PyJsonPath, limit: usize, exact: bool, filter: Option<PyFilter>) -> Self {
        Self(FacetRequest {
            key: key.into(),
            limit,
            filter: filter.map(Filter::from),
            exact,
        })
    }

    /// Facet key.
    #[getter]
    pub fn key(&self) -> PyJsonPath {
        PyJsonPath(self.0.key.clone())
    }

    /// Result limit.
    #[getter]
    pub fn limit(&self) -> usize {
        self.0.limit
    }

    /// Exact count flag.
    #[getter]
    pub fn exact(&self) -> bool {
        self.0.exact
    }

    /// Filter.
    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }
}

/// A facet hit with value and count.
#[pyclass(name = "FacetHit", from_py_object)]
#[derive(Clone, Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFacetHit(FacetValueHit);

#[pymethods]
impl PyFacetHit {
    /// Facet value.
    #[getter]
    pub fn value(&self) -> PyValueVariants {
        PyValueVariants::wrap(match &self.0.value {
            FacetValue::Keyword(str) => ValueVariants::String(str.clone()),
            &FacetValue::Int(int) => ValueVariants::Integer(int),
            &FacetValue::Uuid(uuid) => {
                ValueVariants::String(uuid::Uuid::from_u128(uuid).to_string())
            }
            &FacetValue::Bool(bool) => ValueVariants::Bool(bool),
        })
    }

    /// Count of points with this value.
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

/// Response for facet operation.
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
    /// Facet hits.
    #[getter]
    pub fn hits(&self) -> Vec<PyFacetHit> {
        PyFacetHit::wrap_vec(self.0.hits.clone())
    }

    /// Number of hits.
    fn __len__(&self) -> usize {
        self.0.hits.len()
    }

    /// Iterate over hits.
    fn __iter__(&self) -> FacetHitIter {
        FacetHitIter(PyFacetHitIter {
            inner: self.0.hits.clone().into_iter(),
        })
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

/// `PyFacetHitIter`, typed as `Iterator[FacetHit]`.
struct FacetHitIter(PyFacetHitIter);

impl<'py> IntoPyObject<'py> for FacetHitIter {
    type Target = PyFacetHitIter;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = type_hint_subscript!(
        type_hint_identifier!("collections.abc", "Iterator"),
        PyFacetHit::TYPE_HINT
    );

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        Bound::new(py, self.0)
    }
}
