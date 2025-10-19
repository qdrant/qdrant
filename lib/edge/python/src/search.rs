use std::mem;

use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::data_types::vectors::NamedQuery;
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequest;

use crate::*;

#[pyclass(name = "SearchRequest")]
#[derive(Clone, Debug, Into)]
pub struct PySearchRequest(CoreSearchRequest);

#[pymethods]
impl PySearchRequest {
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query: PyQuery,
        filter: Option<PyFilter>,
        params: Option<PySearchParams>,
        limit: usize,
        offset: usize,
        with_vector: Option<PyWithVector>,
        with_payload: Option<PyWithPayload>,
        score_threshold: Option<f32>,
    ) -> Self {
        Self(CoreSearchRequest {
            query: query.into(),
            filter: filter.map(Into::into),
            params: params.map(Into::into),
            limit,
            offset,
            with_vector: with_vector.map(Into::into),
            with_payload: with_payload.map(Into::into),
            score_threshold,
        })
    }
}

#[derive(Clone, Debug, Into)]
pub struct PyQuery(QueryEnum);

impl<'py> FromPyObject<'py> for PyQuery {
    fn extract_bound(query: &Bound<'py, PyAny>) -> PyResult<Self> {
        let query = if let Ok(single) = query.extract() {
            QueryEnum::Nearest(NamedQuery::default_dense(single))
        } else {
            return Err(PyValueError::new_err(format!(
                "failed to convert Python object {query} into query"
            )));
        };

        Ok(Self(query))
    }
}

#[derive(Clone, Debug, Into)]
pub struct PyFilter(Filter);

impl<'py> FromPyObject<'py> for PyFilter {
    fn extract_bound(_filter: &Bound<'py, PyAny>) -> PyResult<Self> {
        todo!()
    }
}

#[pyclass(name = "SearchParams")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PySearchParams(SearchParams);

#[pymethods]
impl PySearchParams {
    #[new]
    pub fn new(
        hnsw_ef: Option<usize>,
        exact: bool,
        quantization: Option<PyQuantizationSearchParams>,
        indexed_only: bool,
    ) -> Self {
        Self(SearchParams {
            hnsw_ef,
            exact,
            quantization: quantization.map(Into::into),
            indexed_only,
        })
    }
}

#[pyclass(name = "QuantizationSearchParams")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyQuantizationSearchParams(QuantizationSearchParams);

#[pymethods]
impl PyQuantizationSearchParams {
    #[new]
    pub fn new(ignore: bool, rescore: Option<bool>, oversampling: Option<f64>) -> Self {
        Self(QuantizationSearchParams {
            ignore,
            rescore,
            oversampling,
        })
    }
}

#[derive(Clone, Debug, Into)]
pub struct PyWithVector(WithVector);

impl<'py> FromPyObject<'py> for PyWithVector {
    fn extract_bound(with_vector: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Bool(bool),
            Selector(Vec<String>),
        }

        let with_vector = match with_vector.extract()? {
            Helper::Bool(bool) => WithVector::Bool(bool),
            Helper::Selector(vectors) => WithVector::Selector(vectors),
        };

        Ok(Self(with_vector))
    }
}

impl<'py> IntoPyObject<'py> for PyWithVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyWithVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match &self.0 {
            WithVector::Bool(bool) => bool.into_bound_py_any(py),
            WithVector::Selector(vectors) => vectors.into_bound_py_any(py),
        }
    }
}

#[derive(Clone, Debug, Into)]
pub struct PyWithPayload(WithPayloadInterface);

impl<'py> FromPyObject<'py> for PyWithPayload {
    fn extract_bound(with_payload: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Bool(bool),
            // TODO: `Fields(Vec<JsonPath>)`!
            // TODO: `Selector(PayloadSelector)`!
        }

        let with_payload = match with_payload.extract()? {
            Helper::Bool(bool) => WithPayloadInterface::Bool(bool),
        };

        Ok(Self(with_payload))
    }
}

impl<'py> IntoPyObject<'py> for PyWithPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyWithPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match &self.0 {
            WithPayloadInterface::Bool(bool) => bool.into_bound_py_any(py),
            WithPayloadInterface::Fields(_fields) => todo!(),
            WithPayloadInterface::Selector(_selector) => todo!(),
        }
    }
}

#[pyclass(name = "ScoredPoint")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyScoredPoint(pub ScoredPoint);

impl PyScoredPoint {
    pub fn from_rust_vec(points: Vec<ScoredPoint>) -> Vec<Self> {
        // `PyScoredPoint` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(points) }
    }
}

#[pymethods]
impl PyScoredPoint {
    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    pub fn version(&self) -> u64 {
        self.0.version
    }

    #[getter]
    pub fn score(&self) -> f32 {
        self.0.score
    }

    #[getter]
    pub fn vector(&self) -> Option<PyVector> {
        self.0.vector.clone().map(PyVector::from)
    }

    #[getter]
    pub fn payload(&self) -> Option<&PyPayload> {
        self.0.payload.as_ref().map(PyPayload::from_ref)
    }
}
