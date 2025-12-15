use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequest;

use crate::repr::*;
use crate::*;

#[pyclass(name = "SearchRequest")]
#[derive(Clone, Debug, Into)]
pub struct PySearchRequest(CoreSearchRequest);

#[pyclass_repr]
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
            query: QueryEnum::from(query),
            filter: filter.map(Filter::from),
            params: params.map(SearchParams::from),
            limit,
            offset,
            with_vector: with_vector.map(WithVector::from),
            with_payload: with_payload.map(WithPayloadInterface::from),
            score_threshold,
        })
    }

    #[getter]
    pub fn query(&self) -> &PyQuery {
        PyQuery::wrap_ref(&self.0.query)
    }

    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }

    #[getter]
    pub fn params(&self) -> Option<PySearchParams> {
        self.0.params.map(PySearchParams)
    }

    #[getter]
    pub fn limit(&self) -> usize {
        self.0.limit
    }

    #[getter]
    pub fn offset(&self) -> usize {
        self.0.offset
    }

    #[getter]
    pub fn with_vector(&self) -> Option<&PyWithVector> {
        self.0.with_vector.as_ref().map(PyWithVector::wrap_ref)
    }

    #[getter]
    pub fn with_payload(&self) -> Option<&PyWithPayload> {
        self.0.with_payload.as_ref().map(PyWithPayload::wrap_ref)
    }

    #[getter]
    pub fn score_threshold(&self) -> Option<f32> {
        self.0.score_threshold
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PySearchRequest {
    fn _getters(self) {
        // Every field should have a getter method
        let CoreSearchRequest {
            query: _,
            filter: _,
            params: _,
            limit: _,
            offset: _,
            with_vector: _,
            with_payload: _,
            score_threshold: _,
        } = self.0;
    }
}

#[pyclass(name = "SearchParams")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PySearchParams(pub SearchParams);

#[pyclass_repr]
#[pymethods]
impl PySearchParams {
    #[new]
    pub fn new(
        hnsw_ef: Option<usize>,
        exact: bool,
        quantization: Option<PyQuantizationSearchParams>,
        indexed_only: bool,
        acorn: Option<PyAcornSearchParams>,
    ) -> Self {
        Self(SearchParams {
            hnsw_ef,
            exact,
            quantization: quantization.map(QuantizationSearchParams::from),
            indexed_only,
            acorn: acorn.map(AcornSearchParams::from),
        })
    }

    #[getter]
    pub fn hnsw_ef(&self) -> Option<usize> {
        self.0.hnsw_ef
    }

    #[getter]
    pub fn exact(&self) -> bool {
        self.0.exact
    }

    #[getter]
    pub fn quantization(&self) -> Option<PyQuantizationSearchParams> {
        self.0.quantization.map(PyQuantizationSearchParams)
    }

    #[getter]
    pub fn indexed_only(&self) -> bool {
        self.0.indexed_only
    }

    #[getter]
    pub fn acorn(&self) -> Option<PyAcornSearchParams> {
        self.0.acorn.map(PyAcornSearchParams)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PySearchParams {
    fn _getters(self) {
        // Every field should have a getter method
        let SearchParams {
            hnsw_ef: _,
            exact: _,
            quantization: _,
            indexed_only: _,
            acorn: _,
        } = self.0;
    }
}

#[pyclass(name = "QuantizationSearchParams")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyQuantizationSearchParams(QuantizationSearchParams);

#[pyclass_repr]
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

    #[getter]
    pub fn ignore(&self) -> bool {
        self.0.ignore
    }

    #[getter]
    pub fn rescore(&self) -> Option<bool> {
        self.0.rescore
    }

    #[getter]
    pub fn oversampling(&self) -> Option<f64> {
        self.0.oversampling
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyQuantizationSearchParams {
    fn _getters(self) {
        // Every field should have a getter method
        let QuantizationSearchParams {
            ignore: _,
            rescore: _,
            oversampling: _,
        } = self.0;
    }
}

#[pyclass(name = "AcornSearchParams")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyAcornSearchParams(AcornSearchParams);

#[pyclass_repr]
#[pymethods]
impl PyAcornSearchParams {
    #[new]
    pub fn new(enable: bool, max_selectivity: Option<f64>) -> Self {
        Self(AcornSearchParams {
            enable,
            max_selectivity: max_selectivity.map(OrderedFloat),
        })
    }

    #[getter]
    pub fn enable(&self) -> bool {
        self.0.enable
    }

    #[getter]
    pub fn max_selectivity(&self) -> Option<f64> {
        self.0
            .max_selectivity
            .map(|selectivity| selectivity.into_inner())
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyAcornSearchParams {
    fn _getters(self) {
        // Every field should have a getter method
        let AcornSearchParams {
            enable: _,
            max_selectivity: _,
        } = self.0;
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyWithVector(pub WithVector);

impl FromPyObject<'_, '_> for PyWithVector {
    type Error = PyErr;

    fn extract(with_vector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Bool(bool),
            Selector(Vec<String>),
        }

        fn _variants(with_vector: WithVector) {
            match with_vector {
                WithVector::Bool(_) => {}
                WithVector::Selector(_) => {}
            }
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

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyWithVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            WithVector::Bool(bool) => bool.into_bound_py_any(py),
            WithVector::Selector(vectors) => vectors.into_bound_py_any(py),
        }
    }
}

impl Repr for PyWithVector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            WithVector::Bool(bool) => bool.fmt(f),
            WithVector::Selector(vectors) => vectors.fmt(f),
        }
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyWithPayload(WithPayloadInterface);

impl FromPyObject<'_, '_> for PyWithPayload {
    type Error = PyErr;

    fn extract(with_payload: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Bool(bool),
            Fields(Vec<PyJsonPath>),
            Selector(PyPayloadSelector),
        }

        fn _variants(with_payload: WithPayloadInterface) {
            match with_payload {
                WithPayloadInterface::Bool(_) => {}
                WithPayloadInterface::Fields(_) => {}
                WithPayloadInterface::Selector(_) => {}
            }
        }

        let with_payload = match with_payload.extract()? {
            Helper::Bool(bool) => WithPayloadInterface::Bool(bool),
            Helper::Fields(fields) => WithPayloadInterface::Fields(PyJsonPath::peel_vec(fields)),
            Helper::Selector(selector) => {
                WithPayloadInterface::Selector(PayloadSelector::from(selector))
            }
        };

        Ok(Self(with_payload))
    }
}

impl<'py> IntoPyObject<'py> for PyWithPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyWithPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            WithPayloadInterface::Bool(bool) => bool.into_bound_py_any(py),
            WithPayloadInterface::Fields(fields) => {
                PyJsonPath::wrap_slice(fields).into_bound_py_any(py)
            }
            WithPayloadInterface::Selector(selector) => PyPayloadSelector::wrap_ref(selector)
                .clone()
                .into_bound_py_any(py),
        }
    }
}

impl Repr for PyWithPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            WithPayloadInterface::Bool(bool) => bool.fmt(f),
            WithPayloadInterface::Fields(fields) => PyJsonPath::wrap_slice(fields).fmt(f),
            WithPayloadInterface::Selector(selector) => {
                PyPayloadSelector::wrap_ref(selector).fmt(f)
            }
        }
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPayloadSelector(PayloadSelector);

impl FromPyObject<'_, '_> for PyPayloadSelector {
    type Error = PyErr;

    fn extract(selector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let selector = match selector.extract()? {
            PyPayloadSelectorInterface::Include { keys } => {
                PayloadSelector::Include(PayloadSelectorInclude {
                    include: PyJsonPath::peel_vec(keys),
                })
            }
            PyPayloadSelectorInterface::Exclude { keys } => {
                PayloadSelector::Exclude(PayloadSelectorExclude {
                    exclude: PyJsonPath::peel_vec(keys),
                })
            }
        };

        Ok(Self(selector))
    }
}

impl<'py> IntoPyObject<'py> for PyPayloadSelector {
    type Target = PyPayloadSelectorInterface;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let selector = match self.0 {
            PayloadSelector::Include(PayloadSelectorInclude { include }) => {
                PyPayloadSelectorInterface::Include {
                    keys: PyJsonPath::wrap_vec(include),
                }
            }
            PayloadSelector::Exclude(PayloadSelectorExclude { exclude }) => {
                PyPayloadSelectorInterface::Exclude {
                    keys: PyJsonPath::wrap_vec(exclude),
                }
            }
        };

        Bound::new(py, selector)
    }
}

impl Repr for PyPayloadSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (repr, keys) = match &self.0 {
            PayloadSelector::Include(PayloadSelectorInclude { include }) => {
                ("Include", PyJsonPath::wrap_slice(include))
            }
            PayloadSelector::Exclude(PayloadSelectorExclude { exclude }) => {
                ("Exclude", PyJsonPath::wrap_slice(exclude))
            }
        };

        f.complex_enum::<PyPayloadSelectorInterface>(repr, &[("keys", &keys)])
    }
}

#[pyclass(name = "PayloadSelector")]
#[derive(Clone, Debug)]
pub enum PyPayloadSelectorInterface {
    Include { keys: Vec<PyJsonPath> },
    Exclude { keys: Vec<PyJsonPath> },
}

impl Repr for PyPayloadSelectorInterface {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (repr, keys) = match self {
            PyPayloadSelectorInterface::Include { keys } => ("Include", keys),
            PyPayloadSelectorInterface::Exclude { keys } => ("Exclude", keys),
        };

        f.complex_enum::<Self>(repr, &[("keys", keys)])
    }
}
