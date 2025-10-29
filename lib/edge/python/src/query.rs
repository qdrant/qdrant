use std::mem;

use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::data_types::order_by::{Direction, OrderBy, StartFrom};
use segment::data_types::vectors::VectorInternal;
use shard::query::*;

use super::*;

#[pyclass(name = "QueryRequest")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyQueryRequest(ShardQueryRequest);

#[pymethods]
impl PyQueryRequest {
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        prefetches: Vec<PyPrefetch>,
        query: Option<PyScoringQuery>,
        filter: Option<PyFilter>,
        score_threshold: Option<f32>,
        limit: usize,
        offset: usize,
        params: Option<PySearchParams>,
        with_vector: PyWithVector,
        with_payload: PyWithPayload,
    ) -> Self {
        Self(ShardQueryRequest {
            prefetches: PyPrefetch::into_rust_vec(prefetches),
            query: query.map(ScoringQuery::from),
            filter: filter.map(Filter::from),
            score_threshold: score_threshold.map(OrderedFloat),
            limit,
            offset,
            params: params.map(SearchParams::from),
            with_vector: WithVector::from(with_vector),
            with_payload: WithPayloadInterface::from(with_payload),
        })
    }
}

#[pyclass(name = "Prefetch")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyPrefetch(ShardPrefetch);

impl PyPrefetch {
    pub fn into_rust_vec(prefetches: Vec<Self>) -> Vec<ShardPrefetch> {
        // `PyPrefetch` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(prefetches) }
    }
}

#[pymethods]
impl PyPrefetch {
    #[new]
    pub fn new(
        prefetches: Vec<PyPrefetch>,
        query: Option<PyScoringQuery>,
        limit: usize,
        params: Option<PySearchParams>,
        filter: Option<PyFilter>,
        score_threshold: Option<f32>,
    ) -> Self {
        Self(ShardPrefetch {
            prefetches: PyPrefetch::into_rust_vec(prefetches),
            query: query.map(ScoringQuery::from),
            limit,
            params: params.map(SearchParams::from),
            filter: filter.map(Filter::from),
            score_threshold: score_threshold.map(OrderedFloat),
        })
    }
}

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyScoringQuery(ScoringQuery);

impl<'py> FromPyObject<'py> for PyScoringQuery {
    fn extract_bound(query: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Vector(PyQuery),
            Fusion(PyFusion),
            OrderBy(PyOrderBy),
            Formula(PyFormula),
            Sample(PySample),
            Mmr(PyMmr),
        }

        let query = match query.extract()? {
            Helper::Vector(query) => ScoringQuery::Vector(query.into()),
            Helper::Fusion(fusion) => ScoringQuery::Fusion(fusion.into()),
            Helper::OrderBy(order_by) => ScoringQuery::OrderBy(order_by.into()),
            Helper::Formula(formula) => ScoringQuery::Formula(formula.into()),
            Helper::Sample(sample) => ScoringQuery::Sample(sample.into()),
            Helper::Mmr(mmr) => ScoringQuery::Mmr(mmr.into()),
        };

        Ok(Self(query))
    }
}

#[pyclass(name = "Fusion")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyFusion(FusionInternal);

impl PyFusion {
    pub fn from_ref(fusion: &FusionInternal) -> &Self {
        // `PyFusion` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(fusion) }
    }
}

#[pymethods]
impl PyFusion {
    #[staticmethod]
    pub fn rrfk(rrfk: usize) -> Self {
        Self(FusionInternal::RrfK(rrfk))
    }

    #[classattr]
    pub const DBSF: Self = Self(FusionInternal::Dbsf);
}

#[pyclass(name = "OrderBy")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyOrderBy(OrderBy);

#[pymethods]
impl PyOrderBy {
    #[new]
    pub fn new(
        key: PyJsonPath,
        direction: Option<PyDirection>,
        start_from: Option<PyStartFrom>,
    ) -> PyResult<Self> {
        let order_by = OrderBy {
            key: key.into(),
            direction: direction.map(Direction::from),
            start_from: start_from.map(StartFrom::from),
        };

        Ok(Self(order_by))
    }
}

#[pyclass(name = "Direction")]
#[derive(Copy, Clone, Debug)]
pub enum PyDirection {
    Asc,
    Desc,
}

impl From<Direction> for PyDirection {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Asc => PyDirection::Asc,
            Direction::Desc => PyDirection::Desc,
        }
    }
}

impl From<PyDirection> for Direction {
    fn from(direction: PyDirection) -> Self {
        match direction {
            PyDirection::Asc => Direction::Asc,
            PyDirection::Desc => Direction::Desc,
        }
    }
}

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyStartFrom(StartFrom);

impl<'py> FromPyObject<'py> for PyStartFrom {
    fn extract_bound(start_from: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Integer(IntPayloadType),
            Float(FloatPayloadType),
            DateTime(String),
        }

        let start_from = match start_from.extract()? {
            Helper::Integer(int) => StartFrom::Integer(int),
            Helper::Float(float) => StartFrom::Float(float),
            Helper::DateTime(date_time) => {
                let date_time = date_time.parse().map_err(|err| {
                    PyValueError::new_err(format!("failed to parse date-time: {err}"))
                })?;

                StartFrom::Datetime(date_time)
            }
        };

        Ok(Self(start_from))
    }
}

impl<'py> IntoPyObject<'py> for PyStartFrom {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> std::result::Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyStartFrom {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> std::result::Result<Self::Output, Self::Error> {
        match &self.0 {
            StartFrom::Integer(int) => int.into_bound_py_any(py),
            StartFrom::Float(float) => float.into_bound_py_any(py),
            StartFrom::Datetime(date_time) => date_time.to_string().into_bound_py_any(py),
        }
    }
}

#[pyclass(name = "Sample")]
#[derive(Copy, Clone, Debug)]
pub enum PySample {
    Random,
}

impl From<SampleInternal> for PySample {
    fn from(sample: SampleInternal) -> Self {
        match sample {
            SampleInternal::Random => PySample::Random,
        }
    }
}

impl From<PySample> for SampleInternal {
    fn from(sample: PySample) -> Self {
        match sample {
            PySample::Random => SampleInternal::Random,
        }
    }
}

#[pyclass(name = "Mmr")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyMmr(MmrInternal);

#[pymethods]
impl PyMmr {
    #[new]
    pub fn new(
        vector: PyVectorType,
        using: String,
        lambda: f32,
        candidates_limit: usize,
    ) -> PyResult<Self> {
        let mmr = MmrInternal {
            vector: VectorInternal::try_from(vector)?,
            using,
            lambda: OrderedFloat(lambda),
            candidates_limit,
        };

        Ok(Self(mmr))
    }
}
