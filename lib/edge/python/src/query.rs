use std::fmt;

use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::data_types::order_by::{Direction, OrderBy, StartFrom};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorInternal};
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::json_path::JsonPath;
use shard::query::query_enum::QueryEnum;
use shard::query::*;
use shard::scroll::OrderByInterface;

use super::*;
use crate::repr::*;

#[pyclass(name = "QueryRequest")]
#[derive(Clone, Debug, Into)]
pub struct PyQueryRequest(ShardQueryRequest);

#[pyclass_repr]
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
            prefetches: PyPrefetch::peel_vec(prefetches),
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

    #[getter]
    pub fn prefetches(&self) -> &[PyPrefetch] {
        PyPrefetch::wrap_slice(&self.0.prefetches)
    }

    #[getter]
    pub fn query(&self) -> Option<&PyScoringQuery> {
        self.0.query.as_ref().map(PyScoringQuery::wrap_ref)
    }

    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }

    #[getter]
    pub fn score_threshold(&self) -> Option<f32> {
        self.0
            .score_threshold
            .map(|threshold| threshold.into_inner())
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
    pub fn params(&self) -> Option<PySearchParams> {
        self.0.params.map(PySearchParams)
    }

    #[getter]
    pub fn with_vector(&self) -> &PyWithVector {
        PyWithVector::wrap_ref(&self.0.with_vector)
    }

    #[getter]
    pub fn with_payload(&self) -> &PyWithPayload {
        PyWithPayload::wrap_ref(&self.0.with_payload)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyQueryRequest {
    fn _getters(self) {
        // Every field should have a getter method
        let ShardQueryRequest {
            prefetches: _,
            query: _,
            filter: _,
            score_threshold: _,
            limit: _,
            offset: _,
            params: _,
            with_vector: _,
            with_payload: _,
        } = self.0;
    }
}

#[pyclass(name = "Prefetch")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPrefetch(ShardPrefetch);

#[pyclass_repr]
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
            prefetches: PyPrefetch::peel_vec(prefetches),
            query: query.map(ScoringQuery::from),
            limit,
            params: params.map(SearchParams::from),
            filter: filter.map(Filter::from),
            score_threshold: score_threshold.map(OrderedFloat),
        })
    }

    #[getter]
    pub fn prefetches(&self) -> &[PyPrefetch] {
        PyPrefetch::wrap_slice(&self.0.prefetches)
    }

    #[getter]
    pub fn query(&self) -> Option<PyScoringQuery> {
        self.0.query.clone().map(PyScoringQuery)
    }

    #[getter]
    pub fn limit(&self) -> usize {
        self.0.limit
    }

    #[getter]
    pub fn params(&self) -> Option<PySearchParams> {
        self.0.params.map(PySearchParams)
    }

    #[getter]
    pub fn filter(&self) -> Option<PyFilter> {
        self.0.filter.clone().map(PyFilter)
    }

    #[getter]
    pub fn score_threshold(&self) -> Option<f32> {
        self.0
            .score_threshold
            .map(|threshold| threshold.into_inner())
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyPrefetch {
    fn _getters(self) {
        // Every field should have a getter method
        let ShardPrefetch {
            prefetches: _,
            query: _,
            limit: _,
            params: _,
            filter: _,
            score_threshold: _,
        } = self.0;
    }
}

impl<'py> IntoPyObject<'py> for &PyPrefetch {
    type Target = PyPrefetch;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyScoringQuery(ScoringQuery);

impl FromPyObject<'_, '_> for PyScoringQuery {
    type Error = PyErr;

    fn extract(query: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Vector(PyQuery),
            Fusion(PyFusion),
            OrderBy(PyOrderBy),
            Formula(PyFormula),
            Sample(PySample),
            Mmr(PyMmr),
        }

        fn _variants(query: ScoringQuery) {
            match query {
                ScoringQuery::Vector(_) => {}
                ScoringQuery::Fusion(_) => {}
                ScoringQuery::OrderBy(_) => {}
                ScoringQuery::Formula(_) => {}
                ScoringQuery::Sample(_) => {}
                ScoringQuery::Mmr(_) => {}
            }
        }

        let query = match query.extract()? {
            Helper::Vector(query) => ScoringQuery::Vector(QueryEnum::from(query)),
            Helper::Fusion(fusion) => ScoringQuery::Fusion(FusionInternal::from(fusion)),
            Helper::OrderBy(order_by) => ScoringQuery::OrderBy(OrderBy::from(order_by)),
            Helper::Formula(formula) => ScoringQuery::Formula(ParsedFormula::from(formula)),
            Helper::Sample(sample) => ScoringQuery::Sample(SampleInternal::from(sample)),
            Helper::Mmr(mmr) => ScoringQuery::Mmr(MmrInternal::from(mmr)),
        };

        Ok(Self(query))
    }
}

impl<'py> IntoPyObject<'py> for PyScoringQuery {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            ScoringQuery::Vector(vector) => PyQuery(vector).into_bound_py_any(py),
            ScoringQuery::Fusion(fusion) => PyFusion::from(fusion).into_bound_py_any(py),
            ScoringQuery::OrderBy(order_by) => PyOrderBy(order_by).into_bound_py_any(py),
            ScoringQuery::Formula(formula) => PyFormula(formula).into_bound_py_any(py),
            ScoringQuery::Sample(sample) => PySample::from(sample).into_bound_py_any(py),
            ScoringQuery::Mmr(mmr) => PyMmr(mmr).into_bound_py_any(py),
        }
    }
}

impl<'py> IntoPyObject<'py> for &PyScoringQuery {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

impl Repr for PyScoringQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            ScoringQuery::Vector(vector) => PyQuery::wrap_ref(vector).fmt(f),
            ScoringQuery::Fusion(fusion) => PyFusion::from(*fusion).fmt(f),
            ScoringQuery::OrderBy(order_by) => PyOrderBy::wrap_ref(order_by).fmt(f),
            ScoringQuery::Formula(_formula) => f.unimplemented(), // TODO!
            ScoringQuery::Sample(sample) => PySample::from(*sample).fmt(f),
            ScoringQuery::Mmr(mmr) => PyMmr::wrap_ref(mmr).fmt(f),
        }
    }
}

#[pyclass(name = "Fusion")]
#[derive(Copy, Clone, Debug)]
pub enum PyFusion {
    Rrfk { rrfk: usize },
    Dbsf {},
}

#[pymethods]
impl PyFusion {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyFusion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (repr, fields): (_, &[(_, &dyn Repr)]) = match self {
            PyFusion::Rrfk { rrfk } => ("Rrfk", &[("rrfk", rrfk)]),
            PyFusion::Dbsf {} => ("Dbsf", &[]),
        };

        f.complex_enum::<Self>(repr, fields)
    }
}

impl From<FusionInternal> for PyFusion {
    fn from(fusion: FusionInternal) -> Self {
        match fusion {
            FusionInternal::RrfK(rrfk) => PyFusion::Rrfk { rrfk },
            FusionInternal::Dbsf => PyFusion::Dbsf {},
        }
    }
}

impl From<PyFusion> for FusionInternal {
    fn from(fusion: PyFusion) -> Self {
        match fusion {
            PyFusion::Rrfk { rrfk } => FusionInternal::RrfK(rrfk),
            PyFusion::Dbsf {} => FusionInternal::Dbsf,
        }
    }
}

#[pyclass(name = "OrderBy")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyOrderBy(OrderBy);

#[pyclass_repr]
#[pymethods]
impl PyOrderBy {
    #[new]
    pub fn new(
        key: PyJsonPath,
        direction: Option<PyDirection>,
        start_from: Option<PyStartFrom>,
    ) -> PyResult<Self> {
        let order_by = OrderBy {
            key: JsonPath::from(key),
            direction: direction.map(Direction::from),
            start_from: start_from.map(StartFrom::from),
        };

        Ok(Self(order_by))
    }

    #[getter]
    pub fn key(&self) -> &PyJsonPath {
        PyJsonPath::wrap_ref(&self.0.key)
    }

    #[getter]
    pub fn direction(&self) -> Option<PyDirection> {
        self.0.direction.map(PyDirection::from)
    }

    #[getter]
    pub fn start_from(&self) -> Option<PyStartFrom> {
        self.0.start_from.map(PyStartFrom)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyOrderBy {
    fn _getters(self) {
        // Every field should have a getter method
        let OrderBy {
            key: _,
            direction: _,
            start_from: _,
        } = self.0;
    }
}

impl From<OrderByInterface> for PyOrderBy {
    fn from(order_by: OrderByInterface) -> Self {
        Self(OrderBy::from(order_by))
    }
}

impl From<PyOrderBy> for OrderByInterface {
    fn from(order_by: PyOrderBy) -> Self {
        OrderByInterface::Struct(OrderBy::from(order_by))
    }
}

#[pyclass(name = "Direction")]
#[derive(Copy, Clone, Debug)]
pub enum PyDirection {
    Asc,
    Desc,
}

#[pymethods]
impl PyDirection {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            PyDirection::Asc => "Asc",
            PyDirection::Desc => "Desc",
        };

        f.simple_enum::<Self>(repr)
    }
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

#[derive(Copy, Clone, Debug, Into)]
pub struct PyStartFrom(StartFrom);

impl FromPyObject<'_, '_> for PyStartFrom {
    type Error = PyErr;

    fn extract(start_from: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Integer(IntPayloadType),
            Float(FloatPayloadType),
            DateTime(String),
        }

        fn _variants(start_from: StartFrom) {
            match start_from {
                StartFrom::Integer(_) => {}
                StartFrom::Float(_) => {}
                StartFrom::Datetime(_) => {}
            }
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

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyStartFrom {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            StartFrom::Integer(int) => int.into_bound_py_any(py),
            StartFrom::Float(float) => float.into_bound_py_any(py),
            StartFrom::Datetime(date_time) => date_time.to_string().into_bound_py_any(py),
        }
    }
}

impl Repr for PyStartFrom {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            StartFrom::Integer(int) => int.fmt(f),
            StartFrom::Float(float) => float.fmt(f),
            StartFrom::Datetime(date_time) => date_time.to_string().fmt(f),
        }
    }
}

#[pyclass(name = "Sample")]
#[derive(Copy, Clone, Debug)]
pub enum PySample {
    Random,
}

#[pymethods]
impl PySample {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PySample {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            PySample::Random => "Random",
        };

        f.simple_enum::<Self>(repr)
    }
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
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMmr(MmrInternal);

#[pyclass_repr]
#[pymethods]
impl PyMmr {
    #[new]
    pub fn new(
        vector: PyNamedVectorInternal,
        using: Option<String>,
        lambda: f32,
        candidates_limit: usize,
    ) -> Self {
        let mmr = MmrInternal {
            vector: VectorInternal::from(vector),
            using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string()),
            lambda: OrderedFloat(lambda),
            candidates_limit,
        };

        Self(mmr)
    }

    #[getter]
    pub fn vector(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.vector)
    }

    #[getter]
    pub fn using(&self) -> &str {
        &self.0.using
    }

    #[getter]
    pub fn lambda(&self) -> f32 {
        self.0.lambda.into_inner()
    }

    #[getter]
    pub fn candidates_limit(&self) -> usize {
        self.0.candidates_limit
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMmr {
    fn _getters(self) {
        // Every field should have a getter method
        let MmrInternal {
            vector: _,
            using: _,
            lambda: _,
            candidates_limit: _,
        } = self.0;
    }
}
