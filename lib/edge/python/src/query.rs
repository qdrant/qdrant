use std::fmt;

use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use edge::{Prefetch, QueryBatchRequest, QueryRequest};
use ordered_float::OrderedFloat;
use pyo3::exceptions::PyValueError;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use pyo3::{IntoPyObjectExt, PyTypeInfo};
use segment::data_types::order_by::{Direction, OrderBy, OrderByInterface, StartFrom};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorInternal};
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::json_path::JsonPath;
use shard::query::query_enum::QueryEnum;
use shard::query::*;

use super::*;
use crate::repr::*;
use crate::type_hint::Alias;

/// Queries executed together as one planned batch, returning results in the same order.
#[pyclass(name = "QueryBatchRequest", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyQueryBatchRequest(QueryBatchRequest);

#[pyclass_repr]
#[pymethods]
impl PyQueryBatchRequest {
    #[new]
    pub fn new(queries: Vec<PyQueryRequest>) -> Self {
        Self(QueryBatchRequest::new(
            queries.into_iter().map(Into::into).collect(),
        ))
    }

    #[getter]
    pub fn queries(&self) -> Vec<PyQueryRequest> {
        self.0.queries.iter().cloned().map(PyQueryRequest).collect()
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

/// Request for query operation.
///
/// Args:
///     limit: Maximum number of results.
///     offset: Number of results to skip.
///     query: Scoring query (vector, fusion, order_by, etc.).
///     prefetches: Prefetch stages for multi-stage queries.
///     with_vector: Whether to include vectors.
///     with_payload: Whether to include payload.
///     filter: Filter conditions.
///     score_threshold: Minimum score threshold.
///     params: Search parameters.
#[pyclass(name = "QueryRequest", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyQueryRequest(QueryRequest);

#[pyclass_repr]
#[pymethods]
impl PyQueryRequest {
    #[new]
    #[pyo3(signature = (
        limit,
        offset = None,
        query = None,
        prefetches = None,
        with_vector = None,
        with_payload = None,
        filter = None,
        score_threshold = None,
        params = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        limit: usize,
        offset: Option<usize>,
        query: Option<PyScoringQuery>,
        prefetches: Option<Vec<PyPrefetch>>,
        with_vector: Option<PyWithVector>,
        with_payload: Option<PyWithPayload>,
        filter: Option<PyFilter>,
        score_threshold: Option<f32>,
        params: Option<PySearchParams>,
    ) -> Self {
        Self(QueryRequest {
            prefetches: PyPrefetch::peel_vec(prefetches.unwrap_or_default()),
            limit,
            offset: offset.unwrap_or(0),
            with_vector: with_vector.map(WithVector::from).unwrap_or_default(),
            with_payload: with_payload
                .map(WithPayloadInterface::from)
                .unwrap_or_default(),
            query: query.map(ScoringQuery::from),
            filter: filter.map(Filter::from),
            score_threshold,
            params: params.map(SearchParams::from),
        })
    }

    /// Prefetch stages.
    #[getter]
    pub fn prefetches(&self) -> &[PyPrefetch] {
        PyPrefetch::wrap_slice(&self.0.prefetches)
    }

    /// Scoring query.
    #[getter]
    pub fn query(&self) -> Option<&PyScoringQuery> {
        self.0.query.as_ref().map(PyScoringQuery::wrap_ref)
    }

    /// Filter.
    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }

    /// Score threshold.
    #[getter]
    pub fn score_threshold(&self) -> Option<f32> {
        self.0.score_threshold
    }

    /// Result limit.
    #[getter]
    pub fn limit(&self) -> usize {
        self.0.limit
    }

    /// Result offset.
    #[getter]
    pub fn offset(&self) -> usize {
        self.0.offset
    }

    /// Search parameters.
    #[getter]
    pub fn params(&self) -> Option<PySearchParams> {
        self.0.params.clone().map(PySearchParams)
    }

    /// With vector flag.
    #[getter]
    pub fn with_vector(&self) -> &PyWithVector {
        PyWithVector::wrap_ref(&self.0.with_vector)
    }

    /// With payload flag.
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
        let QueryRequest {
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

/// A prefetch stage for multi-stage queries.
///
/// Args:
///     limit: Maximum number of results for this stage.
///     query: Scoring query.
///     prefetches: Nested prefetch stages.
///     params: Search parameters.
///     filter: Filter conditions.
///     score_threshold: Minimum score threshold.
#[pyclass(name = "Prefetch", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPrefetch(Prefetch);

#[pyclass_repr]
#[pymethods]
impl PyPrefetch {
    #[new]
    #[pyo3(signature = (
        limit,
        query = None,
        prefetches = None,
        params = None,
        filter = None,
        score_threshold = None,
    ))]
    pub fn new(
        limit: usize,
        query: Option<PyScoringQuery>,
        prefetches: Option<Vec<PyPrefetch>>,
        params: Option<PySearchParams>,
        filter: Option<PyFilter>,
        score_threshold: Option<f32>,
    ) -> Self {
        Self(Prefetch {
            prefetches: PyPrefetch::peel_vec(prefetches.unwrap_or_default()),
            limit,
            query: query.map(ScoringQuery::from),
            params: params.map(SearchParams::from),
            filter: filter.map(Filter::from),
            score_threshold,
        })
    }

    /// Nested prefetch stages.
    #[getter]
    pub fn prefetches(&self) -> &[PyPrefetch] {
        PyPrefetch::wrap_slice(&self.0.prefetches)
    }

    /// Scoring query.
    #[getter]
    pub fn query(&self) -> Option<PyScoringQuery> {
        self.0.query.clone().map(PyScoringQuery)
    }

    /// Result limit.
    #[getter]
    pub fn limit(&self) -> usize {
        self.0.limit
    }

    /// Search parameters.
    #[getter]
    pub fn params(&self) -> Option<PySearchParams> {
        self.0.params.clone().map(PySearchParams)
    }

    /// Filter.
    #[getter]
    pub fn filter(&self) -> Option<PyFilter> {
        self.0.filter.clone().map(PyFilter)
    }

    /// Score threshold.
    #[getter]
    pub fn score_threshold(&self) -> Option<f32> {
        self.0.score_threshold
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyPrefetch {
    fn _getters(self) {
        // Every field should have a getter method
        let Prefetch {
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
    const OUTPUT_TYPE: PyStaticExpr = PyPrefetch::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyScoringQuery(ScoringQuery);

pub const SCORING_QUERY: Alias = Alias {
    name: "ScoringQueryType",
    definition: ScoringQueryHelper::INPUT_TYPE,
};

#[derive(FromPyObject, IntoPyObject)]
enum ScoringQueryHelper {
    Vector(PyQuery),
    Fusion(PyFusion),
    OrderBy(PyOrderBy),
    Formula(PyFormula),
    Sample(PySample),
    Mmr(PyMmr),
}

impl FromPyObject<'_, '_> for PyScoringQuery {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = SCORING_QUERY.hint();

    fn extract(query: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
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
            ScoringQueryHelper::Vector(query) => ScoringQuery::Vector(QueryEnum::from(query)),
            ScoringQueryHelper::Fusion(fusion) => {
                ScoringQuery::Fusion(FusionInternal::from(fusion))
            }
            ScoringQueryHelper::OrderBy(order_by) => ScoringQuery::OrderBy(OrderBy::from(order_by)),
            ScoringQueryHelper::Formula(formula) => {
                ScoringQuery::Formula(ParsedFormula::from(formula))
            }
            ScoringQueryHelper::Sample(sample) => {
                ScoringQuery::Sample(SampleInternal::from(sample))
            }
            ScoringQueryHelper::Mmr(mmr) => ScoringQuery::Mmr(MmrInternal::from(mmr)),
        };

        Ok(Self(query))
    }
}

impl<'py> IntoPyObject<'py> for PyScoringQuery {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = SCORING_QUERY.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            ScoringQuery::Vector(vector) => ScoringQueryHelper::Vector(PyQuery(vector)),
            ScoringQuery::Fusion(fusion) => ScoringQueryHelper::Fusion(fusion.into()),
            ScoringQuery::OrderBy(order_by) => ScoringQueryHelper::OrderBy(PyOrderBy(order_by)),
            ScoringQuery::Formula(formula) => ScoringQueryHelper::Formula(PyFormula(formula)),
            ScoringQuery::Sample(sample) => ScoringQueryHelper::Sample(sample.into()),
            ScoringQuery::Mmr(mmr) => ScoringQueryHelper::Mmr(PyMmr(mmr)),
        }
        .into_bound_py_any(py)
    }
}

impl<'py> IntoPyObject<'py> for &PyScoringQuery {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = SCORING_QUERY.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

impl Repr for PyScoringQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            ScoringQuery::Vector(vector) => PyQuery::wrap_ref(vector).fmt(f),
            ScoringQuery::Fusion(fusion) => PyFusion::from(fusion.clone()).fmt(f),
            ScoringQuery::OrderBy(order_by) => PyOrderBy::wrap_ref(order_by).fmt(f),
            ScoringQuery::Formula(_formula) => f.unimplemented(), // TODO!
            ScoringQuery::Sample(sample) => PySample::from(*sample).fmt(f),
            ScoringQuery::Mmr(mmr) => PyMmr::wrap_ref(mmr).fmt(f),
        }
    }
}

/// Fusion methods for combining multiple prefetch results.
#[pyclass(name = "Fusion", from_py_object)]
#[derive(Clone, Debug)]
pub enum PyFusion {
    /// RRF (Reciprocal Rank Fusion) with given parameters.
    ///
    /// Args:
    ///     k: The RRF k parameter.
    ///     weights: Optional weights for each prefetch source.
    ///              Higher weight gives more influence on the final ranking.
    ///              If not specified, all prefetches are weighted equally.
    ///
    /// Examples:
    ///     # Basic RRF with k=2
    ///     Fusion.Rrf(k=2)
    ///
    ///     # Weighted RRF - first prefetch has 3x weight
    ///     Fusion.Rrf(k=2, weights=[3.0, 1.0])
    #[pyo3(constructor = (k, weights = None))]
    Rrf { k: usize, weights: Option<Vec<f32>> },
    /// DBSF (Distribution-Based Score Fusion).
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
        match self {
            PyFusion::Rrf { k, weights: None } => {
                f.complex_enum::<Self>("Rrf", &[("k", k as &dyn Repr)])
            }
            PyFusion::Rrf {
                k,
                weights: Some(weights),
            } => f.complex_enum::<Self>("Rrf", &[("k", k as &dyn Repr), ("weights", weights)]),
            PyFusion::Dbsf {} => f.complex_enum::<Self>("Dbsf", &[]),
        }
    }
}

impl From<FusionInternal> for PyFusion {
    fn from(fusion: FusionInternal) -> Self {
        match fusion {
            FusionInternal::Rrf { k, weights } => PyFusion::Rrf {
                k,
                weights: weights.map(|w| w.into_iter().map(|f| f.into_inner()).collect()),
            },
            FusionInternal::Dbsf => PyFusion::Dbsf {},
        }
    }
}

impl From<PyFusion> for FusionInternal {
    fn from(fusion: PyFusion) -> Self {
        match fusion {
            PyFusion::Rrf { k, weights } => FusionInternal::Rrf {
                k,
                weights: weights.map(|w| w.into_iter().map(ordered_float::OrderedFloat).collect()),
            },
            PyFusion::Dbsf {} => FusionInternal::Dbsf,
        }
    }
}

/// Order results by a payload field.
///
/// Args:
///     key: Payload field path.
///     direction: Sort direction.
///     start_from: Starting value.
#[pyclass(name = "OrderBy", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyOrderBy(OrderBy);

#[pyclass_repr]
#[pymethods]
impl PyOrderBy {
    #[new]
    #[pyo3(signature = (key, direction = None, start_from = None))]
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

    /// Field key.
    #[getter]
    pub fn key(&self) -> &PyJsonPath {
        PyJsonPath::wrap_ref(&self.0.key)
    }

    /// Sort direction.
    #[getter]
    pub fn direction(&self) -> Option<PyDirection> {
        self.0.direction.map(PyDirection::from)
    }

    /// Starting value.
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

/// Sort direction.
#[pyclass(name = "Direction", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyDirection {
    Asc,
    Desc,
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

pub const START_FROM: Alias = Alias {
    name: "StartFromType",
    definition: StartFromHelper::INPUT_TYPE,
};

#[derive(FromPyObject)]
enum StartFromHelper {
    Integer(IntPayloadType),
    Float(FloatPayloadType),
    DateTime(String),
}

impl FromPyObject<'_, '_> for PyStartFrom {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = START_FROM.hint();

    fn extract(start_from: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(start_from: StartFrom) {
            match start_from {
                StartFrom::Integer(_) => {}
                StartFrom::Float(_) => {}
                StartFrom::Datetime(_) => {}
            }
        }

        let start_from = match start_from.extract()? {
            StartFromHelper::Integer(int) => StartFrom::Integer(int),
            StartFromHelper::Float(float) => StartFrom::Float(float),
            StartFromHelper::DateTime(date_time) => {
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
    const OUTPUT_TYPE: PyStaticExpr = START_FROM.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyStartFrom {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = START_FROM.hint();

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

/// Sampling methods.
#[pyclass(name = "Sample", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PySample {
    Random,
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

/// Maximal Marginal Relevance for result diversification.
///
/// Args:
///     vector: Query vector.
///     lambda_: Balance between relevance and diversity (0-1).
///     candidates_limit: Number of candidates to consider.
///     using: Named vector to use.
#[pyclass(name = "Mmr", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMmr(MmrInternal);

#[pyclass_repr]
#[pymethods]
impl PyMmr {
    #[new]
    #[pyo3(signature = (vector, lambda_, candidates_limit, using = None))]
    pub fn new(
        vector: PyNamedVectorInternal,
        lambda_: f32,
        candidates_limit: usize,
        using: Option<String>,
    ) -> Self {
        let mmr = MmrInternal {
            vector: VectorInternal::from(vector),
            using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string()),
            lambda: OrderedFloat(lambda_),
            candidates_limit,
        };

        Self(mmr)
    }

    /// Query vector.
    #[getter]
    pub fn vector(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.vector)
    }

    /// Named vector.
    #[getter]
    pub fn using(&self) -> &str {
        &self.0.using
    }

    /// Balance between relevance and diversity.
    #[getter]
    pub fn lambda_(&self) -> f32 {
        self.0.lambda.into_inner()
    }

    /// Candidates limit.
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
