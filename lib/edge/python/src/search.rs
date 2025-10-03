use derive_more::Into;
use pyo3::prelude::*;
use segment::data_types::vectors::*;
use segment::types::*;
use shard::query::query_enum::QueryEnum;
use shard::search::*;

use super::*;

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

#[pyclass(name = "Query")]
#[derive(Clone, Debug, Into)]
pub struct PyQuery(QueryEnum);

#[pymethods]
impl PyQuery {
    #[staticmethod]
    pub fn nearest(query: PyQueryVector, using: Option<String>) -> Self {
        Self(QueryEnum::Nearest(NamedQuery {
            query: query.into(),
            using,
        }))
    }
}

impl PyQuery {
    fn _variants(query: QueryEnum) {
        match query {
            QueryEnum::Nearest(_) => (),
            QueryEnum::RecommendBestScore(_) => todo!(), // TODO!
            QueryEnum::RecommendSumScores(_) => todo!(), // TODO!
            QueryEnum::Discover(_) => todo!(),           // TODO!
            QueryEnum::Context(_) => todo!(),            // TODO!
        }
    }
}

#[pyclass(name = "QueryVector")]
#[derive(Clone, Debug, Into)]
pub struct PyQueryVector(VectorInternal);

#[pymethods]
impl PyQueryVector {
    #[staticmethod]
    pub fn dense(vector: Vec<f32>) -> Self {
        Self(VectorInternal::Dense(vector))
    }
}

impl PyQueryVector {
    fn _variants(query: VectorInternal) {
        match query {
            VectorInternal::Dense(_) => (),
            VectorInternal::Sparse(_) => todo!(),     // TODO!
            VectorInternal::MultiDense(_) => todo!(), // TODO!
        }
    }
}

#[pyclass(name = "Filter")]
#[derive(Clone, Debug, Into)]
pub struct PyFilter(Filter);

#[pymethods]
impl PyFilter {
    // TODO!
}

#[pyclass(name = "SearchParams")]
#[derive(Clone, Debug, Into)]
pub struct PySearchParams(SearchParams);

#[pymethods]
impl PySearchParams {
    // TODO!
}

#[pyclass(name = "WithVector")]
#[derive(Clone, Debug, Into)]
pub struct PyWithVector(WithVector);

#[pymethods]
impl PyWithVector {
    #[new]
    fn new(with_vector: bool) -> Self {
        Self(WithVector::Bool(with_vector))
    }

    #[staticmethod]
    fn selector(vectors: Vec<VectorNameBuf>) -> Self {
        Self(WithVector::Selector(vectors)) // TODO?
    }
}

impl PyWithVector {
    fn _variants(with_vector: WithVector) {
        match with_vector {
            WithVector::Bool(_) => (),
            WithVector::Selector(_) => (), // TODO?
        }
    }
}

#[pyclass(name = "WithPayload")]
#[derive(Clone, Debug, Into)]
pub struct PyWithPayload(WithPayloadInterface);

#[pymethods]
impl PyWithPayload {
    #[new]
    pub fn new(with_payload: bool) -> Self {
        Self(WithPayloadInterface::Bool(with_payload))
    }
}

impl PyWithPayload {
    fn _variants(with_payload: WithPayloadInterface) {
        match with_payload {
            WithPayloadInterface::Bool(_) => (),
            WithPayloadInterface::Fields(_) => todo!(), // TODO!
            WithPayloadInterface::Selector(_) => todo!(), // TODO!
        }
    }
}

#[pyclass(name = "ScoredPoint")]
#[derive(Clone, Debug, Into)]
pub struct PyScoredPoint(pub ScoredPoint);

#[pymethods]
impl PyScoredPoint {
    #[getter]
    fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    fn version(&self) -> u64 {
        self.0.version
    }

    #[getter]
    fn score(&self) -> f32 {
        self.0.score
    }

    #[getter]
    fn vector(&self) -> Option<DenseVector> {
        let Some(vector) = &self.0.vector else {
            return None;
        };

        match vector {
            VectorStructInternal::Single(vec) => Some(vec.clone()),
            _ => None, // TODO!
        }
    }

    #[getter]
    fn payload(&self) -> Option<PyPayload> {
        self.0.payload.clone().map(PyPayload)
    }
}
