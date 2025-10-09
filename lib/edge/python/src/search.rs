use derive_more::Into;
use pyo3::prelude::*;
use segment::data_types::vectors::*;
use segment::json_path::JsonPath;
use segment::types::*;
use shard::query::query_enum::QueryEnum;
use shard::search::*;

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

    #[staticmethod]
    pub fn sparse(vector: PySparseVector) -> Self {
        Self(VectorInternal::Sparse(vector.into()))
    }
}

impl PyQueryVector {
    fn _variants(query: VectorInternal) {
        match query {
            VectorInternal::Dense(_) => (),
            VectorInternal::Sparse(_) => (),
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

#[pyclass(name = "WithVector")]
#[derive(Clone, Debug, Into)]
pub struct PyWithVector(WithVector);

#[pymethods]
impl PyWithVector {
    #[new]
    pub fn new(with_vector: bool) -> Self {
        Self(WithVector::Bool(with_vector))
    }

    #[staticmethod]
    pub fn selector(vectors: Vec<VectorNameBuf>) -> Self {
        Self(WithVector::Selector(vectors))
    }
}

impl PyWithVector {
    fn _variants(with_vector: WithVector) {
        match with_vector {
            WithVector::Bool(_) => (),
            WithVector::Selector(_) => (),
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

    #[staticmethod]
    pub fn fields(fields: Vec<PyJsonPath>) -> Self {
        let fields = fields.into_iter().map(Into::into).collect(); // TODO: Transmute!?
        Self(WithPayloadInterface::Fields(fields))
    }

    #[staticmethod]
    pub fn selector(selector: PyPayloadSelector) -> Self {
        Self(WithPayloadInterface::Selector(selector.into()))
    }
}

impl PyWithPayload {
    fn _variants(with_payload: WithPayloadInterface) {
        match with_payload {
            WithPayloadInterface::Bool(_) => (),
            WithPayloadInterface::Fields(_) => (),
            WithPayloadInterface::Selector(_) => (),
        }
    }
}

#[pyclass(name = "PayloadSelector")]
#[derive(Clone, Debug, Into)]
pub struct PyPayloadSelector(PayloadSelector);

#[pymethods]
impl PyPayloadSelector {
    #[staticmethod]
    pub fn include(fields: Vec<PyJsonPath>) -> Self {
        let include = PayloadSelectorInclude {
            include: fields.into_iter().map(Into::into).collect(), // TODO: Transmute!?
        };

        Self(PayloadSelector::Include(include))
    }

    #[staticmethod]
    pub fn exclude(fields: Vec<PyJsonPath>) -> Self {
        let exclude = PayloadSelectorExclude {
            exclude: fields.into_iter().map(Into::into).collect(), // TODO: Transmute!?
        };

        Self(PayloadSelector::Exclude(exclude))
    }
}

impl PyPayloadSelector {
    fn _variants(payload_selector: PayloadSelector) {
        match payload_selector {
            PayloadSelector::Include(_) => (),
            PayloadSelector::Exclude(_) => (),
        }
    }
}

#[pyclass(name = "JsonPath")]
#[derive(Clone, Debug, Into)]
pub struct PyJsonPath(JsonPath);

#[pymethods]
impl PyJsonPath {
    #[new]
    pub fn new(json_path: &str) -> super::Result<Self> {
        let json_path = json_path.parse().map_err(|()| {
            OperationError::validation_error(format!("{json_path} is not a valid JSON path"))
        })?;

        Ok(Self(json_path))
    }
}

#[pyclass(name = "ScoredPoint")]
#[derive(Clone, Debug, Into)]
pub struct PyScoredPoint(pub ScoredPoint);

#[pymethods]
impl PyScoredPoint {
    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId::from(self.0.id)
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
    pub fn vector(&self) -> Option<DenseVector> {
        let Some(vector) = &self.0.vector else {
            return None;
        };

        match vector {
            VectorStructInternal::Single(vec) => Some(vec.clone()),
            _ => None, // TODO!
        }
    }

    #[getter]
    pub fn payload(&self) -> Option<&PyPayload> {
        self.0.payload.as_ref().map(PyPayload::from_ref)
    }
}
