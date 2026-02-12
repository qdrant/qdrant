use bytemuck::TransparentWrapper;
use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::prelude::*;
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequest;

use crate::repr::*;
use crate::*;

#[pyclass(name = "SearchRequest", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PySearchRequest(CoreSearchRequest);

#[pyclass_repr]
#[pymethods]
impl PySearchRequest {
    #[new]
    #[pyo3(signature = (
        query,
        limit,
        offset = None,
        filter = None,
        params = None,
        with_vector = None,
        with_payload = None,
        score_threshold = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query: PyQuery,
        limit: usize,
        offset: Option<usize>,
        filter: Option<PyFilter>,
        params: Option<PySearchParams>,
        with_vector: Option<PyWithVector>,
        with_payload: Option<PyWithPayload>,
        score_threshold: Option<f32>,
    ) -> Self {
        Self(CoreSearchRequest {
            query: QueryEnum::from(query),
            limit,
            offset: offset.unwrap_or(0),
            filter: filter.map(Filter::from),
            params: params.map(SearchParams::from),
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

#[pyclass(name = "SearchParams", from_py_object)]
#[derive(Copy, Clone, Debug, Into)]
pub struct PySearchParams(pub SearchParams);

#[pyclass_repr]
#[pymethods]
impl PySearchParams {
    #[new]
    #[pyo3(signature = (
        hnsw_ef = None,
        exact = false,
        quantization = None,
        indexed_only = false,
        acorn = None,
    ))]
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

#[pyclass(name = "QuantizationSearchParams", from_py_object)]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyQuantizationSearchParams(QuantizationSearchParams);

#[pyclass_repr]
#[pymethods]
impl PyQuantizationSearchParams {
    #[new]
    #[pyo3(signature = (ignore = false, rescore = None, oversampling = None))]
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

#[pyclass(name = "AcornSearchParams", from_py_object)]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyAcornSearchParams(AcornSearchParams);

#[pyclass_repr]
#[pymethods]
impl PyAcornSearchParams {
    #[new]
    #[pyo3(signature = (enable = false, max_selectivity = None))]
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
