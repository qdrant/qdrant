use bytemuck::TransparentWrapper as _;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;
use shard::scroll::*;

use crate::query::PyOrderBy;
use crate::repr::*;
use crate::types::*;

#[pyclass(name = "ScrollRequest")]
#[derive(Clone, Debug, Into)]
pub struct PyScrollRequest(ScrollRequestInternal);

#[pyclass_repr]
#[pymethods]
impl PyScrollRequest {
    #[new]
    pub fn new(
        offset: Option<PyPointId>,
        limit: Option<usize>,
        filter: Option<PyFilter>,
        with_payload: Option<PyWithPayload>,
        with_vector: PyWithVector,
        order_by: Option<PyOrderBy>,
    ) -> Self {
        Self(ScrollRequestInternal {
            offset: offset.map(PointIdType::from),
            limit,
            filter: filter.map(Filter::from),
            with_payload: with_payload.map(WithPayloadInterface::from),
            with_vector: WithVector::from(with_vector),
            order_by: order_by.map(OrderByInterface::from),
        })
    }

    #[getter]
    pub fn offset(&self) -> Option<PyPointId> {
        self.0.offset.map(PyPointId)
    }

    #[getter]
    pub fn limit(&self) -> Option<usize> {
        self.0.limit
    }

    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }

    #[getter]
    pub fn with_payload(&self) -> Option<&PyWithPayload> {
        self.0.with_payload.as_ref().map(PyWithPayload::wrap_ref)
    }

    #[getter]
    pub fn with_vector(&self) -> &PyWithVector {
        PyWithVector::wrap_ref(&self.0.with_vector)
    }

    #[getter]
    pub fn order_by(&self) -> Option<PyOrderBy> {
        self.0.order_by.clone().map(PyOrderBy::from)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyScrollRequest {
    fn _getters(self) {
        // Every field should have a getter method
        let ScrollRequestInternal {
            offset: _,
            limit: _,
            filter: _,
            with_payload: _,
            with_vector: _,
            order_by: _,
        } = self.0;
    }
}
