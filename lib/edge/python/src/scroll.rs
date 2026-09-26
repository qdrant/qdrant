use bytemuck::TransparentWrapper as _;
use derive_more::Into;
use edge::ScrollRequest;
use pyo3::prelude::*;
use segment::data_types::order_by::OrderByInterface;
use segment::types::*;

use crate::query::PyOrderBy;
use crate::repr::*;
use crate::types::*;

/// Request for scroll operation.
///
/// Args:
///     offset: Starting point ID.
///     limit: Maximum number of results.
///     filter: Filter conditions.
///     with_payload: Whether to include payload.
///     with_vector: Whether to include vectors.
///     order_by: Order by configuration.
#[pyclass(name = "ScrollRequest", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyScrollRequest(ScrollRequest);

#[pyclass_repr]
#[pymethods]
impl PyScrollRequest {
    #[new]
    #[pyo3(signature = (
        offset = None,
        limit = None,
        filter = None,
        with_payload = None,
        with_vector = None,
        order_by = None,
    ))]
    pub fn new(
        offset: Option<PyPointId>,
        limit: Option<usize>,
        filter: Option<PyFilter>,
        with_payload: Option<PyWithPayload>,
        with_vector: Option<PyWithVector>,
        order_by: Option<PyOrderBy>,
    ) -> Self {
        Self(ScrollRequest {
            offset: offset.map(PointIdType::from),
            limit,
            filter: filter.map(Filter::from),
            with_payload: with_payload.map(WithPayloadInterface::from),
            with_vector: with_vector.map(WithVector::from).unwrap_or_default(),
            order_by: order_by.map(OrderByInterface::from),
        })
    }

    /// Offset point ID.
    #[getter]
    pub fn offset(&self) -> Option<PyPointId> {
        self.0.offset.map(PyPointId)
    }

    /// Result limit.
    #[getter]
    pub fn limit(&self) -> Option<usize> {
        self.0.limit
    }

    /// Filter.
    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }

    /// With payload flag.
    #[getter]
    pub fn with_payload(&self) -> Option<&PyWithPayload> {
        self.0.with_payload.as_ref().map(PyWithPayload::wrap_ref)
    }

    /// With vector flag.
    #[getter]
    pub fn with_vector(&self) -> &PyWithVector {
        PyWithVector::wrap_ref(&self.0.with_vector)
    }

    /// Order by configuration.
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
        let ScrollRequest {
            offset: _,
            limit: _,
            filter: _,
            with_payload: _,
            with_vector: _,
            order_by: _,
        } = self.0;
    }
}
