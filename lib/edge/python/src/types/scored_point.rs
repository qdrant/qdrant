use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::ScoredPoint;

use super::PyOrderValue;
use crate::repr::*;
use crate::{PyPayload, PyPointId, PyVectorInternal};

#[pyclass(name = "ScoredPoint")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyScoredPoint(pub ScoredPoint);

#[pyclass_repr]
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
    pub fn vector(&self) -> Option<&PyVectorInternal> {
        self.0.vector.as_ref().map(PyVectorInternal::wrap_ref)
    }

    #[getter]
    pub fn payload(&self) -> Option<&PyPayload> {
        self.0.payload.as_ref().map(PyPayload::wrap_ref)
    }

    #[getter]
    pub fn order_value(&self) -> Option<PyOrderValue> {
        self.0.order_value.map(PyOrderValue::from)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyScoredPoint {
    fn _getters(self) {
        // Every field should have a getter method
        let ScoredPoint {
            id: _,
            version: _,
            score: _,
            vector: _,
            payload: _,
            shard_key: _, // not relevant for Qdrant Edge
            order_value: _,
        } = self.0;
    }
}
