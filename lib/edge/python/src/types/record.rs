use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use shard::retrieve::record_internal::RecordInternal;

use crate::repr::*;
use crate::*;

#[pyclass(name = "Record")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyRecord(pub RecordInternal);

#[pyclass_repr]
#[pymethods]
impl PyRecord {
    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
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

impl PyRecord {
    fn _getters(self) {
        // Every field should have a getter method
        let RecordInternal {
            id: _,
            payload: _,
            vector: _,
            shard_key: _, // not relevant for Qdrant Edge
            order_value: _,
        } = self.0;
    }
}
