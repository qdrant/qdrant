use std::fmt;

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

impl Repr for PyRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.class::<Self>(&[
            ("id", &self.id()),
            ("vector", &self.vector()),
            ("payload", &self.payload()),
            ("order_value", &self.order_value()),
        ])
    }
}
