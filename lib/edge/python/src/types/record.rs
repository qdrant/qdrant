use std::mem;

use derive_more::Into;
use pyo3::prelude::*;
use segment::data_types::order_by::OrderValue;
use shard::retrieve::record_internal::RecordInternal;

use crate::*;

#[pyclass(name = "Record")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyRecord(pub RecordInternal);

impl PyRecord {
    pub fn from_rust_vec(records: Vec<RecordInternal>) -> Vec<Self> {
        // `PyRecord` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(records) }
    }
}

#[pymethods]
impl PyRecord {
    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    pub fn vector(&self) -> Option<&PyVectorInternal> {
        self.0.vector.as_ref().map(PyVectorInternal::from_ref)
    }

    #[getter]
    pub fn payload(&self) -> Option<&PyPayload> {
        self.0.payload.as_ref().map(PyPayload::from_ref)
    }

    #[getter]
    pub fn order_value(&self) -> Option<PyOrderValue> {
        self.0.order_value.map(PyOrderValue::from)
    }
}

#[derive(IntoPyObject)]
pub enum PyOrderValue {
    Int(i64),
    Float(f64),
}

impl From<OrderValue> for PyOrderValue {
    fn from(value: OrderValue) -> Self {
        match value {
            OrderValue::Int(int) => Self::Int(int),
            OrderValue::Float(float) => Self::Float(float),
        }
    }
}
