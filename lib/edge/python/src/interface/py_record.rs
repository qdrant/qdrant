use derive_more::Into;
use pyo3::prelude::*;
use segment::data_types::order_by::OrderValue;
use shard::operations::point_ops::VectorStructPersisted;
use shard::retrieve::record_internal::RecordInternal;

use crate::interface::py_vector::PyVector;
use crate::{PyPayload, PyPointId};

#[pyclass(name = "Record")]
#[derive(Clone, Debug, Into)]
pub struct PyRecord(pub RecordInternal);

#[pymethods]
impl PyRecord {
    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    pub fn vector(&self) -> Option<PyVector> {
        let Some(vector) = &self.0.vector else {
            return None;
        };

        let vector_persisted = VectorStructPersisted::from(vector.clone());

        Some(PyVector(vector_persisted))
    }

    #[getter]
    pub fn payload(&self) -> Option<PyPayload> {
        self.0.payload.clone().map(PyPayload)
    }

    #[getter]
    pub fn order_value(&self) -> Option<PyOrderValue> {
        self.0.order_value.map(PyOrderValue::from)
    }
}

#[derive(IntoPyObject)]
pub enum PyOrderValue {
    // Put Int first so ints don't get parsed as floats (since f64 can extract from ints).
    Int(i64),
    Float(f64),
}

impl From<OrderValue> for PyOrderValue {
    fn from(value: OrderValue) -> Self {
        match value {
            OrderValue::Int(int) => PyOrderValue::Int(int),
            OrderValue::Float(float) => PyOrderValue::Float(float),
        }
    }
}
