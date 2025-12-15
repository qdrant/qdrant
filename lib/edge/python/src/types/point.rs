use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::{Payload, PointIdType};
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

use crate::repr::*;
use crate::{PyPayload, PyPointId, PyVector};

#[pyclass(name = "Point")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPoint(PointStructPersisted);

#[pyclass_repr]
#[pymethods]
impl PyPoint {
    #[new]
    pub fn new(id: PyPointId, vector: PyVector, payload: Option<PyPayload>) -> Self {
        let point = PointStructPersisted {
            id: PointIdType::from(id),
            vector: VectorStructPersisted::from(vector),
            payload: payload.map(Payload::from),
        };

        Self(point)
    }

    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    pub fn vector(&self) -> &PyVector {
        PyVector::wrap_ref(&self.0.vector)
    }

    #[getter]
    pub fn payload(&self) -> Option<&PyPayload> {
        self.0.payload.as_ref().map(PyPayload::wrap_ref)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyPoint {
    fn _getters(self) {
        // Every field should have a getter method
        let PointStructPersisted {
            id: _,
            vector: _,
            payload: _,
        } = self.0;
    }
}
