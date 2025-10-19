use derive_more::Into;
use pyo3::prelude::*;
use segment::types::{Payload, PointIdType};
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

use crate::{PyPayload, PyPointId, PyVector};

#[pyclass(name = "Point")]
#[derive(Clone, Debug, Into)]
pub struct PyPoint(PointStructPersisted);

#[pymethods]
impl PyPoint {
    #[new]
    pub fn new(id: PyPointId, vector: PyVector, payload: Option<PyPayload>) -> Result<Self, PyErr> {
        let point = PointStructPersisted {
            id: PointIdType::from(id),
            vector: VectorStructPersisted::from(vector),
            payload: payload.map(Payload::from),
        };

        Ok(Self(point))
    }

    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    pub fn vector(&self) -> PyVector {
        PyVector::from(self.0.vector.clone())
    }

    #[getter]
    pub fn payload(&self) -> Option<&PyPayload> {
        self.0.payload.as_ref().map(PyPayload::from_ref)
    }
}
