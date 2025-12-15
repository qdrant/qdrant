use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::{pyclass, pymethods};
use segment::types::PointIdType;
use shard::operations::point_ops::VectorStructPersisted;
use shard::operations::vector_ops::PointVectorsPersisted;

use crate::repr::*;
use crate::types::{PyPointId, PyVector};

#[pyclass(name = "PointVectors")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPointVectors(pub PointVectorsPersisted);

#[pyclass_repr]
#[pymethods]
impl PyPointVectors {
    #[new]
    pub fn new(id: PyPointId, vector: PyVector) -> Self {
        Self(PointVectorsPersisted {
            id: PointIdType::from(id),
            vector: VectorStructPersisted::from(vector),
        })
    }

    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    pub fn vector(&self) -> &PyVector {
        PyVector::wrap_ref(&self.0.vector)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyPointVectors {
    fn _getters(self) {
        // Every field should have a getter method
        let PointVectorsPersisted { id: _, vector: _ } = self.0;
    }
}
