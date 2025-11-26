use derive_more::Into;
use pyo3::{pyclass, pymethods};
use segment::types::PointIdType;
use shard::operations::point_ops::VectorStructPersisted;
use shard::operations::vector_ops::PointVectorsPersisted;

use crate::types::{PyPointId, PyVector};

#[pyclass(name = "PointVectors")]
#[derive(Clone, Debug, Into)]
pub struct PyPointVectors(pub PointVectorsPersisted);

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
    fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    fn vector(&self) -> PyVector {
        PyVector::from(self.0.vector.clone())
    }
}
