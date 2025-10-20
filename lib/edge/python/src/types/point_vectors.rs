use derive_more::Into;
use pyo3::{pyclass, pymethods};
use segment::types::PointIdType;
use shard::operations::point_ops::VectorStructPersisted;
use shard::operations::vector_ops::PointVectorsPersisted;

use crate::types::{PyPointId, PyVector};

#[pyclass(name = "PointVectors")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
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
}
