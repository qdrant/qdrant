use derive_more::Into;
use pyo3::prelude::*;
use shard::operations::point_ops::*;
use shard::operations::{CollectionUpdateOperations, point_ops};

use crate::*;

#[pyclass(name = "UpdateOperation")]
#[derive(Clone, Debug, Into)]
pub struct PyUpdateOperation(CollectionUpdateOperations);

#[pymethods]
impl PyUpdateOperation {
    #[staticmethod]
    pub fn upsert_points(points: Vec<PyPoint>) -> Self {
        let points = points.into_iter().map(Into::into).collect();

        let operation =
            CollectionUpdateOperations::PointOperation(point_ops::PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(points),
            ));

        Self(operation)
    }
}

#[pyclass(name = "Point")]
#[derive(Clone, Debug, Into)]
pub struct PyPoint(PointStructPersisted);

#[pymethods]
impl PyPoint {
    #[new]
    pub fn new(id: PyPointId, vector: PyVector, payload: Option<PyPayload>) -> Result<Self, PyErr> {
        let point = PointStructPersisted {
            id: PointIdType::try_from(id)?,
            vector: VectorStructPersisted::from(vector),
            payload: payload.map(Payload::from),
        };

        Ok(Self(point))
    }
}
