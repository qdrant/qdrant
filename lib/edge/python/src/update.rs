use derive_more::Into;
use pyo3::prelude::*;
use segment::data_types::vectors::*;
use shard::operations::point_ops::*;
use shard::operations::{CollectionUpdateOperations, point_ops};

use super::*;

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
    pub fn new(id: PyPointId, vector: PyVector, payload: Option<PyPayload>) -> Self {
        let point = PointStructPersisted {
            id: id.into(),
            vector: vector.into(),
            payload: payload.map(Into::into),
        };

        Self(point)
    }
}

#[pyclass(name = "Vector")]
#[derive(Clone, Debug, Into)]
pub struct PyVector(VectorStructPersisted);

#[pymethods]
impl PyVector {
    #[staticmethod]
    fn single(vec: DenseVector) -> Self {
        Self(VectorStructPersisted::Single(vec))
    }
}

impl PyVector {
    fn _variants(vector: VectorStructPersisted) {
        match vector {
            VectorStructPersisted::Single(_) => (),
            VectorStructPersisted::MultiDense(_) => todo!(), // TODO!
            VectorStructPersisted::Named(_) => todo!(),      // TODO!
        }
    }
}
