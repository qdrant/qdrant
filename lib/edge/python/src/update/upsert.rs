use crate::types::filter::PyFilter;
use crate::types::point::PyPoint;
use crate::update::PyUpdateOperation;
use pyo3::prelude::*;
use segment::types::Filter;
use shard::operations::CollectionUpdateOperations;
use shard::operations::point_ops;
use shard::operations::point_ops::PointInsertOperationsInternal;
use shard::operations::point_ops::PointStructPersisted;

#[pymethods]
impl PyUpdateOperation {
    #[staticmethod]
    pub fn upsert_points(points: Vec<PyPoint>) -> Self {
        let points = points.into_iter().map(PointStructPersisted::from).collect();

        let operation =
            CollectionUpdateOperations::PointOperation(point_ops::PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(points),
            ));

        Self(operation)
    }

    #[staticmethod]
    pub fn update_conditional(points: Vec<PyPoint>, condition: PyFilter) -> Self {
        let points = points.into_iter().map(PointStructPersisted::from).collect();
        let points_op = PointInsertOperationsInternal::PointsList(points);

        let condition = Filter::from(condition);

        let operation = CollectionUpdateOperations::PointOperation(
            point_ops::PointOperations::UpsertPointsConditional(
                point_ops::ConditionalInsertOperationInternal {
                    points_op,
                    condition,
                },
            ),
        );

        Self(operation)
    }
}
