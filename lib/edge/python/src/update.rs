use bytemuck::TransparentWrapperAlloc as _;
use derive_more::Into;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::{Filter, Payload, VectorNameBuf};
use shard::operations::point_ops::{PointIdsList, PointInsertOperationsInternal};
use shard::operations::{CollectionUpdateOperations, payload_ops, point_ops, vector_ops};

use crate::*;

#[pyclass(name = "UpdateOperation")]
#[derive(Clone, Debug, Into)]
pub struct PyUpdateOperation(CollectionUpdateOperations);

#[pymethods]
impl PyUpdateOperation {
    #[staticmethod]
    #[pyo3(signature = (points, condition=None))]
    pub fn upsert_points(points: Vec<PyPoint>, condition: Option<PyFilter>) -> Self {
        let points = PointInsertOperationsInternal::PointsList(PyPoint::peel_vec(points));

        let operation = match condition {
            Some(condition) => point_ops::PointOperations::UpsertPointsConditional(
                point_ops::ConditionalInsertOperationInternal {
                    points_op: points,
                    condition: Filter::from(condition),
                },
            ),

            None => point_ops::PointOperations::UpsertPoints(points),
        };

        Self(CollectionUpdateOperations::PointOperation(operation))
    }

    #[staticmethod]
    pub fn delete_points(point_ids: Vec<PyPointId>) -> Self {
        let operation = point_ops::PointOperations::DeletePoints {
            ids: PyPointId::peel_vec(point_ids),
        };

        Self(CollectionUpdateOperations::PointOperation(operation))
    }

    #[staticmethod]
    pub fn delete_points_by_filter(filter: PyFilter) -> Self {
        let operation = point_ops::PointOperations::DeletePointsByFilter(Filter::from(filter));
        Self(CollectionUpdateOperations::PointOperation(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (point_vectors, condition=None))]
    pub fn update_vectors(point_vectors: Vec<PyPointVectors>, condition: Option<PyFilter>) -> Self {
        let operation = vector_ops::VectorOperations::UpdateVectors(vector_ops::UpdateVectorsOp {
            points: PyPointVectors::peel_vec(point_vectors),
            update_filter: condition.map(Filter::from),
        });

        Self(CollectionUpdateOperations::VectorOperation(operation))
    }

    #[staticmethod]
    pub fn delete_vectors(point_ids: Vec<PyPointId>, vector_names: Vec<VectorNameBuf>) -> Self {
        let operation = vector_ops::VectorOperations::DeleteVectors(
            PointIdsList::from(PyPointId::peel_vec(point_ids)),
            vector_names,
        );

        Self(CollectionUpdateOperations::VectorOperation(operation))
    }

    #[staticmethod]
    pub fn delete_vectors_by_filter(filter: PyFilter, vector_names: Vec<VectorNameBuf>) -> Self {
        let operation =
            vector_ops::VectorOperations::DeleteVectorsByFilter(Filter::from(filter), vector_names);

        Self(CollectionUpdateOperations::VectorOperation(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (point_ids, payload, key=None))]
    pub fn set_payload(
        point_ids: Vec<PyPointId>,
        payload: PyPayload,
        key: Option<PyJsonPath>,
    ) -> Self {
        let operation = payload_ops::PayloadOps::SetPayload(payload_ops::SetPayloadOp {
            payload: Payload::from(payload),
            points: Some(PyPointId::peel_vec(point_ids)),
            filter: None,
            key: key.map(JsonPath::from),
        });

        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (filter, payload, key=None))]
    pub fn set_payload_by_filter(
        filter: PyFilter,
        payload: PyPayload,
        key: Option<PyJsonPath>,
    ) -> Self {
        let operation = payload_ops::PayloadOps::SetPayload(payload_ops::SetPayloadOp {
            payload: Payload::from(payload),
            points: None,
            filter: Some(Filter::from(filter)),
            key: key.map(JsonPath::from),
        });

        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }

    #[staticmethod]
    pub fn delete_payload(point_ids: Vec<PyPointId>, keys: Vec<PyJsonPath>) -> Self {
        let operation = payload_ops::PayloadOps::DeletePayload(payload_ops::DeletePayloadOp {
            keys: PyJsonPath::peel_vec(keys),
            points: Some(PyPointId::peel_vec(point_ids)),
            filter: None,
        });

        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }

    #[staticmethod]
    pub fn delete_payload_by_filter(filter: PyFilter, keys: Vec<PyJsonPath>) -> Self {
        let operation = payload_ops::PayloadOps::DeletePayload(payload_ops::DeletePayloadOp {
            keys: PyJsonPath::peel_vec(keys),
            points: None,
            filter: Some(Filter::from(filter)),
        });

        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }

    #[staticmethod]
    pub fn clear_payload(point_ids: Vec<PyPointId>) -> Self {
        let operation = payload_ops::PayloadOps::ClearPayload {
            points: PyPointId::peel_vec(point_ids),
        };

        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }

    #[staticmethod]
    pub fn clear_payload_by_filter(filter: PyFilter) -> Self {
        let operation = payload_ops::PayloadOps::ClearPayloadByFilter(Filter::from(filter));
        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (point_ids, payload, key=None))]
    pub fn overwrite_payload(
        point_ids: Vec<PyPointId>,
        payload: PyPayload,
        key: Option<PyJsonPath>,
    ) -> Self {
        let operation = payload_ops::PayloadOps::OverwritePayload(payload_ops::SetPayloadOp {
            payload: Payload::from(payload),
            points: Some(PyPointId::peel_vec(point_ids)),
            filter: None,
            key: key.map(JsonPath::from),
        });

        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (filter, payload, key=None))]
    pub fn overwrite_payload_by_filter(
        filter: PyFilter,
        payload: PyPayload,
        key: Option<PyJsonPath>,
    ) -> Self {
        let operation = payload_ops::PayloadOps::OverwritePayload(payload_ops::SetPayloadOp {
            payload: Payload::from(payload),
            points: None,
            filter: Some(Filter::from(filter)),
            key: key.map(JsonPath::from),
        });

        Self(CollectionUpdateOperations::PayloadOperation(operation))
    }
}
