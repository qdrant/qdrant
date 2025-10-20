use std::str::FromStr;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::{Filter, Payload, VectorNameBuf};
use shard::operations::point_ops::{
    PointIdsList, PointInsertOperationsInternal, PointStructPersisted,
};
use shard::operations::vector_ops::PointVectorsPersisted;
use shard::operations::{CollectionUpdateOperations, payload_ops, point_ops, vector_ops};

use crate::types::{PyFilter, PyPayload, PyPoint, PyPointId, PyPointVectors};

#[pyclass(name = "UpdateOperation")]
#[derive(Clone, Debug, Into)]
pub struct PyUpdateOperation(CollectionUpdateOperations);

#[pymethods] // Can't split impl block due to pyo3 limitations, so all constructors go here
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
    pub fn upsert_points_conditional(points: Vec<PyPoint>, condition: PyFilter) -> Self {
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

    #[staticmethod]
    pub fn delete_points(ids: Vec<PyPointId>) -> Self {
        let point_ids = PyPointId::into_rust_vec(ids);

        let operation =
            CollectionUpdateOperations::PointOperation(point_ops::PointOperations::DeletePoints {
                ids: point_ids,
            });

        Self(operation)
    }

    #[staticmethod]
    pub fn delete_points_by_filter(filter: PyFilter) -> Self {
        let filter = Filter::from(filter);

        let operation = CollectionUpdateOperations::PointOperation(
            point_ops::PointOperations::DeletePointsByFilter(filter),
        );

        Self(operation)
    }

    #[staticmethod]
    pub fn update_vectors(point_vectors: Vec<PyPointVectors>) -> Self {
        let points = point_vectors
            .into_iter()
            .map(PointVectorsPersisted::from)
            .collect();

        let operation = CollectionUpdateOperations::VectorOperation(
            vector_ops::VectorOperations::UpdateVectors(vector_ops::UpdateVectorsOp {
                points,
                update_filter: None,
            }),
        );

        Self(operation)
    }

    #[staticmethod]
    pub fn update_vectors_conditional(
        point_vectors: Vec<PyPointVectors>,
        filter: PyFilter,
    ) -> Self {
        let points = point_vectors
            .into_iter()
            .map(PointVectorsPersisted::from)
            .collect();
        let filter = Filter::from(filter);
        let operation = CollectionUpdateOperations::VectorOperation(
            vector_ops::VectorOperations::UpdateVectors(vector_ops::UpdateVectorsOp {
                points,
                update_filter: Some(filter),
            }),
        );
        Self(operation)
    }

    #[staticmethod]
    pub fn delete_vectors(ids: Vec<PyPointId>, vector_names: Vec<VectorNameBuf>) -> Self {
        let point_ids = PyPointId::into_rust_vec(ids);
        let operation = CollectionUpdateOperations::VectorOperation(
            vector_ops::VectorOperations::DeleteVectors(
                PointIdsList::from(point_ids),
                vector_names,
            ),
        );
        Self(operation)
    }

    #[staticmethod]
    pub fn delete_vectors_by_filter(filter: PyFilter, vector_names: Vec<VectorNameBuf>) -> Self {
        let filter = Filter::from(filter);
        let operation = CollectionUpdateOperations::VectorOperation(
            vector_ops::VectorOperations::DeleteVectorsByFilter(filter, vector_names),
        );
        Self(operation)
    }

    #[staticmethod]
    #[pyo3(signature = (ids, payload, key=None))]
    pub fn set_payload(
        ids: Vec<PyPointId>,
        payload: PyPayload,
        key: Option<String>,
    ) -> Result<Self, PyErr> {
        let point_ids = PyPointId::into_rust_vec(ids);
        let payload = Payload::from(payload);

        let key = key
            .map(|k| JsonPath::from_str(&k).map_err(|_| PyErr::new::<PyValueError, _>(k)))
            .transpose()?;

        let operation = CollectionUpdateOperations::PayloadOperation(
            payload_ops::PayloadOps::SetPayload(payload_ops::SetPayloadOp {
                payload,
                points: Some(point_ids),
                filter: None,
                key,
            }),
        );

        Ok(Self(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (filter, payload, key=None))]
    pub fn set_payload_by_filter(
        filter: PyFilter,
        payload: PyPayload,
        key: Option<String>,
    ) -> Result<Self, PyErr> {
        let filter = Filter::from(filter);
        let payload = Payload::from(payload);

        let key = key
            .map(|k| JsonPath::from_str(&k).map_err(|_| PyErr::new::<PyValueError, _>(k)))
            .transpose()?;

        let operation = CollectionUpdateOperations::PayloadOperation(
            payload_ops::PayloadOps::SetPayload(payload_ops::SetPayloadOp {
                payload,
                points: None,
                filter: Some(filter),
                key,
            }),
        );
        Ok(Self(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (ids, keys))]
    pub fn delete_payload(ids: Vec<PyPointId>, keys: Vec<String>) -> Result<Self, PyErr> {
        let point_ids = PyPointId::into_rust_vec(ids);

        let keys: Vec<_> = keys
            .into_iter()
            .map(|k| JsonPath::from_str(&k).map_err(|_| PyErr::new::<PyValueError, _>(k)))
            .collect::<Result<_, _>>()?;

        let operation = CollectionUpdateOperations::PayloadOperation(
            payload_ops::PayloadOps::DeletePayload(payload_ops::DeletePayloadOp {
                keys,
                points: Some(point_ids),
                filter: None,
            }),
        );
        Ok(Self(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (filter, keys))]
    pub fn delete_payload_by_filter(filter: PyFilter, keys: Vec<String>) -> Result<Self, PyErr> {
        let filter = Filter::from(filter);
        let keys: Vec<_> = keys
            .into_iter()
            .map(|k| JsonPath::from_str(&k).map_err(|_| PyErr::new::<PyValueError, _>(k)))
            .collect::<Result<_, _>>()?;

        let operation = CollectionUpdateOperations::PayloadOperation(
            payload_ops::PayloadOps::DeletePayload(payload_ops::DeletePayloadOp {
                keys,
                points: None,
                filter: Some(filter),
            }),
        );
        Ok(Self(operation))
    }

    #[staticmethod]
    pub fn clear_payload(ids: Vec<PyPointId>) -> Self {
        let point_ids = PyPointId::into_rust_vec(ids);
        let operation =
            CollectionUpdateOperations::PayloadOperation(payload_ops::PayloadOps::ClearPayload {
                points: point_ids,
            });
        Self(operation)
    }

    #[staticmethod]
    pub fn clear_payload_by_filter(filter: PyFilter) -> Self {
        let filter = Filter::from(filter);
        let operation = CollectionUpdateOperations::PayloadOperation(
            payload_ops::PayloadOps::ClearPayloadByFilter(filter),
        );
        Self(operation)
    }

    #[staticmethod]
    #[pyo3(signature = (ids, payload, key=None))]
    pub fn overwrite_payload(
        ids: Vec<PyPointId>,
        payload: PyPayload,
        key: Option<String>,
    ) -> Result<Self, PyErr> {
        let point_ids = PyPointId::into_rust_vec(ids);
        let payload = Payload::from(payload);

        let key = key
            .map(|k| JsonPath::from_str(&k).map_err(|_| PyErr::new::<PyValueError, _>(k)))
            .transpose()?;

        let operation = CollectionUpdateOperations::PayloadOperation(
            payload_ops::PayloadOps::OverwritePayload(payload_ops::SetPayloadOp {
                payload,
                points: Some(point_ids),
                filter: None,
                key,
            }),
        );

        Ok(Self(operation))
    }

    #[staticmethod]
    #[pyo3(signature = (filter, payload, key=None))]
    pub fn overwrite_payload_by_filter(
        filter: PyFilter,
        payload: PyPayload,
        key: Option<String>,
    ) -> Result<Self, PyErr> {
        let filter = Filter::from(filter);
        let payload = Payload::from(payload);

        let key = key
            .map(|k| JsonPath::from_str(&k).map_err(|_| PyErr::new::<PyValueError, _>(k)))
            .transpose()?;

        let operation = CollectionUpdateOperations::PayloadOperation(
            payload_ops::PayloadOps::OverwritePayload(payload_ops::SetPayloadOp {
                payload,
                points: None,
                filter: Some(filter),
                key,
            }),
        );
        Ok(Self(operation))
    }
}
