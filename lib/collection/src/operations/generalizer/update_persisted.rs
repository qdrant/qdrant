use segment::types::Payload;
use serde_json::Value;
use shard::operations::payload_ops::{PayloadOps, SetPayloadOp};
use shard::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, ConditionalInsertOperationInternal,
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, PointSyncOperation,
    VectorPersisted, VectorStructPersisted,
};
use shard::operations::vector_ops::{PointVectorsPersisted, UpdateVectorsOp, VectorOperations};
use shard::operations::{CollectionUpdateOperations, FieldIndexOperations};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;

use crate::operations::generalizer::{Generalizer, Loggable};

impl Generalizer for Payload {
    fn remove_vectors_and_payloads(&self) -> Self {
        let mut stripped_payload = Payload::default();
        stripped_payload.0.insert(
            "keys".to_string(),
            Value::Array(self.keys().cloned().map(Value::String).collect()),
        );
        stripped_payload
    }
}

impl Loggable for CollectionUpdateOperations {
    fn to_log_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn request_name(&self) -> &'static str {
        "points-update"
    }
}

impl Generalizer for CollectionUpdateOperations {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                CollectionUpdateOperations::PointOperation(
                    point_operation.remove_vectors_and_payloads(),
                )
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                CollectionUpdateOperations::VectorOperation(
                    vector_operation.remove_vectors_and_payloads(),
                )
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                CollectionUpdateOperations::PayloadOperation(
                    payload_operation.remove_vectors_and_payloads(),
                )
            }
            CollectionUpdateOperations::FieldIndexOperation(field_operation) => {
                CollectionUpdateOperations::FieldIndexOperation(
                    field_operation.remove_vectors_and_payloads(),
                )
            }
        }
    }
}

impl Generalizer for PointOperations {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            PointOperations::UpsertPoints(upsert_operation) => {
                PointOperations::UpsertPoints(upsert_operation.remove_vectors_and_payloads())
            }
            PointOperations::UpsertPointsConditional(upsert_conditional_operation) => {
                PointOperations::UpsertPointsConditional(
                    upsert_conditional_operation.remove_vectors_and_payloads(),
                )
            }
            PointOperations::DeletePoints { ids } => {
                PointOperations::DeletePoints { ids: ids.clone() }
            }
            PointOperations::DeletePointsByFilter(filter) => {
                PointOperations::DeletePointsByFilter(filter.clone())
            }
            PointOperations::SyncPoints(sync_operation) => {
                PointOperations::SyncPoints(sync_operation.remove_vectors_and_payloads())
            }
        }
    }
}

impl Generalizer for PointSyncOperation {
    fn remove_vectors_and_payloads(&self) -> Self {
        let Self {
            from_id,
            to_id,
            points,
        } = self;

        Self {
            from_id: *from_id,
            to_id: *to_id,
            points: points
                .iter()
                .map(|point| point.remove_vectors_and_payloads())
                .collect(),
        }
    }
}

impl Generalizer for PointStructPersisted {
    fn remove_vectors_and_payloads(&self) -> Self {
        let Self {
            id,
            vector,
            payload,
        } = self;

        Self {
            id: *id,
            vector: vector.remove_vectors_and_payloads(),
            payload: payload.as_ref().map(|p| p.remove_vectors_and_payloads()),
        }
    }
}

impl Generalizer for ConditionalInsertOperationInternal {
    fn remove_vectors_and_payloads(&self) -> Self {
        let Self {
            points_op,
            condition,
        } = self;

        Self {
            condition: condition.clone(),
            points_op: points_op.remove_vectors_and_payloads(),
        }
    }
}

impl Generalizer for PointInsertOperationsInternal {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            PointInsertOperationsInternal::PointsBatch(batch) => {
                PointInsertOperationsInternal::PointsBatch(batch.remove_vectors_and_payloads())
            }
            PointInsertOperationsInternal::PointsList(list) => {
                PointInsertOperationsInternal::PointsList(
                    list.iter()
                        .map(|point| point.remove_vectors_and_payloads())
                        .collect(),
                )
            }
        }
    }
}

impl Generalizer for BatchPersisted {
    fn remove_vectors_and_payloads(&self) -> Self {
        let Self {
            ids,
            vectors,
            payloads,
        } = self;

        let vectors = match vectors {
            BatchVectorStructPersisted::Single(vectors) => BatchVectorStructPersisted::Single(
                vectors.iter().map(|v| vec![v.len() as f32]).collect(),
            ),
            BatchVectorStructPersisted::MultiDense(multi) => {
                BatchVectorStructPersisted::MultiDense(
                    multi
                        .iter()
                        .map(|v| {
                            let dim = if v.is_empty() { 0 } else { v[0].len() };
                            vec![vec![v.len() as f32, dim as f32]]
                        })
                        .collect(),
                )
            }
            BatchVectorStructPersisted::Named(named) => {
                let generalized_named = named
                    .iter()
                    .map(|(name, vectors)| {
                        let generalized_vectors = vectors
                            .iter()
                            .map(|vector| vector.remove_vectors_and_payloads())
                            .collect();
                        (name.clone(), generalized_vectors)
                    })
                    .collect();
                BatchVectorStructPersisted::Named(generalized_named)
            }
        };

        Self {
            ids: ids.clone(),
            vectors,
            payloads: payloads.as_ref().map(|pls| {
                pls.iter()
                    .map(|payload| payload.as_ref().map(|pl| pl.remove_vectors_and_payloads()))
                    .collect()
            }),
        }
    }
}

impl Generalizer for VectorOperations {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            VectorOperations::UpdateVectors(update_vectors) => {
                VectorOperations::UpdateVectors(update_vectors.remove_vectors_and_payloads())
            }
            VectorOperations::DeleteVectors(_, _) => self.clone(),
            VectorOperations::DeleteVectorsByFilter(_, _) => self.clone(),
        }
    }
}

impl Generalizer for UpdateVectorsOp {
    fn remove_vectors_and_payloads(&self) -> Self {
        let UpdateVectorsOp {
            points,
            update_filter,
        } = self;

        Self {
            points: points
                .iter()
                .map(|point| point.remove_vectors_and_payloads())
                .collect(),
            update_filter: update_filter.clone(),
        }
    }
}

impl Generalizer for PointVectorsPersisted {
    fn remove_vectors_and_payloads(&self) -> Self {
        let PointVectorsPersisted { id, vector } = self;
        Self {
            id: *id,
            vector: vector.remove_vectors_and_payloads(),
        }
    }
}

impl Generalizer for VectorStructPersisted {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            VectorStructPersisted::Single(dense) => {
                VectorStructPersisted::Single(vec![dense.len() as f32])
            }
            VectorStructPersisted::MultiDense(multi) => {
                let dim = if multi.is_empty() { 0 } else { multi[0].len() };
                VectorStructPersisted::MultiDense(vec![vec![multi.len() as f32, dim as f32]])
            }
            VectorStructPersisted::Named(named) => {
                let generalized_named = named
                    .iter()
                    .map(|(name, vector)| (name.clone(), vector.remove_vectors_and_payloads()))
                    .collect();
                VectorStructPersisted::Named(generalized_named)
            }
        }
    }
}

impl Generalizer for VectorPersisted {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            VectorPersisted::Dense(dense) => VectorPersisted::Dense(vec![dense.len() as f32]),
            VectorPersisted::Sparse(sparse) => VectorPersisted::Sparse(
                SparseVector::new(vec![sparse.len() as DimId], vec![0.0]).unwrap(),
            ),
            VectorPersisted::MultiDense(multi) => {
                let dim = if multi.is_empty() { 0 } else { multi[0].len() };
                VectorPersisted::MultiDense(vec![vec![multi.len() as f32, dim as f32]])
            }
        }
    }
}

impl Generalizer for PayloadOps {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            PayloadOps::SetPayload(set_payload) => {
                PayloadOps::SetPayload(set_payload.remove_vectors_and_payloads())
            }
            PayloadOps::DeletePayload(delete_payload) => {
                PayloadOps::DeletePayload(delete_payload.clone())
            }
            PayloadOps::ClearPayload { points } => PayloadOps::ClearPayload {
                points: points.clone(),
            },
            PayloadOps::ClearPayloadByFilter(filter) => {
                PayloadOps::ClearPayloadByFilter(filter.clone())
            }
            PayloadOps::OverwritePayload(overwrite_payload) => {
                PayloadOps::OverwritePayload(overwrite_payload.remove_vectors_and_payloads())
            }
        }
    }
}

impl Generalizer for SetPayloadOp {
    fn remove_vectors_and_payloads(&self) -> Self {
        let Self {
            payload,
            points,
            filter,
            key,
        } = self;

        Self {
            payload: payload.remove_vectors_and_payloads(),
            points: points.clone(),
            filter: filter.clone(),
            key: key.clone(),
        }
    }
}

impl Generalizer for FieldIndexOperations {
    fn remove_vectors_and_payloads(&self) -> Self {
        self.clone()
    }
}
