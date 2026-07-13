use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::PointOperations;
use crate::operations::vector_name_ops::VectorNameOperations;
use crate::operations::vector_ops::VectorOperations;
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations};

impl CollectionUpdateOperations {
    /// A stable, human-readable name for the operation, including its inner variant.
    ///
    /// Used for audit logging and for reporting slow operations during WAL recovery.
    pub fn operation_name(&self) -> &'static str {
        match self {
            CollectionUpdateOperations::PointOperation(op) => match op {
                PointOperations::UpsertPoints(_) => "upsert_points",
                PointOperations::UpsertPointsConditional(_) => "upsert_points_conditional",
                PointOperations::DeletePoints { .. } => "delete_points",
                PointOperations::DeletePointsByFilter(_) => "delete_points_by_filter",
                PointOperations::SyncPoints(_) => "sync_points",
                PointOperations::UpsertPointsRaw(_) => "upsert_points_raw",
                PointOperations::SyncPointsRaw(_) => "sync_points_raw",
            },
            CollectionUpdateOperations::VectorOperation(op) => match op {
                VectorOperations::UpdateVectors(_) => "update_vectors",
                VectorOperations::DeleteVectors(_, _) => "delete_vectors",
                VectorOperations::DeleteVectorsByFilter(_, _) => "delete_vectors_by_filter",
            },
            CollectionUpdateOperations::PayloadOperation(op) => match op {
                PayloadOps::SetPayload(_) => "set_payload",
                PayloadOps::DeletePayload(_) => "delete_payload",
                PayloadOps::ClearPayload { .. } => "clear_payload",
                PayloadOps::ClearPayloadByFilter(_) => "clear_payload_by_filter",
                PayloadOps::OverwritePayload(_) => "overwrite_payload",
            },
            CollectionUpdateOperations::FieldIndexOperation(op) => match op {
                FieldIndexOperations::CreateIndex(_) => "create_field_index",
                FieldIndexOperations::DeleteIndex(_) => "delete_field_index",
            },
            CollectionUpdateOperations::VectorNameOperation(op) => match op {
                VectorNameOperations::CreateVectorName(_) => "create_vector_name",
                VectorNameOperations::DeleteVectorName(_) => "delete_vector_name",
            },
            #[cfg(feature = "staging")]
            CollectionUpdateOperations::StagingOperation(_) => "debug",
        }
    }
}
