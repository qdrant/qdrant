use crate::operations::payload_ops::PayloadOps;
use crate::operations::point_ops::PointOperations;
use crate::operations::vector_name_ops::VectorNameOperations;
use crate::operations::vector_ops::VectorOperations;
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations};

impl CollectionUpdateOperations {
    /// A human-readable label for the operation, including its inner variant.
    ///
    /// Intended for logging (e.g. reporting slow operations during WAL recovery).
    pub fn label(&self) -> &'static str {
        match self {
            CollectionUpdateOperations::PointOperation(op) => match op {
                PointOperations::UpsertPoints(_) => "PointOperation::UpsertPoints",
                PointOperations::UpsertPointsConditional(_) => {
                    "PointOperation::UpsertPointsConditional"
                }
                PointOperations::DeletePoints { .. } => "PointOperation::DeletePoints",
                PointOperations::DeletePointsByFilter(_) => "PointOperation::DeletePointsByFilter",
                PointOperations::SyncPoints(_) => "PointOperation::SyncPoints",
            },
            CollectionUpdateOperations::VectorOperation(op) => match op {
                VectorOperations::UpdateVectors(_) => "VectorOperation::UpdateVectors",
                VectorOperations::DeleteVectors(..) => "VectorOperation::DeleteVectors",
                VectorOperations::DeleteVectorsByFilter(..) => {
                    "VectorOperation::DeleteVectorsByFilter"
                }
            },
            CollectionUpdateOperations::PayloadOperation(op) => match op {
                PayloadOps::SetPayload(_) => "PayloadOperation::SetPayload",
                PayloadOps::DeletePayload(_) => "PayloadOperation::DeletePayload",
                PayloadOps::ClearPayload { .. } => "PayloadOperation::ClearPayload",
                PayloadOps::ClearPayloadByFilter(_) => "PayloadOperation::ClearPayloadByFilter",
                PayloadOps::OverwritePayload(_) => "PayloadOperation::OverwritePayload",
            },
            CollectionUpdateOperations::FieldIndexOperation(op) => match op {
                FieldIndexOperations::CreateIndex(_) => "FieldIndexOperation::CreateIndex",
                FieldIndexOperations::DeleteIndex(_) => "FieldIndexOperation::DeleteIndex",
            },
            CollectionUpdateOperations::VectorNameOperation(op) => match op {
                VectorNameOperations::CreateVectorName(_) => {
                    "VectorNameOperation::CreateVectorName"
                }
                VectorNameOperations::DeleteVectorName(_) => {
                    "VectorNameOperation::DeleteVectorName"
                }
            },
            #[cfg(feature = "staging")]
            CollectionUpdateOperations::StagingOperation(_) => "StagingOperation",
        }
    }
}
