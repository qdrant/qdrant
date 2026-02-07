use shard::operations::payload_ops::PayloadOps;
use shard::operations::point_ops::PointOperations;
use shard::operations::vector_ops::VectorOperations;
use shard::operations::{CollectionUpdateOperations, FieldIndexOperations};

use crate::content_manager::collection_meta_ops::CollectionMetaOperations;

pub trait AuditableOperation {
    fn operation_name(&self) -> &'static str;
}

impl AuditableOperation for CollectionUpdateOperations {
    fn operation_name(&self) -> &'static str {
        match self {
            CollectionUpdateOperations::PointOperation(op) => match op {
                PointOperations::UpsertPoints(_) => "upsert_points",
                PointOperations::UpsertPointsConditional(_) => "upsert_points_conditional",
                PointOperations::DeletePoints { .. } => "delete_points",
                PointOperations::DeletePointsByFilter(_) => "delete_points_by_filter",
                PointOperations::SyncPoints(_) => "sync_points",
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
            #[cfg(feature = "staging")]
            CollectionUpdateOperations::StagingOperation(_) => "debug",
        }
    }
}

impl AuditableOperation for CollectionMetaOperations {
    fn operation_name(&self) -> &'static str {
        match self {
            CollectionMetaOperations::CreateCollection(_) => "create_collection",
            CollectionMetaOperations::UpdateCollection(_) => "update_collection",
            CollectionMetaOperations::DeleteCollection(_) => "delete_collection",
            CollectionMetaOperations::ChangeAliases(_) => "change_aliases",
            CollectionMetaOperations::Resharding(_, _) => "resharding",
            CollectionMetaOperations::TransferShard(_, _) => "transfer_shard",
            CollectionMetaOperations::SetShardReplicaState(_) => "set_shard_replica_state",
            CollectionMetaOperations::CreateShardKey(_) => "create_shard_key",
            CollectionMetaOperations::DropShardKey(_) => "drop_shard_key",
            CollectionMetaOperations::CreatePayloadIndex(_) => "create_payload_index",
            CollectionMetaOperations::DropPayloadIndex(_) => "drop_payload_index",
            CollectionMetaOperations::Nop { .. } => "nop",
            #[cfg(feature = "staging")]
            CollectionMetaOperations::TestSlowDown(_) => "debug",
        }
    }
}
