use shard::operations::CollectionUpdateOperations;

use crate::content_manager::collection_meta_ops::CollectionMetaOperations;

pub trait AuditableOperation {
    fn operation_name(&self) -> &'static str;
}

impl AuditableOperation for CollectionUpdateOperations {
    fn operation_name(&self) -> &'static str {
        // Delegate to the canonical mapping defined next to the enum in the `shard` crate.
        CollectionUpdateOperations::operation_name(self)
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
            CollectionMetaOperations::CreateNamedVector(_) => "create_named_vector",
            CollectionMetaOperations::DeleteNamedVector(_) => "delete_named_vector",
            CollectionMetaOperations::Nop { .. } => "nop",
            #[cfg(feature = "staging")]
            CollectionMetaOperations::TestSlowDown(_)
            | CollectionMetaOperations::TestTransientError(_) => "debug",
        }
    }
}
