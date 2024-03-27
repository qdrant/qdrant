use super::access::Access;
use crate::content_manager::claims::{incompatible_with_collection_claim, PointsOpClaimsChecker};
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::errors::StorageError;

impl Access {
    pub fn check_point_op(&self, op: &mut impl PointsOpClaimsChecker) -> Result<(), StorageError> {
        for collection in op.collections_used() {
            self.check_partial_collection_rights(collection)?;
        }
        if let Some(payload) = self.payload.as_ref() {
            op.apply_payload_claim(payload)?;
        }
        Ok(())
    }

    pub fn check_collection_meta_operation(
        &self,
        operation: &CollectionMetaOperations,
    ) -> Result<(), StorageError> {
        match operation {
            CollectionMetaOperations::CreateCollection(_)
            | CollectionMetaOperations::UpdateCollection(_)
            | CollectionMetaOperations::DeleteCollection(_)
            | CollectionMetaOperations::ChangeAliases(_)
            | CollectionMetaOperations::TransferShard(_, _)
            | CollectionMetaOperations::SetShardReplicaState(_)
            | CollectionMetaOperations::CreateShardKey(_)
            | CollectionMetaOperations::DropShardKey(_) => {
                if self.collections.is_some() {
                    return incompatible_with_collection_claim();
                }
            }
            CollectionMetaOperations::CreatePayloadIndex(op) => {
                self.check_whole_collection_rights(&op.collection_name)?;
            }
            CollectionMetaOperations::DropPayloadIndex(op) => {
                self.check_whole_collection_rights(&op.collection_name)?;
            }
            CollectionMetaOperations::Nop { token: _ } => (),
        }
        Ok(())
    }
}
