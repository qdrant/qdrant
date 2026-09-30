use super::TableOfContent;
use crate::content_manager::consensus_state_machine::Action;
use crate::content_manager::errors::{StorageError, StorageResult};

impl TableOfContent {
    pub(super) fn apply_action_sync(&self, action: Action) -> StorageResult<()> {
        self.general_runtime.block_on(self.apply_action(action))
    }

    async fn apply_action(&self, action: Action) -> StorageResult<()> {
        match &action {
            Action::UpdateAliases { set, remove } => {
                // Keep searches from observing a mapping while it is being replaced.
                let _collections = self.collections.write().await;
                let mut persistence = self.alias_persistence.write().await;
                let mut aliases = persistence.state().clone();

                for alias in remove {
                    aliases.remove(alias);
                }
                for (alias, collection) in set {
                    aliases.insert(alias.clone(), collection.clone());
                }

                persistence.apply_state(aliases)
            }

            Action::CreateCollection { .. }
            | Action::DropCollection { .. }
            | Action::UpdateCollectionConfig { .. }
            | Action::AddNamedVector { .. }
            | Action::DropNamedVector { .. }
            | Action::SetPayloadIndex { .. }
            | Action::DropPayloadIndex { .. }
            | Action::CreateAndRegisterShards { .. }
            | Action::InvalidateCleanLocalShards { .. }
            | Action::RemoveShardKey { .. }
            | Action::DropShard { .. }
            | Action::SetShardNumber { .. }
            | Action::RemoveShardFromKeyMapping { .. }
            | Action::SetReplicaState { .. }
            | Action::RemoveReplica { .. }
            | Action::InitLocalShard { .. }
            | Action::RegisterTransfer { .. }
            | Action::SetTransferMethod { .. }
            | Action::DeleteMigratedPoints { .. }
            | Action::RevertHashRing { .. }
            | Action::SetReshardingState { .. }
            | Action::SetReshardingStage { .. }
            | Action::StopTransferDriver { .. }
            | Action::RevertProxyShard { .. }
            | Action::UnproxifyShard { .. }
            | Action::SpawnTransferDriver { .. }
            | Action::UnregisterTransfer { .. }
            | Action::SetPeerMetadata { .. }
            | Action::SetClusterMetadataKey { .. }
            | Action::SetQuotaConfig { .. } => Err(StorageError::service_error(format!(
                "Action applier does not support {action:?}"
            ))),

            #[cfg(feature = "staging")]
            Action::TestSlowDown(_) | Action::TestTransientError(_) => Err(
                StorageError::service_error(format!("Action applier does not support {action:?}")),
            ),
        }
    }
}
