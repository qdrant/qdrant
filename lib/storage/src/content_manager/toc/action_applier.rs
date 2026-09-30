use collection::events::IndexCreatedEvent;
use common::ambient::{AmbientContext, AmbientFutureExt};

use super::TableOfContent;
use crate::content_manager::consensus_state_machine::Action;
use crate::content_manager::errors::{StorageError, StorageResult};

impl TableOfContent {
    pub(super) fn apply_action_sync(&self, action: Action) -> StorageResult<()> {
        self.general_runtime.block_on(self.apply_action(action))
    }

    async fn apply_action(&self, action: Action) -> StorageResult<()> {
        match &action {
            Action::SetPayloadIndex {
                collection,
                field_name,
                field_schema,
            } => {
                let collection_ctx =
                    AmbientContext::request(self.get_collection_hw_metrics(collection.clone()));

                self.get_collection_unchecked(collection)
                    .await?
                    .create_payload_index(field_name.clone(), field_schema.clone())
                    .measured(collection_ctx)
                    .await?;

                issues::publish(IndexCreatedEvent {
                    collection_id: collection.clone(),
                    field_name: field_name.clone(),
                });

                Ok(())
            }

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
