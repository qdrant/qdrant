use super::ConsensusManager;
use crate::content_manager::CollectionContainer;
use crate::content_manager::consensus_state_machine::Action;
use crate::content_manager::errors::{StorageError, StorageResult};

impl<C: CollectionContainer> ConsensusManager<C> {
    /// Apply one planned action, returning runtime failures as service errors for replay
    pub fn apply_action(&self, action: Action) -> StorageResult<()> {
        self.apply_action_impl(action).map_err(|error| {
            if let StorageError::ServiceError { .. } = error {
                error
            } else {
                StorageError::service_error(format!("Failed to apply consensus action: {error}"))
            }
        })
    }

    fn apply_action_impl(&self, action: Action) -> StorageResult<()> {
        match action {
            Action::AddPeer { peer_id, uri } => self.persistent.write().insert_peer(peer_id, uri),

            Action::RemovePeer { peer_id } => {
                // Earlier collection actions own replica and transfer cleanup
                self.toc.remove_peer_channels(peer_id)?;

                let persistent = self.persistent.write();
                persistent.peer_metadata_by_id.write().remove(&peer_id);
                persistent.save()
            }

            Action::SetPeerMetadata { peer_id, metadata } => self
                .persistent
                .write()
                .update_peer_metadata(peer_id, metadata),

            Action::SetClusterMetadataKey { key, value } => {
                let mut persistent = self.persistent.write();
                persistent.update_cluster_metadata_key(key, value);
                persistent.save()
            }

            Action::SetQuotaConfig { config } => self.toc.set_quota_config(config),

            Action::CreateCollection { .. }
            | Action::DropCollection { .. }
            | Action::UpdateCollectionConfig { .. }
            | Action::AddNamedVector { .. }
            | Action::DropNamedVector { .. }
            | Action::SetPayloadIndex { .. }
            | Action::DropPayloadIndex { .. }
            | Action::CreateAndRegisterShards { .. }
            | Action::RemoveShardKey { .. }
            | Action::RemoveShardFromKeyMapping { .. }
            | Action::DropShard { .. }
            | Action::SetShardNumber { .. }
            | Action::SetReplicaState { .. }
            | Action::RemoveReplica { .. }
            | Action::InitLocalShard { .. }
            | Action::StopTransferDriver { .. }
            | Action::RevertProxyShard { .. }
            | Action::UnproxifyShard { .. }
            | Action::SetReshardingState { .. }
            | Action::SetReshardingStage { .. }
            | Action::RevertHashRing { .. }
            | Action::DeleteMigratedPoints { .. }
            | Action::InvalidateCleanLocalShards { .. }
            | Action::SpawnTransferDriver { .. }
            | Action::UnregisterTransfer { .. }
            | Action::RegisterTransfer { .. }
            | Action::SetTransferMethod { .. }
            | Action::UpdateAliases { .. } => self.toc.apply_action(action),

            #[cfg(feature = "staging")]
            Action::TestSlowDown(_) | Action::TestTransientError(_) => {
                self.toc.apply_action(action)
            }
        }
    }
}
