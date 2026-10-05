use std::collections::{BTreeSet, HashMap};
use std::mem;
use std::sync::Arc;

use collection::collection::Collection;
use collection::collection_state;
use collection::shards::CollectionId;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::PeerId;
use common::fs::safe_delete_in_tmp;

use super::TableOfContent;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::collections_ops::Checker as _;
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::{Action, NodeContext};
use crate::content_manager::errors::StorageError;
use crate::content_manager::{CollectionContainer, consensus_manager};
use crate::quota::QuotaConfig;

impl CollectionContainer for TableOfContent {
    fn apply_action(&self, action: Action) -> Result<(), StorageError> {
        self.apply_action_sync(action)
    }

    fn perform_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        self.perform_collection_meta_op_sync(operation)
    }

    fn collections_snapshot(&self) -> consensus_manager::CollectionsSnapshot {
        self.collections_snapshot_sync()
    }

    fn apply_collections_snapshot(
        &self,
        data: consensus_manager::CollectionsSnapshot,
    ) -> Result<(), StorageError> {
        self.apply_collections_snapshot(data)
    }

    fn remove_peer(&self, peer_id: PeerId) -> Result<(), StorageError> {
        self.general_runtime.block_on(async {
            // Validation:
            // 1. Check that we are not removing some unique shards (removed)

            // Validation passed

            self.remove_shards_at_peer(peer_id).await?;

            if self.this_peer_id == peer_id {
                // We are detaching the current peer, so we need to remove all connections
                // Remove all peers from the channel service

                let ids_to_drop: Vec<_> = self
                    .channel_service
                    .id_to_address
                    .read()
                    .keys()
                    .filter(|id| **id != self.this_peer_id)
                    .copied()
                    .collect();
                for id in ids_to_drop {
                    self.channel_service.remove_peer(id).await;
                }
            } else {
                self.channel_service.remove_peer(peer_id).await;
            }
            Ok(())
        })
    }

    fn peer_has_shards(&self, peer_id: PeerId) -> bool {
        self.general_runtime.block_on(self.peer_has_shards(peer_id))
    }

    fn quota_config(&self) -> QuotaConfig {
        self.quota_manager().config()
    }

    fn set_quota_config(&self, config: QuotaConfig) -> Result<(), StorageError> {
        Ok(self.quota_manager().set_config(config)?)
    }

    fn sync_local_state(&self) -> Result<(), StorageError> {
        self.general_runtime.block_on(async {
            let collections = self.collections.read().await;
            let transfer_failure_callback =
                Self::on_transfer_failure_callback(self.consensus_proposal_sender.clone());
            let transfer_success_callback =
                Self::on_transfer_success_callback(self.consensus_proposal_sender.clone());

            for collection in collections.values() {
                let finish_shard_initialize = Self::change_peer_state_callback(
                    self.consensus_proposal_sender.clone(),
                    collection.name().to_string(),
                    ReplicaState::Active,
                    Some(ReplicaState::Initializing),
                );
                let convert_to_listener_callback = Self::change_peer_state_callback(
                    self.consensus_proposal_sender.clone(),
                    collection.name().to_string(),
                    ReplicaState::Listener,
                    Some(ReplicaState::Active),
                );
                let convert_from_listener_to_active_callback = Self::change_peer_state_callback(
                    self.consensus_proposal_sender.clone(),
                    collection.name().to_string(),
                    ReplicaState::Active,
                    Some(ReplicaState::Listener),
                );

                collection
                    .sync_local_state(
                        transfer_failure_callback.clone(),
                        transfer_success_callback.clone(),
                        finish_shard_initialize,
                        convert_to_listener_callback,
                        convert_from_listener_to_active_callback,
                    )
                    .await?;
            }
            Ok(())
        })
    }

    fn node_context(&self) -> NodeContext {
        NodeContext::from_storage_config(
            &self.storage_config,
            self.this_peer_id,
            self.is_distributed(),
        )
    }

    fn collection_names(&self) -> BTreeSet<CollectionId> {
        self.general_runtime
            .block_on(async { self.collections.read().await.keys().cloned().collect() })
    }

    fn alias_mapping(&self) -> AliasMapping {
        self.general_runtime
            .block_on(async { self.alias_persistence.read().await.state().clone() })
    }

    fn collection_state(&self, collection: &str) -> Option<collection_state::State> {
        self.general_runtime.block_on(async {
            let collection = self.collections.read().await.get(collection)?.state().await;
            Some(collection)
        })
    }

    fn take_dirty_collections(&self) -> BTreeSet<CollectionId> {
        mem::take(&mut self.dirty_collections.lock())
    }
}

impl TableOfContent {
    fn collections_snapshot_sync(&self) -> consensus_manager::CollectionsSnapshot {
        self.general_runtime.block_on(self.collections_snapshot())
    }

    async fn collections_snapshot(&self) -> consensus_manager::CollectionsSnapshot {
        let mut collections: HashMap<CollectionId, collection_state::State> = HashMap::new();
        for (id, collection) in self.collections.read().await.iter() {
            collections.insert(id.clone(), collection.state().await);
        }
        consensus_manager::CollectionsSnapshot {
            collections,
            aliases: self.alias_persistence.read().await.state().clone(),
        }
    }

    fn apply_collections_snapshot(
        &self,
        data: consensus_manager::CollectionsSnapshot,
    ) -> Result<(), StorageError> {
        self.general_runtime.block_on(async {
            for (id, state) in &data.collections {
                // Collection construction and state application can take a long time. Clone the
                // handle so unrelated collection lookups do not wait for this work.
                let mut existing_collection = self.collections.read().await.get(id).cloned();

                if let Some(collection) = &existing_collection {
                    let collection_uuid = collection.uuid().await;

                    let recreate_collection = if collection_uuid != state.config.uuid {
                        log::warn!(
                            "Recreating collection {id}, because collection UUID is different: \
                             existing collection UUID: {collection_uuid:?}, \
                             Raft snapshot collection UUID: {:?}",
                            state.config.uuid,
                        );

                        true
                    } else if let Err(err) = collection.check_config_compatible(&state.config).await {
                        log::warn!(
                            "Recreating collection {id}, because collection config is incompatible: \
                             {err}",
                        );

                        true
                    } else {
                        false
                    };

                    if recreate_collection {
                        // Deletion waits for outstanding handles before removing the files.
                        drop(existing_collection.take());
                        self.delete_collection(id).await?;
                    }
                }

                let collection_exists = existing_collection.is_some();

                // Serialize filesystem changes with regular collection creation and deletion.
                let collection_create_guard = if !collection_exists {
                    Some(self.collection_create_lock.lock().await)
                } else {
                    None
                };

                let existing_collection = if let Some(collection) = existing_collection {
                    collection
                } else {
                    self.collections.read().await.validate_collection_not_exists(id)?;
                    let collection_path = self.create_collection_path(id).await?;
                    let snapshots_path = self.create_snapshots_path(id).await?;
                    let shard_distribution =
                        CollectionShardDistribution::from_shards_info(state.shards.clone());
                    let collection = Collection::new(
                        id.clone(),
                        self.this_peer_id,
                        &collection_path,
                        &snapshots_path,
                        &state.config,
                        self.storage_config
                            .to_shared_storage_config(self.is_distributed())
                            .into(),
                        shard_distribution,
                        Some(state.shards_key_mapping.clone()),
                        self.channel_service.clone(),
                        Self::change_peer_from_state_callback(
                            self.consensus_proposal_sender.clone(),
                            id.clone(),
                            ReplicaState::Dead,
                        ),
                        Self::request_shard_transfer_callback(
                            self.consensus_proposal_sender.clone(),
                            id.clone(),
                        ),
                        Self::abort_shard_transfer_callback(
                            self.consensus_proposal_sender.clone(),
                            id.clone(),
                        ),
                        Some(self.adaptive_search_handle.clone()),
                        Some(self.update_runtime.handle().clone()),
                        self.optimizer_resource_budget.clone(),
                        self.storage_config.optimizers_overwrite.clone(),
                    )
                    .await?;
                    Arc::new(collection)
                };

                let result = self
                    .apply_collection_snapshot_state(id, &existing_collection, state)
                    .await;

                // A new collection already exists on disk, but we need to set replica states
                // and disable new local replicas before we can add it to the list of collections.
                //
                // If we fail while preparing the collection, we must remove it from disk.
                // Otherwise, the next attempt to create it would fail because it exists on disk,
                // but is not in the list of collections.
                if let Err(error) = result {
                    if !collection_exists {
                        existing_collection.stop_gracefully().await;
                        drop(existing_collection);

                        let path = self.get_collection_path(id);
                        let deleted_dir = self.storage_config.storage_path.join(".deleted");

                        let to_delete =
                            safe_delete_in_tmp(&path, &deleted_dir).map_err(|cleanup_error| {
                                StorageError::service_error(format!(
                                    "Failed to apply snapshot state for collection {id}: {error}; \
                                     failed to remove unpublished collection: {cleanup_error}",
                                ))
                            })?;

                        tokio::task::spawn_blocking(move || {
                            if let Err(error) = to_delete.close() {
                                log::error!("Can't delete unpublished collection from disk: {error}");
                            }
                        });
                    }

                    return Err(error);
                }

                // Mark local shards as dead (to initiate shard transfer),
                // if collection has been created during snapshot application
                if !collection_exists {
                    for shard_id in existing_collection.get_local_shards().await {
                        let shard_holder = existing_collection.shards_holder().read_owned().await;

                        let Some(replica_set) = shard_holder.get_shard(shard_id) else {
                            continue;
                        };

                        if replica_set.is_local().await {
                            replica_set.add_locally_disabled(None, self.this_peer_id, None);
                        }
                    }

                    // Keep new collections hidden while applying replica states and local
                    // disabling, so requests cannot reach them between these steps.
                    let mut collections = self.collections.write().await;
                    collections.validate_collection_not_exists(id)?;
                    collections.insert(id.clone(), existing_collection);
                }

                drop(collection_create_guard);
            }

            // Collect names without retaining collection handles that would delay deletion.
            let collection_names: Vec<_> = self.collections.read().await.keys().cloned().collect();

            // Remove collections that are present locally, but are not in the snapshot state
            for collection_name in &collection_names {
                if !data.collections.contains_key(collection_name) {
                    log::debug!(
                        "Deleting collection {collection_name} \
                         because it is not part of the consensus snapshot",
                    );

                    self.delete_collection(collection_name).await?;
                }
            }

            // Apply alias mapping
            self.alias_persistence
                .write()
                .await
                .apply_state(data.aliases)?;

            Ok(())
        })
    }

    async fn apply_collection_snapshot_state(
        &self,
        id: &str,
        collection: &Collection,
        state: &collection_state::State,
    ) -> Result<(), StorageError> {
        if &collection.state().await == state {
            return Ok(());
        }

        let Some(proposal_sender) = self.consensus_proposal_sender.clone() else {
            log::error!("Can't apply state: single node mode");
            return Ok(());
        };

        // State application may discover a transfer that the sender needs to abort.
        let abort_transfer = |transfer| {
            let abort_transfer = ConsensusOperations::abort_transfer(
                id.to_string(),
                transfer,
                "sender was not up to date",
            );

            if let Err(err) = proposal_sender.send(abort_transfer) {
                log::error!("Can't report transfer progress to consensus: {err}");
            }
        };

        #[cfg(test)]
        tests::fail_collection_snapshot_state(id)?;

        collection
            .apply_state(state.clone(), self.this_peer_id(), abort_transfer)
            .await?;

        Ok(())
    }

    async fn remove_shards_at_peer(&self, peer_id: PeerId) -> Result<(), StorageError> {
        let collections = self.collections.read().await;
        for collection in collections.values() {
            collection.remove_shards_at_peer(peer_id).await?;
        }
        Ok(())
    }

    fn on_transfer_failure_callback(
        proposal_sender: Option<OperationSender>,
    ) -> collection::collection::OnTransferFailure {
        Arc::new(move |transfer, collection_name, reason| {
            if let Some(proposal_sender) = &proposal_sender {
                let operation = ConsensusOperations::abort_transfer(
                    collection_name.clone(),
                    transfer.clone(),
                    reason,
                );
                if let Err(send_error) = proposal_sender.send(operation) {
                    log::error!(
                        "Can't send proposal to abort transfer of shard {} of collection {collection_name}. Error: {send_error}",
                        transfer.shard_id,
                    );
                }
            }
        })
    }

    fn on_transfer_success_callback(
        proposal_sender: Option<OperationSender>,
    ) -> collection::collection::OnTransferSuccess {
        Arc::new(move |transfer, collection_name| {
            if let Some(proposal_sender) = &proposal_sender {
                let operation =
                    ConsensusOperations::finish_transfer(collection_name.clone(), transfer.clone());
                if let Err(send_error) = proposal_sender.send(operation) {
                    log::error!(
                        "Can't send proposal to complete transfer of shard {} of collection {collection_name}. Error: {send_error}",
                        transfer.shard_id,
                    );
                }
            }
        })
    }
}

#[cfg(test)]
mod tests;
