use std::collections::HashSet;
use std::path::Path;

use collection::collection_state;
use collection::config::ShardingMethod;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use collection::shards::{transfer, CollectionId};
use uuid::Uuid;

use super::TableOfContent;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::collections_ops::Checker as _;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::errors::StorageError;
use crate::content_manager::shard_distribution::ShardDistributionProposal;

impl TableOfContent {
    pub(super) fn perform_collection_meta_op_sync(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        self.general_runtime
            .block_on(self.perform_collection_meta_op(operation))
    }

    pub async fn perform_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        match operation {
            CollectionMetaOperations::CreateCollection(mut operation) => {
                log::info!("Creating collection {}", operation.collection_name);
                let distribution = match operation.take_distribution() {
                    None => match operation
                        .create_collection
                        .sharding_method
                        .unwrap_or_default()
                    {
                        ShardingMethod::Auto => CollectionShardDistribution::all_local(
                            operation.create_collection.shard_number,
                            self.this_peer_id,
                        ),
                        ShardingMethod::Custom => ShardDistributionProposal::empty().into(),
                    },
                    Some(distribution) => distribution.into(),
                };
                self.create_collection(
                    &operation.collection_name,
                    operation.create_collection,
                    distribution,
                )
                .await
            }
            CollectionMetaOperations::UpdateCollection(operation) => {
                log::info!("Updating collection {}", operation.collection_name);
                self.update_collection(operation).await
            }
            CollectionMetaOperations::DeleteCollection(operation) => {
                log::info!("Deleting collection {}", operation.0);
                self.delete_collection(&operation.0).await
            }
            CollectionMetaOperations::ChangeAliases(operation) => {
                log::debug!("Changing aliases");
                self.update_aliases(operation).await
            }
            CollectionMetaOperations::TransferShard(collection, operation) => {
                log::debug!("Transfer shard {:?} of {}", operation, collection);

                self.handle_transfer(collection, operation)
                    .await
                    .map(|()| true)
            }
            CollectionMetaOperations::SetShardReplicaState(operation) => {
                log::debug!("Set shard replica state {:?}", operation);
                self.set_shard_replica_state(operation).await.map(|()| true)
            }
            CollectionMetaOperations::Nop { .. } => Ok(true),
            CollectionMetaOperations::CreateShardKey(create_shard_key) => {
                log::debug!("Create shard key {:?}", create_shard_key);
                self.create_shard_key(create_shard_key).await.map(|()| true)
            }
            CollectionMetaOperations::DropShardKey(drop_shard_key) => {
                log::debug!("Drop shard key {:?}", drop_shard_key);
                self.drop_shard_key(drop_shard_key).await.map(|()| true)
            }
            CollectionMetaOperations::CreatePayloadIndex(create_payload_index) => {
                log::debug!("Create payload index {:?}", create_payload_index);
                self.create_payload_index(create_payload_index)
                    .await
                    .map(|()| true)
            }
            CollectionMetaOperations::DropPayloadIndex(drop_payload_index) => {
                log::debug!("Drop payload index {:?}", drop_payload_index);
                self.drop_payload_index(drop_payload_index)
                    .await
                    .map(|()| true)
            }
        }
    }

    async fn update_collection(
        &self,
        mut operation: UpdateCollectionOperation,
    ) -> Result<bool, StorageError> {
        let replica_changes = operation.take_shard_replica_changes();
        let UpdateCollection {
            vectors,
            hnsw_config,
            params,
            optimizers_config,
            quantization_config,
            sparse_vectors,
        } = operation.update_collection;
        let collection = self.get_collection(&operation.collection_name).await?;
        let mut recreate_optimizers = false;

        if let Some(diff) = optimizers_config {
            collection.update_optimizer_params_from_diff(diff).await?;
            recreate_optimizers = true;
        }
        if let Some(diff) = params {
            collection.update_params_from_diff(diff).await?;
            recreate_optimizers = true;
        }
        if let Some(diff) = hnsw_config {
            collection.update_hnsw_config_from_diff(diff).await?;
            recreate_optimizers = true;
        }
        if let Some(diff) = vectors {
            collection.update_vectors_from_diff(&diff).await?;
            recreate_optimizers = true;
        }
        if let Some(diff) = quantization_config {
            collection
                .update_quantization_config_from_diff(diff)
                .await?;
            recreate_optimizers = true;
        }
        if let Some(diff) = sparse_vectors {
            collection.update_sparse_vectors_from_other(&diff).await?;
            recreate_optimizers = true;
        }
        if let Some(changes) = replica_changes {
            collection.handle_replica_changes(changes).await?;
        }

        // Recreate optimizers
        if recreate_optimizers {
            collection.recreate_optimizers_blocking().await?;
        }
        Ok(true)
    }

    pub(super) async fn delete_collection(
        &self,
        collection_name: &str,
    ) -> Result<bool, StorageError> {
        if let Some(removed) = self.collections.write().await.remove(collection_name) {
            self.alias_persistence
                .write()
                .await
                .remove_collection(collection_name)?;

            let path = self.get_collection_path(collection_name);
            drop(removed);

            // Move collection to ".deleted" folder to prevent accidental reuse
            let uuid = Uuid::new_v4().to_string();
            let removed_collections_path =
                Path::new(&self.storage_config.storage_path).join(".deleted");
            tokio::fs::create_dir_all(&removed_collections_path).await?;
            let deleted_path = removed_collections_path
                .join(collection_name)
                .with_extension(uuid);
            tokio::fs::rename(path, &deleted_path).await?;

            // At this point collection is removed from memory and moved to ".deleted" folder.
            // Next time we load service the collection will not appear in the list of collections.
            // We can take our time to delete the collection from disk.
            tokio::spawn(async move {
                if let Err(error) = tokio::fs::remove_dir_all(&deleted_path).await {
                    log::error!(
                        "Can't delete collection {} from disk. Error: {}",
                        deleted_path.display(),
                        error
                    );
                }
            });
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// performs several alias changes in an atomic fashion
    async fn update_aliases(
        &self,
        operation: ChangeAliasesOperation,
    ) -> Result<bool, StorageError> {
        // Lock all collections for alias changes
        // Prevent search on partially switched collections
        let collection_lock = self.collections.write().await;
        let mut alias_lock = self.alias_persistence.write().await;
        for action in operation.actions {
            match action {
                AliasOperations::CreateAlias(CreateAliasOperation {
                    create_alias:
                        CreateAlias {
                            collection_name,
                            alias_name,
                        },
                }) => {
                    collection_lock
                        .validate_collection_exists(&collection_name)
                        .await?;
                    collection_lock
                        .validate_collection_not_exists(&alias_name)
                        .await?;

                    alias_lock.insert(alias_name, collection_name)?;
                }
                AliasOperations::DeleteAlias(DeleteAliasOperation {
                    delete_alias: DeleteAlias { alias_name },
                }) => {
                    alias_lock.remove(&alias_name)?;
                }
                AliasOperations::RenameAlias(RenameAliasOperation {
                    rename_alias:
                        RenameAlias {
                            old_alias_name,
                            new_alias_name,
                        },
                }) => {
                    alias_lock.rename_alias(&old_alias_name, new_alias_name)?;
                }
            };
        }
        Ok(true)
    }

    async fn handle_transfer(
        &self,
        collection_id: CollectionId,
        transfer_operation: ShardTransferOperations,
    ) -> Result<(), StorageError> {
        let collection = self.get_collection(&collection_id).await?;
        let proposal_sender = if let Some(proposal_sender) = self.consensus_proposal_sender.clone()
        {
            proposal_sender
        } else {
            return Err(StorageError::service_error(
                "Can't handle transfer, this is a single node deployment",
            ));
        };

        match transfer_operation {
            ShardTransferOperations::Start(transfer) => {
                let collection_state::State {
                    config: _,
                    shards,
                    transfers,
                    shards_key_mapping: _,
                    payload_index_schema: _,
                } = collection.state().await;
                let all_peers: HashSet<_> = self
                    .channel_service
                    .id_to_address
                    .read()
                    .keys()
                    .cloned()
                    .collect();
                let shard_state = shards.get(&transfer.shard_id).map(|info| &info.replicas);

                // Valid transfer:
                // All peers: 123, 321, 111, 222, 333
                // Peers: shard_id=1 - [{123: Active}]
                // Transfer: {123 -> 321}, shard_id=1

                // Invalid transfer:
                // All peers: 123, 321, 111, 222, 333
                // Peers: shard_id=1 - [{123: Active}]
                // Transfer: {321 -> 123}, shard_id=1

                transfer::helpers::validate_transfer(
                    &transfer,
                    &all_peers,
                    shard_state,
                    &transfers,
                )?;

                let on_finish = {
                    let collection_id = collection_id.clone();
                    let transfer = transfer.clone();
                    let proposal_sender = proposal_sender.clone();
                    async move {
                        let operation =
                            ConsensusOperations::finish_transfer(collection_id, transfer);

                        if let Err(error) = proposal_sender.send(operation) {
                            log::error!("Can't report transfer progress to consensus: {}", error)
                        };
                    }
                };

                let on_failure = {
                    let collection_id = collection_id.clone();
                    let transfer = transfer.clone();
                    async move {
                        if let Err(error) =
                            proposal_sender.send(ConsensusOperations::abort_transfer(
                                collection_id,
                                transfer,
                                "transmission failed",
                            ))
                        {
                            log::error!("Can't report transfer progress to consensus: {}", error)
                        };
                    }
                };

                let shard_consensus = match self.shard_transfer_dispatcher.lock().as_ref() {
                    Some(consensus) => Box::new(consensus.clone()),
                    None => {
                        return Err(StorageError::service_error(
                            "Can't handle transfer, this is a single node deployment",
                        ))
                    }
                };

                let temp_dir = self.optional_temp_or_storage_temp_path()?;
                collection
                    .start_shard_transfer(
                        transfer.clone(),
                        shard_consensus,
                        temp_dir,
                        on_finish,
                        on_failure,
                    )
                    .await?;
            }
            ShardTransferOperations::Finish(transfer) => {
                // Validate transfer exists to prevent double handling
                transfer::helpers::validate_transfer_exists(
                    &transfer.key(),
                    &collection.state().await.transfers,
                )?;
                collection.finish_shard_transfer(transfer).await?;
            }
            ShardTransferOperations::SnapshotRecovered(transfer) => {
                // Validate transfer exists to prevent double handling
                transfer::helpers::validate_transfer_exists(
                    &transfer,
                    &collection.state().await.transfers,
                )?;

                // Set shard state from `PartialSnapshot` to `Partial`
                let operation = SetShardReplicaState {
                    collection_name: collection_id,
                    shard_id: transfer.shard_id,
                    peer_id: transfer.to,
                    state: ReplicaState::Partial,
                    from_state: Some(ReplicaState::PartialSnapshot),
                };
                log::debug!(
                    "Set shard replica state from {:?} to {:?} after snapshot recovery",
                    ReplicaState::PartialSnapshot,
                    ReplicaState::Partial,
                );
                self.set_shard_replica_state(operation).await?;
            }
            ShardTransferOperations::Abort { transfer, reason } => {
                // Validate transfer exists to prevent double handling
                transfer::helpers::validate_transfer_exists(
                    &transfer,
                    &collection.state().await.transfers,
                )?;
                log::warn!("Aborting shard transfer: {reason}");
                collection.abort_shard_transfer(transfer).await?;
            }
        };
        Ok(())
    }

    async fn set_shard_replica_state(
        &self,
        operation: SetShardReplicaState,
    ) -> Result<(), StorageError> {
        self.get_collection(&operation.collection_name)
            .await?
            .set_shard_replica_state(
                operation.shard_id,
                operation.peer_id,
                operation.state,
                operation.from_state,
            )
            .await?;
        Ok(())
    }

    async fn create_shard_key(&self, operation: CreateShardKey) -> Result<(), StorageError> {
        self.get_collection(&operation.collection_name)
            .await?
            .create_shard_key(operation.shard_key, operation.placement)
            .await?;
        Ok(())
    }

    async fn drop_shard_key(&self, operation: DropShardKey) -> Result<(), StorageError> {
        self.get_collection(&operation.collection_name)
            .await?
            .drop_shard_key(operation.shard_key)
            .await?;
        Ok(())
    }

    async fn create_payload_index(
        &self,
        operation: CreatePayloadIndex,
    ) -> Result<(), StorageError> {
        self.get_collection(&operation.collection_name)
            .await?
            .create_payload_index(operation.field_name, operation.field_schema)
            .await?;
        Ok(())
    }

    async fn drop_payload_index(&self, operation: DropPayloadIndex) -> Result<(), StorageError> {
        self.get_collection(&operation.collection_name)
            .await?
            .drop_payload_index(operation.field_name)
            .await?;
        Ok(())
    }
}
