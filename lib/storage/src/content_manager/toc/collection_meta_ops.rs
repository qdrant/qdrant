use std::collections::HashSet;
use std::path::Path;

use collection::collection_state;
use collection::config::ShardingMethod;
use collection::events::{CollectionDeletedEvent, IndexCreatedEvent};
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use collection::shards::transfer::ShardTransfer;
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
                        ShardingMethod::Auto => {
                            let shard_number =
                                operation.create_collection.shard_number.or_else(|| {
                                    self.storage_config
                                        .collection
                                        .as_ref()
                                        .map(|i| i.shard_number_per_node)
                                });
                            CollectionShardDistribution::all_local(shard_number, self.this_peer_id)
                        }
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
            CollectionMetaOperations::Resharding(collection, operation) => {
                log::debug!("Resharding {operation:?} of {collection}");

                self.handle_resharding(collection, operation)
                    .await
                    .map(|_| true)
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
        let collection = self
            .get_collection_unchecked(&operation.collection_name)
            .await?;
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
        let _collection_create_guard = self.collection_create_lock.lock().await;
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

            // Solve all issues related to this collection
            issues::publish(CollectionDeletedEvent {
                collection_id: collection_name.to_string(),
            });

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
            // we hold the collection_create lock to make sure no one is creating this collection
            // otherwise we would delete its content now
            let path = self.get_collection_path(collection_name);
            if path.exists() {
                log::warn!(
                    "Collection {} is not loaded, but its directory still exists. Deleting it.",
                    collection_name
                );
                tokio::fs::remove_dir_all(path).await?;
            }
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

    async fn handle_resharding(
        &self,
        collection_id: CollectionId,
        operation: ReshardingOperation,
    ) -> Result<(), StorageError> {
        let collection = self.get_collection_unchecked(&collection_id).await?;
        let proposal_sender = if let Some(proposal_sender) = self.consensus_proposal_sender.clone()
        {
            proposal_sender
        } else {
            return Err(StorageError::service_error(
                "Can't handle resharding, this is a single node deployment",
            ));
        };

        match operation {
            ReshardingOperation::Start(key) => {
                let consensus = match self.shard_transfer_dispatcher.lock().as_ref() {
                    Some(consensus) => Box::new(consensus.clone()),
                    None => {
                        return Err(StorageError::service_error(
                            "Can't handle transfer, this is a single node deployment",
                        ))
                    }
                };

                let on_finish = {
                    let collection_id = collection_id.clone();
                    let key = key.clone();
                    let proposal_sender = proposal_sender.clone();
                    async move {
                        let operation = ConsensusOperations::finish_resharding(collection_id, key);
                        if let Err(error) = proposal_sender.send(operation) {
                            log::error!("Can't report resharding progress to consensus: {error}");
                        };
                    }
                };

                let on_failure = {
                    let collection_id = collection_id.clone();
                    let key = key.clone();
                    async move {
                        if let Err(error) = proposal_sender
                            .send(ConsensusOperations::abort_resharding(collection_id, key))
                        {
                            log::error!("Can't report resharding progress to consensus: {error}");
                        };
                    }
                };

                let temp_dir = self.optional_temp_or_storage_temp_path()?;
                collection
                    .start_resharding(key, consensus, temp_dir, on_finish, on_failure)
                    .await?;
            }

            ReshardingOperation::Abort(key) => {
                collection.abort_resharding(key).await?;
            }
        }

        Ok(())
    }

    async fn handle_transfer(
        &self,
        collection_id: CollectionId,
        transfer_operation: ShardTransferOperations,
    ) -> Result<(), StorageError> {
        let collection = self.get_collection_unchecked(&collection_id).await?;
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
                    shards,
                    transfers,
                    shards_key_mapping,
                    ..
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
                    &shards_key_mapping,
                )?;

                let on_finish = {
                    let collection_id = collection_id.clone();
                    let transfer = transfer.clone();
                    let proposal_sender = proposal_sender.clone();
                    async move {
                        let operation =
                            ConsensusOperations::finish_transfer(collection_id, transfer);

                        if let Err(error) = proposal_sender.send(operation) {
                            log::error!("Can't report transfer progress to consensus: {error}");
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
                            log::error!("Can't report transfer progress to consensus: {error}");
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
                        transfer,
                        shard_consensus,
                        temp_dir,
                        on_finish,
                        on_failure,
                    )
                    .await?;
            }
            ShardTransferOperations::Restart(transfer_restart) => {
                let transfers: HashSet<transfer::ShardTransfer> =
                    collection.state().await.transfers;

                let transfer_key = transfer_restart.key();

                let Some(old_transfer) = transfer::helpers::get_transfer(&transfer_key, &transfers)
                else {
                    return Err(StorageError::bad_request(format!(
                        "There is no transfer for shard {} from {} to {}",
                        transfer_key.shard_id, transfer_key.from, transfer_key.to,
                    )));
                };

                if old_transfer.method == Some(transfer_restart.method) {
                    return Err(StorageError::bad_request(format!(
                        "Cannot restart transfer for shard {} from {} to {}, its configuration did not change",
                        transfer_restart.shard_id, transfer_restart.from, transfer_restart.to,
                    )));
                }

                // Abort and start transfer
                Box::pin(self.handle_transfer(
                    collection_id.clone(),
                    ShardTransferOperations::Abort {
                        transfer: transfer_restart.key(),
                        reason: "restart transfer".into(),
                    },
                ))
                .await?;

                let new_transfer = ShardTransfer {
                    shard_id: transfer_restart.shard_id,
                    from: transfer_restart.from,
                    to: transfer_restart.to,
                    sync: old_transfer.sync, // Preserve sync flag from the old transfer
                    method: Some(transfer_restart.method),
                    to_shard_id: None,
                };

                Box::pin(
                    self.handle_transfer(
                        collection_id,
                        ShardTransferOperations::Start(new_transfer),
                    ),
                )
                .await?;
            }
            ShardTransferOperations::Finish(transfer) => {
                // Validate transfer exists to prevent double handling
                transfer::helpers::validate_transfer_exists(
                    &transfer.key(),
                    &collection.state().await.transfers,
                )?;
                collection.finish_shard_transfer(transfer, None).await?;
            }
            ShardTransferOperations::RecoveryToPartial(transfer)
            | ShardTransferOperations::SnapshotRecovered(transfer) => {
                // Validate transfer exists to prevent double handling
                transfer::helpers::validate_transfer_exists(
                    &transfer,
                    &collection.state().await.transfers,
                )?;

                let collection = self.get_collection_unchecked(&collection_id).await?;

                let current_state = collection
                    .state()
                    .await
                    .shards
                    .get(&transfer.shard_id)
                    .and_then(|info| info.replicas.get(&transfer.to))
                    .copied();

                let Some(current_state) = current_state else {
                    return Err(StorageError::bad_input(format!(
                        "Replica {} of {collection_id}:{} does not exist",
                        transfer.to, transfer.shard_id,
                    )));
                };

                match current_state {
                    ReplicaState::PartialSnapshot | ReplicaState::Recovery => (),
                    _ => {
                        return Err(StorageError::bad_input(format!(
                            "Replica {} of {collection_id}:{} has unexpected {current_state:?} \
                             (expected {:?} or {:?})",
                            transfer.to,
                            transfer.shard_id,
                            ReplicaState::PartialSnapshot,
                            ReplicaState::Recovery,
                        )))
                    }
                }

                log::debug!(
                    "Set shard replica state from {current_state:?} to {:?} after snapshot recovery",
                    ReplicaState::Partial,
                );

                collection
                    .set_shard_replica_state(
                        transfer.shard_id,
                        transfer.to,
                        ReplicaState::Partial,
                        Some(current_state),
                    )
                    .await?;
            }
            ShardTransferOperations::Abort { transfer, reason } => {
                // Validate transfer exists to prevent double handling
                transfer::helpers::validate_transfer_exists(
                    &transfer,
                    &collection.state().await.transfers,
                )?;
                log::warn!("Aborting shard transfer: {reason}");
                collection.abort_shard_transfer(transfer, None).await?;
            }
        };
        Ok(())
    }

    async fn set_shard_replica_state(
        &self,
        operation: SetShardReplicaState,
    ) -> Result<(), StorageError> {
        self.get_collection_unchecked(&operation.collection_name)
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
        self.get_collection_unchecked(&operation.collection_name)
            .await?
            .create_shard_key(operation.shard_key, operation.placement)
            .await?;
        Ok(())
    }

    async fn drop_shard_key(&self, operation: DropShardKey) -> Result<(), StorageError> {
        self.get_collection_unchecked(&operation.collection_name)
            .await?
            .drop_shard_key(operation.shard_key)
            .await?;
        Ok(())
    }

    async fn create_payload_index(
        &self,
        operation: CreatePayloadIndex,
    ) -> Result<(), StorageError> {
        self.get_collection_unchecked(&operation.collection_name)
            .await?
            .create_payload_index(operation.field_name.clone(), operation.field_schema)
            .await?;

        // We can solve issues related to this missing index
        issues::publish(IndexCreatedEvent {
            collection_id: operation.collection_name,
            field_name: operation.field_name,
        });

        Ok(())
    }

    async fn drop_payload_index(&self, operation: DropPayloadIndex) -> Result<(), StorageError> {
        self.get_collection_unchecked(&operation.collection_name)
            .await?
            .drop_payload_index(operation.field_name)
            .await?;
        Ok(())
    }
}
