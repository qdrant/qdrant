use std::num::NonZeroU32;

use collection::collection::Collection;
use collection::config::{self, CollectionConfigInternal, CollectionParams, ShardingMethod};
use collection::operations::config_diff::DiffConfig as _;
use collection::operations::types::{CollectionResult, VectorsConfig};
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};

use super::TableOfContent;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::collections_ops::Checker as _;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::errors::StorageError;

impl TableOfContent {
    pub(super) async fn create_collection(
        &self,
        collection_name: &str,
        operation: CreateCollection,
        collection_shard_distribution: CollectionShardDistribution,
    ) -> Result<bool, StorageError> {
        // Collection operations require multiple file operations,
        // before collection can actually be registered in the service.
        // To prevent parallel writing of the files, we use this lock.
        let collection_create_guard = self.collection_create_lock.lock().await;

        let CreateCollection {
            mut vectors,
            shard_number,
            sharding_method,
            on_disk_payload,
            hnsw_config: hnsw_config_diff,
            wal_config: wal_config_diff,
            optimizers_config: optimizers_config_diff,
            replication_factor,
            write_consistency_factor,
            quantization_config,
            sparse_vectors,
            strict_mode_config,
            uuid,
            metadata,
        } = operation;

        {
            let collections = self.collections.read().await;
            collections.validate_collection_not_exists(collection_name)?;

            if let Some(max_collections) = self.storage_config.max_collections
                && collections.len() >= max_collections
            {
                return Err(StorageError::bad_request(format!(
                    "Can't create collection with name {collection_name}. Max collections limit reached: {max_collections}",
                )));
            }
        }

        if self
            .alias_persistence
            .read()
            .await
            .check_alias_exists(collection_name)
        {
            return Err(StorageError::bad_input(format!(
                "Can't create collection with name {collection_name}. Alias with the same name already exists",
            )));
        }

        let collection_path = self.create_collection_path(collection_name).await?;
        // derive the snapshots path for the collection to be used across collection operation, the directories for the snapshot
        // is created only when a create snapshot api is invoked.
        let snapshots_path = self.snapshots_path_for_collection(collection_name);

        let collection_defaults_config = self.storage_config.collection.as_ref();

        let default_shard_number = collection_defaults_config
            .and_then(|x| x.shard_number)
            .unwrap_or_else(|| config::default_shard_number().get());

        let shard_number = match sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                if let Some(shard_number) = shard_number {
                    debug_assert_eq!(
                        shard_number as usize,
                        collection_shard_distribution.shard_count(),
                        "If shard number was supplied then this exact number should be used in a distribution",
                    );
                    shard_number
                } else {
                    collection_shard_distribution.shard_count() as u32
                }
            }
            ShardingMethod::Custom => {
                if let Some(shard_number) = shard_number {
                    shard_number
                } else {
                    default_shard_number
                }
            }
        };

        let replication_factor = replication_factor
            .or_else(|| collection_defaults_config.and_then(|i| i.replication_factor))
            .unwrap_or_else(|| config::default_replication_factor().get());

        let write_consistency_factor = write_consistency_factor
            .or_else(|| collection_defaults_config.and_then(|i| i.write_consistency_factor))
            .unwrap_or_else(|| config::default_write_consistency_factor().get());

        // Apply default vector config values if not set.
        let vectors_defaults = collection_defaults_config.and_then(|i| i.vectors.as_ref());
        if let Some(vectors_defaults) = vectors_defaults {
            match &mut vectors {
                VectorsConfig::Single(s) => {
                    if let Some(on_disk_default) = vectors_defaults.on_disk {
                        s.on_disk.get_or_insert(on_disk_default);
                    }
                }
                VectorsConfig::Multi(m) => {
                    for (_, vec_params) in m.iter_mut() {
                        if let Some(on_disk_default) = vectors_defaults.on_disk {
                            vec_params.on_disk.get_or_insert(on_disk_default);
                        }
                    }
                }
            };
        }

        let collection_params = CollectionParams {
            vectors,
            sparse_vectors,
            shard_number: NonZeroU32::new(shard_number)
                .ok_or_else(|| StorageError::bad_input("`shard_number` cannot be 0"))?,
            sharding_method,
            on_disk_payload: on_disk_payload.unwrap_or(self.storage_config.on_disk_payload),
            replication_factor: NonZeroU32::new(replication_factor).ok_or_else(|| {
                StorageError::BadInput {
                    description: "`replication_factor` cannot be 0".to_string(),
                }
            })?,
            write_consistency_factor: NonZeroU32::new(write_consistency_factor).ok_or_else(
                || StorageError::BadInput {
                    description: "`write_consistency_factor` cannot be 0".to_string(),
                },
            )?,
            read_fan_out_factor: None,
        };
        let wal_config = self.storage_config.wal.update_opt(wal_config_diff.as_ref());

        let optimizer_config = self
            .storage_config
            .optimizers
            .update_opt(optimizers_config_diff.as_ref());

        let hnsw_config = self
            .storage_config
            .hnsw_index
            .update_opt(hnsw_config_diff.as_ref());

        let quantization_config = match quantization_config {
            None => self
                .storage_config
                .collection
                .as_ref()
                .and_then(|i| i.quantization.clone()),
            Some(diff) => Some(diff),
        };

        let strict_mode_config = match strict_mode_config {
            Some(diff) => {
                let default_config = self
                    .storage_config
                    .collection
                    .as_ref()
                    .and_then(|i| i.strict_mode.clone())
                    .unwrap_or_default();
                Some(default_config.update(&diff))
            }
            None => self
                .storage_config
                .collection
                .as_ref()
                .and_then(|i| i.strict_mode.as_ref())
                .cloned(),
        };

        let storage_config = self
            .storage_config
            .to_shared_storage_config(self.is_distributed())
            .into();

        let collection_config = CollectionConfigInternal {
            wal_config,
            params: collection_params,
            optimizer_config,
            hnsw_config,
            quantization_config,
            strict_mode_config,
            uuid,
            metadata,
        };

        // No shard key mapping on creation, shard keys are set up after creating the collection
        let shard_key_mapping = None;

        let collection = Collection::new(
            collection_name.to_string(),
            self.this_peer_id,
            &collection_path,
            &snapshots_path,
            &collection_config,
            storage_config,
            collection_shard_distribution,
            shard_key_mapping,
            self.channel_service.clone(),
            Self::change_peer_from_state_callback(
                self.consensus_proposal_sender.clone(),
                collection_name.to_string(),
                ReplicaState::Dead,
            ),
            Self::request_shard_transfer_callback(
                self.consensus_proposal_sender.clone(),
                collection_name.to_string(),
            ),
            Self::abort_shard_transfer_callback(
                self.consensus_proposal_sender.clone(),
                collection_name.to_string(),
            ),
            Some(self.search_runtime.handle().clone()),
            Some(self.update_runtime.handle().clone()),
            self.optimizer_resource_budget.clone(),
            self.storage_config.optimizers_overwrite.clone(),
        )
        .await?;

        collection.print_warnings().await;

        let local_shards = collection.get_local_shards().await;

        {
            let mut write_collections = self.collections.write().await;
            write_collections.validate_collection_not_exists(collection_name)?;
            write_collections.insert(collection_name.to_string(), collection);
        }

        drop(collection_create_guard);

        // Notify the collection is created and ready to use
        for shard_id in local_shards {
            self.on_peer_created(collection_name.to_string(), self.this_peer_id, shard_id)
                .await?;
        }

        Ok(true)
    }

    async fn on_peer_created(
        &self,
        collection_name: String,
        peer_id: PeerId,
        shard_id: ShardId,
    ) -> CollectionResult<()> {
        if let Some(proposal_sender) = &self.consensus_proposal_sender {
            let operation =
                ConsensusOperations::initialize_replica(collection_name.clone(), shard_id, peer_id);
            if let Err(send_error) = proposal_sender.send(operation) {
                log::error!(
                    "Can't send proposal to deactivate replica on peer {peer_id} of shard {shard_id} of collection {collection_name}. Error: {send_error}",
                );
            }
        } else {
            // Just activate the shard
            let collections = self.collections.read().await;
            if let Some(collection) = collections.get(&collection_name) {
                collection
                    .set_shard_replica_state(
                        shard_id,
                        peer_id,
                        ReplicaState::Active,
                        Some(ReplicaState::Initializing),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
