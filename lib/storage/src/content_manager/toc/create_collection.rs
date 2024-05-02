use std::collections::BTreeMap;
use std::num::NonZeroU32;

use collection::collection::Collection;
use collection::config::{self, CollectionConfig, CollectionParams, ShardingMethod};
use collection::events::CollectionCreatedEvent;
use collection::operations::config_diff::DiffConfig as _;
use collection::operations::types::{
    check_sparse_compatible, CollectionResult, SparseVectorParams, VectorsConfig,
};
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::CollectionId;

use super::TableOfContent;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::collections_ops::Checker as _;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::data_transfer;
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
            init_from,
            quantization_config,
            sparse_vectors,
        } = operation;

        self.collections
            .read()
            .await
            .validate_collection_not_exists(collection_name)
            .await?;

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

        if let Some(init_from) = &init_from {
            self.check_collections_compatibility(&vectors, &sparse_vectors, &init_from.collection)
                .await?;
        }

        let collection_path = self.create_collection_path(collection_name).await?;
        let snapshots_path = self.create_snapshots_path(collection_name).await?;

        let collection_defaults_config = self.storage_config.collection.as_ref();

        let default_shard_number = collection_defaults_config
            .map(|x| x.shard_number)
            .unwrap_or_else(|| config::default_shard_number().get());

        let shard_number = match sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                if let Some(shard_number) = shard_number {
                    debug_assert_eq!(
                        shard_number as usize,
                        collection_shard_distribution.shard_count(),
                        "If shard number was supplied then this exact number should be used in a distribution"
                    );
                    shard_number
                } else {
                    collection_shard_distribution.shard_count() as u32
                }
            }
            ShardingMethod::Custom => {
                if init_from.is_some() {
                    return Err(StorageError::bad_input(
                        "Can't initialize collection from another collection with custom sharding method"
                    ));
                }
                if let Some(shard_number) = shard_number {
                    shard_number
                } else {
                    default_shard_number
                }
            }
        };

        let replication_factor = replication_factor
            .or_else(|| collection_defaults_config.map(|i| i.replication_factor))
            .unwrap_or_else(|| config::default_replication_factor().get());

        let write_consistency_factor = write_consistency_factor
            .or_else(|| collection_defaults_config.map(|i| i.write_consistency_factor))
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
            shard_number: NonZeroU32::new(shard_number).ok_or(StorageError::BadInput {
                description: "`shard_number` cannot be 0".to_string(),
            })?,
            sharding_method,
            on_disk_payload: on_disk_payload.unwrap_or(self.storage_config.on_disk_payload),
            replication_factor: NonZeroU32::new(replication_factor).ok_or(
                StorageError::BadInput {
                    description: "`replication_factor` cannot be 0".to_string(),
                },
            )?,
            write_consistency_factor: NonZeroU32::new(write_consistency_factor).ok_or(
                StorageError::BadInput {
                    description: "`write_consistency_factor` cannot be 0".to_string(),
                },
            )?,
            read_fan_out_factor: None,
        };
        let wal_config = match wal_config_diff {
            None => self.storage_config.wal.clone(),
            Some(diff) => diff.update(&self.storage_config.wal)?,
        };

        let optimizers_config = match optimizers_config_diff {
            None => self.storage_config.optimizers.clone(),
            Some(diff) => diff.update(&self.storage_config.optimizers)?,
        };

        let hnsw_config = match hnsw_config_diff {
            None => self.storage_config.hnsw_index.clone(),
            Some(diff) => diff.update(&self.storage_config.hnsw_index)?,
        };

        let quantization_config = match quantization_config {
            None => self
                .storage_config
                .collection
                .as_ref()
                .and_then(|i| i.quantization.clone()),
            Some(diff) => Some(diff),
        };

        let storage_config = self
            .storage_config
            .to_shared_storage_config(self.is_distributed())
            .into();

        let collection_config = CollectionConfig {
            wal_config,
            params: collection_params,
            optimizer_config: optimizers_config,
            hnsw_config,
            quantization_config,
        };
        let collection = Collection::new(
            collection_name.to_string(),
            self.this_peer_id,
            &collection_path,
            &snapshots_path,
            &collection_config,
            storage_config,
            collection_shard_distribution,
            self.channel_service.clone(),
            Self::change_peer_state_callback(
                self.consensus_proposal_sender.clone(),
                collection_name.to_string(),
                ReplicaState::Dead,
                None,
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
            self.optimizer_cpu_budget.clone(),
        )
        .await?;

        let local_shards = collection.get_local_shards().await;

        {
            let mut write_collections = self.collections.write().await;
            write_collections
                .validate_collection_not_exists(collection_name)
                .await?;
            write_collections.insert(collection_name.to_string(), collection);
        }

        drop(collection_create_guard);

        // Notify the collection is created and ready to use
        for shard_id in local_shards {
            self.on_peer_created(collection_name.to_string(), self.this_peer_id, shard_id)
                .await?;
        }

        issues::publish(CollectionCreatedEvent);

        if let Some(init_from) = init_from {
            self.run_data_initialization(init_from.collection, collection_name.to_string())
                .await;
        }

        Ok(true)
    }

    async fn check_collections_compatibility(
        &self,
        vectors: &VectorsConfig,
        sparse_vectors: &Option<BTreeMap<String, SparseVectorParams>>,
        source_collection: &CollectionId,
    ) -> Result<(), StorageError> {
        let collection = self.get_collection_unchecked(source_collection).await?;
        let collection_vectors_schema = collection.state().await.config.params.vectors;
        collection_vectors_schema.check_compatible(vectors)?;
        let collection_sparse_vectors_schema =
            collection.state().await.config.params.sparse_vectors;
        if let (Some(collection_sparse_vectors_schema), Some(sparse_vectors)) =
            (&collection_sparse_vectors_schema, sparse_vectors)
        {
            check_sparse_compatible(collection_sparse_vectors_schema, sparse_vectors)?;
        }
        Ok(())
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
                        "Can't send proposal to deactivate replica on peer {} of shard {} of collection {}. Error: {}",
                        peer_id,
                        shard_id,
                        collection_name,
                        send_error
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

    async fn run_data_initialization(
        &self,
        from_collection: CollectionId,
        to_collection: CollectionId,
    ) {
        let collections = self.collections.clone();
        let this_peer_id = self.this_peer_id;
        self.general_runtime.spawn(async move {
            // Create indexes
            match data_transfer::transfer_indexes(
                collections.clone(),
                &from_collection,
                &to_collection,
                this_peer_id,
            )
            .await
            {
                Ok(_) => {}
                Err(err) => {
                    log::error!("Initialization failed: {}", err)
                }
            }

            // Transfer data
            match data_transfer::populate_collection(
                collections,
                &from_collection,
                &to_collection,
                this_peer_id,
            )
            .await
            {
                Ok(_) => log::info!(
                    "Collection {} initialized with data from {}",
                    to_collection,
                    from_collection
                ),
                Err(err) => log::error!("Initialization failed: {}", err),
            }
        });
    }
}
