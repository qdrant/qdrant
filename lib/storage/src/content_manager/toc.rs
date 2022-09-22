use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, read_dir, remove_dir_all};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use collection::collection::Collection;
use collection::collection_state;
use collection::collection_state::ShardInfo;
use collection::config::{default_replication_factor, CollectionConfig, CollectionParams};
use collection::operations::config_diff::DiffConfig;
use collection::operations::snapshot_ops::SnapshotDescription;
use collection::operations::types::{
    CountRequest, CountResult, PointRequest, RecommendRequest, RecommendRequestBatch, Record,
    ScrollRequest, ScrollResult, SearchRequest, SearchRequestBatch, UpdateResult,
};
use collection::operations::CollectionUpdateOperations;
use collection::shard::collection_shard_distribution::CollectionShardDistribution;
use collection::shard::{replica_set, ChannelService, CollectionId, PeerId, ShardId};
use collection::telemetry::CollectionTelemetry;
use segment::types::ScoredPoint;
use tokio::runtime::Runtime;
use tokio::sync::{RwLock, RwLockReadGuard};

use super::collection_meta_ops::{
    CreateCollectionOperation, SetShardReplicaState, ShardTransferOperations,
};
use super::{consensus_state, CollectionContainer};
use crate::content_manager::alias_mapping::AliasPersistence;
use crate::content_manager::collection_meta_ops::{
    AliasOperations, ChangeAliasesOperation, CollectionMetaOperations, CreateAlias,
    CreateAliasOperation, CreateCollection, DeleteAlias, DeleteAliasOperation, RenameAlias,
    RenameAliasOperation, UpdateCollection,
};
use crate::content_manager::collections_ops::{Checker, Collections};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::errors::StorageError;
use crate::content_manager::shard_distribution::ShardDistributionProposal;
use crate::types::{PeerAddressById, StorageConfig};
use crate::ConsensusOperations;

pub const ALIASES_PATH: &str = "aliases";
pub const COLLECTIONS_DIR: &str = "collections";
pub const SNAPSHOTS_TMP_DIR: &str = "snapshots_tmp";
pub const FULL_SNAPSHOT_FILE_NAME: &str = "full-snapshot";

/// The main object of the service. It holds all objects, required for proper functioning.
/// In most cases only one `TableOfContent` is enough for service. It is created only once during
/// the launch of the service.
pub struct TableOfContent {
    collections: Arc<RwLock<Collections>>,
    storage_config: StorageConfig,
    search_runtime: Runtime,
    collection_management_runtime: Runtime,
    alias_persistence: RwLock<AliasPersistence>,
    pub this_peer_id: PeerId,
    channel_service: ChannelService,
    /// Backlink to the consensus
    consensus_proposal_sender: OperationSender,
}

impl TableOfContent {
    /// PeerId does not change during execution so it is ok to copy it here.
    pub fn new(
        storage_config: &StorageConfig,
        search_runtime: Runtime,
        channel_service: ChannelService,
        this_peer_id: PeerId,
        consensus_proposal_sender: OperationSender,
    ) -> Self {
        let snapshots_path = Path::new(&storage_config.snapshots_path.clone()).to_owned();
        create_dir_all(&snapshots_path).expect("Can't create Snapshots directory");
        let collections_path = Path::new(&storage_config.storage_path).join(&COLLECTIONS_DIR);
        let collection_management_runtime = Runtime::new().unwrap();
        create_dir_all(&collections_path).expect("Can't create Collections directory");
        let collection_paths =
            read_dir(&collections_path).expect("Can't read Collections directory");
        let mut collections: HashMap<String, Collection> = Default::default();
        for entry in collection_paths {
            let collection_path = entry
                .expect("Can't access of one of the collection files")
                .path();
            let collection_name = collection_path
                .file_name()
                .expect("Can't resolve a filename of one of the collection files")
                .to_str()
                .expect("A filename of one of the collection files is not a valid UTF-8")
                .to_string();
            let collection_snapshots_path =
                Self::collection_snapshots_path(&snapshots_path, &collection_name);
            create_dir_all(&collection_snapshots_path).unwrap_or_else(|e| {
                panic!(
                    "Can't create a directory for snapshot of {}: {}",
                    collection_name, e
                )
            });
            log::info!("Loading collection: {}", collection_name);
            let collection = collection_management_runtime.block_on(Collection::load(
                collection_name.clone(),
                &collection_path,
                &collection_snapshots_path,
                channel_service.clone(),
            ));

            collections.insert(collection_name, collection);
        }
        let alias_path = Path::new(&storage_config.storage_path).join(ALIASES_PATH);
        let alias_persistence =
            AliasPersistence::open(alias_path).expect("Can't open database by the provided config");
        TableOfContent {
            collections: Arc::new(RwLock::new(collections)),
            storage_config: storage_config.clone(),
            search_runtime,
            alias_persistence: RwLock::new(alias_persistence),
            collection_management_runtime,
            this_peer_id,
            channel_service,
            consensus_proposal_sender,
        }
    }

    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        Path::new(&self.storage_config.storage_path)
            .join(&COLLECTIONS_DIR)
            .join(collection_name)
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_config.storage_path
    }

    pub fn snapshots_path(&self) -> &str {
        &self.storage_config.snapshots_path
    }

    fn collection_snapshots_path(snapshots_path: &Path, collection_name: &str) -> PathBuf {
        snapshots_path.join(collection_name)
    }

    async fn create_snapshots_path(&self, collection_name: &str) -> Result<PathBuf, StorageError> {
        let snapshots_path = Self::collection_snapshots_path(
            Path::new(&self.storage_config.snapshots_path),
            collection_name,
        );
        tokio::fs::create_dir_all(&snapshots_path)
            .await
            .map_err(|err| StorageError::ServiceError {
                description: format!(
                    "Can't create directory for snapshots {}. Error: {}",
                    collection_name, err
                ),
            })?;

        Ok(snapshots_path)
    }

    async fn create_collection_path(&self, collection_name: &str) -> Result<PathBuf, StorageError> {
        let path = self.get_collection_path(collection_name);

        tokio::fs::create_dir_all(&path)
            .await
            .map_err(|err| StorageError::ServiceError {
                description: format!(
                    "Can't create directory for collection {}. Error: {}",
                    collection_name, err
                ),
            })?;

        Ok(path)
    }

    /// Finds the original name of the collection
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection or alias to resolve
    ///
    /// # Result
    ///
    /// If the collection exists - return its name
    /// If alias exists - returns the original collection name
    /// If neither exists - returns [`StorageError`]
    async fn resolve_name(&self, collection_name: &str) -> Result<String, StorageError> {
        let alias_collection_name = self.alias_persistence.read().await.get(collection_name);

        let resolved_name = match alias_collection_name {
            None => collection_name.to_string(),
            Some(resolved_alias) => resolved_alias,
        };
        self.collections
            .read()
            .await
            .validate_collection_exists(&resolved_name)
            .await?;
        Ok(resolved_name)
    }

    async fn create_collection(
        &self,
        collection_name: &str,
        operation: CreateCollection,
        collection_shard_distribution: CollectionShardDistribution,
    ) -> Result<bool, StorageError> {
        let CreateCollection {
            vectors,
            shard_number,
            on_disk_payload,
            hnsw_config: hnsw_config_diff,
            wal_config: wal_config_diff,
            optimizers_config: optimizers_config_diff,
            replication_factor,
        } = operation;

        self.collections
            .read()
            .await
            .validate_collection_not_exists(collection_name)
            .await?;

        let collection_path = self.create_collection_path(collection_name).await?;
        let snapshots_path = self.create_snapshots_path(collection_name).await?;

        if let Some(shard_number) = shard_number {
            debug_assert_eq!(
                shard_number as usize,
                collection_shard_distribution.shard_count(),
                "If shard number was supplied then this exact number should be used in a distribution"
            )
        }
        let replication_factor =
            replication_factor.unwrap_or_else(|| default_replication_factor().get());

        let collection_params = CollectionParams {
            vectors,
            shard_number: NonZeroU32::new(collection_shard_distribution.shard_count() as u32)
                .ok_or(StorageError::BadInput {
                    description: "`shard_number` cannot be 0".to_string(),
                })?,
            on_disk_payload: on_disk_payload.unwrap_or(self.storage_config.on_disk_payload),
            replication_factor: NonZeroU32::new(replication_factor).ok_or(
                StorageError::BadInput {
                    description: "`replication_factor` cannot be 0".to_string(),
                },
            )?,
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
            None => self.storage_config.hnsw_index,
            Some(diff) => diff.update(&self.storage_config.hnsw_index)?,
        };

        let collection_config = CollectionConfig {
            wal_config,
            params: collection_params,
            optimizer_config: optimizers_config,
            hnsw_config,
        };
        let collection = Collection::new(
            collection_name.to_string(),
            self.this_peer_id,
            &collection_path,
            &snapshots_path,
            &collection_config,
            collection_shard_distribution,
            self.channel_service.clone(),
            self.on_peer_failure_callback(collection_name.to_string()),
        )
        .await?;

        let mut write_collections = self.collections.write().await;
        write_collections
            .validate_collection_not_exists(collection_name)
            .await?;
        write_collections.insert(collection_name.to_string(), collection);
        Ok(true)
    }

    fn on_peer_failure_callback(&self, collection_name: String) -> replica_set::OnPeerFailure {
        let proposal_sender = self.consensus_proposal_sender.clone();
        Arc::new(move |peer_id, shard_id| {
            let proposal_sender = proposal_sender.clone();
            let collection_name = collection_name.clone();
            Box::new(async move {
                proposal_sender
                    .send(ConsensusOperations::CollectionMeta(
                        CollectionMetaOperations::SetShardReplicaState(SetShardReplicaState {
                            collection_name,
                            shard_id,
                            peer_id,
                            active: false,
                        })
                        .into(),
                    ))
                    .unwrap();
            })
        })
    }

    async fn update_collection(
        &self,
        collection_name: &str,
        operation: UpdateCollection,
    ) -> Result<bool, StorageError> {
        let UpdateCollection {
            optimizers_config,
            params,
        } = operation;
        let collection = self.get_collection(collection_name).await?;
        if let Some(diff) = optimizers_config {
            collection.update_optimizer_params_from_diff(diff).await?
        }
        if let Some(diff) = params {
            collection.update_params_from_diff(diff).await?;
        }
        Ok(true)
    }

    async fn delete_collection(&self, collection_name: &str) -> Result<bool, StorageError> {
        if let Some(mut removed) = self.collections.write().await.remove(collection_name) {
            removed.before_drop().await;
            let path = self.get_collection_path(collection_name);
            drop(removed);
            remove_dir_all(path).map_err(|err| StorageError::ServiceError {
                description: format!(
                    "Can't delete collection {}, error: {}",
                    collection_name, err
                ),
            })?;
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

    pub fn perform_collection_meta_op_sync(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        self.collection_management_runtime
            .block_on(self.perform_collection_meta_op(operation))
    }

    pub async fn perform_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        match operation {
            CollectionMetaOperations::CreateCollection(mut operation) => {
                let distribution = match operation.take_distribution() {
                    None => CollectionShardDistribution::all_local(
                        operation.create_collection.shard_number,
                    ),
                    Some(distribution) => distribution.into(self.this_peer_id),
                };
                self.create_collection(
                    &operation.collection_name,
                    operation.create_collection,
                    distribution,
                )
                .await
            }
            CollectionMetaOperations::UpdateCollection(operation) => {
                self.update_collection(&operation.collection_name, operation.update_collection)
                    .await
            }
            CollectionMetaOperations::DeleteCollection(operation) => {
                self.delete_collection(&operation.0).await
            }
            CollectionMetaOperations::ChangeAliases(operation) => {
                self.update_aliases(operation).await
            }
            CollectionMetaOperations::TransferShard(collection, operation) => self
                .handle_transfer(collection, operation)
                .await
                .map(|()| true),
            CollectionMetaOperations::SetShardReplicaState(operation) => {
                self.set_shard_replica_state(operation).await.map(|()| true)
            }
        }
    }

    pub async fn set_shard_replica_state(
        &self,
        operation: SetShardReplicaState,
    ) -> Result<(), StorageError> {
        self.get_collection(&operation.collection_name)
            .await?
            .set_shard_replica_state(operation.shard_id, operation.peer_id, operation.active)
            .await?;
        Ok(())
    }

    /// Cancels all transfers where the source peer is the current peer.
    pub async fn cancel_outgoing_all_transfers(&self, reason: &str) -> Result<(), StorageError> {
        let collections = self.collections.read().await;
        let proposal_sender = self.consensus_proposal_sender.clone();
        for collection in collections.values() {
            for transfer in collection.get_outgoing_transfers(&self.this_peer_id).await {
                let cancel_transfer =
                    ConsensusOperations::abort_transfer(collection.name(), transfer, reason);
                proposal_sender.send(cancel_transfer)?;
            }
        }
        Ok(())
    }

    pub async fn handle_transfer(
        &self,
        collection_id: CollectionId,
        transfer_operation: ShardTransferOperations,
    ) -> Result<(), StorageError> {
        let collection = self.get_collection(&collection_id).await?;
        match transfer_operation {
            ShardTransferOperations::Start(transfer) => {
                // check that transfer can be performed
                if self.this_peer_id == transfer.from
                    && !collection
                        .is_shard_local(&transfer.shard_id)
                        .await
                        .unwrap_or(false)
                {
                    let err = Err(StorageError::BadRequest {
                        description: format!(
                            "Shard {} not local on {} peer",
                            transfer.shard_id, self.this_peer_id
                        ),
                    });
                    self.consensus_proposal_sender
                        .send(ConsensusOperations::abort_transfer(
                            collection_id,
                            transfer,
                            "Bad source peer",
                        ))?;
                    return err;
                }

                let proposal_sender = self.consensus_proposal_sender.clone();
                let collection_id_clone = collection_id.clone();
                let transfer_clone = transfer.clone();

                let on_finish = async move {
                    let operation =
                        ConsensusOperations::finish_transfer(collection_id_clone, transfer_clone);

                    if let Err(error) = proposal_sender.send(operation) {
                        log::error!("Can't report transfer progress to consensus: {}", error)
                    };
                };

                let proposal_sender = self.consensus_proposal_sender.clone();
                let collection_id_clone = collection_id.clone();
                let transfer_clone = transfer.clone();

                let on_failure = async move {
                    if let Err(error) = proposal_sender.send(ConsensusOperations::abort_transfer(
                        collection_id_clone,
                        transfer_clone,
                        "transmission failed",
                    )) {
                        log::error!("Can't report transfer progress to consensus: {}", error)
                    };
                };

                collection
                    .start_shard_transfer(transfer, on_finish, on_failure)
                    .await
            }
            ShardTransferOperations::Finish(transfer) => {
                collection.finish_shard_transfer(transfer).await
            }
            ShardTransferOperations::Abort { transfer, reason } => {
                log::warn!("Aborting shard transfer: {reason}");
                collection.abort_shard_transfer(transfer).await
            }
        }?;
        Ok(())
    }

    pub async fn get_collection(
        &self,
        collection_name: &str,
    ) -> Result<RwLockReadGuard<Collection>, StorageError> {
        let read_collection = self.collections.read().await;
        let real_collection_name = self.resolve_name(collection_name).await?;
        // resolve_name already checked collection existence, unwrap is safe here
        Ok(RwLockReadGuard::map(read_collection, |collection| {
            collection.get(&real_collection_name).unwrap()
        }))
    }

    /// Initiate temporary shard.
    ///
    /// Fails if the collection does not exist
    pub async fn initiate_temporary_shard(
        &self,
        collection_name: String,
        shard_id: ShardId,
    ) -> Result<(), StorageError> {
        log::info!(
            "Initiating temporary shard {}:{}",
            collection_name,
            shard_id
        );
        let collection = self.get_collection(&collection_name).await?;
        collection.initiate_temporary_shard(shard_id).await?;
        Ok(())
    }

    /// Recommend points using positive and negative example from the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - for what collection do we recommend
    /// * `request` - [`RecommendRequest`]
    ///
    /// # Result
    ///
    /// Points with recommendation score
    pub async fn recommend(
        &self,
        collection_name: &str,
        request: RecommendRequest,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .recommend_by(request, self.search_runtime.handle(), shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// Recommend points in a batchi fashion using positive and negative example from the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - for what collection do we recommend
    /// * `request` - [`RecommendRequestBatch`]
    ///
    /// # Result
    ///
    /// Points with recommendation score
    pub async fn recommend_batch(
        &self,
        collection_name: &str,
        request: RecommendRequestBatch,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .recommend_batch_by(request, self.search_runtime.handle(), shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// Search for the closest points using vector similarity with given restrictions defined
    /// in the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we search
    /// * `request` - [`SearchRequest`]
    /// * `shard_selection` - which local shard to use
    /// # Result
    ///
    /// Points with search score
    pub async fn search(
        &self,
        collection_name: &str,
        request: SearchRequest,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search(request, self.search_runtime.handle(), shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// Search in a batching fashion for the closest points using vector similarity with given restrictions defined
    /// in the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we search
    /// * `request` - [`SearchRequestBatch`]
    /// * `shard_selection` - which local shard to use
    /// # Result
    ///
    /// Points with search score
    pub async fn search_batch(
        &self,
        collection_name: &str,
        request: SearchRequestBatch,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search_batch(request, self.search_runtime.handle(), shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// Count points in the collection.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we count
    /// * `request` - [`CountRequest`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// Number of points in the collection.
    ///
    pub async fn count(
        &self,
        collection_name: &str,
        request: CountRequest,
        shard_selection: Option<ShardId>,
    ) -> Result<CountResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .count(request, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// Return specific points by IDs
    ///
    /// # Arguments
    ///
    /// * `collection_name` - select from this collection
    /// * `request` - [`PointRequest`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn retrieve(
        &self,
        collection_name: &str,
        request: PointRequest,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<Record>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .retrieve(request, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// List of all collections
    pub async fn all_collections(&self) -> Vec<String> {
        self.collections.read().await.keys().cloned().collect()
    }

    /// List of all collections
    pub fn all_collections_sync(&self) -> Vec<String> {
        self.collection_management_runtime
            .block_on(self.collections.read())
            .keys()
            .cloned()
            .collect()
    }

    /// List of all aliases for a given collection
    pub async fn collection_aliases(
        &self,
        collection_name: &str,
    ) -> Result<Vec<String>, StorageError> {
        let result = self
            .alias_persistence
            .read()
            .await
            .collection_aliases(collection_name);
        Ok(result)
    }

    /// Paginate over all stored points with given filtering conditions
    ///
    /// # Arguments
    ///
    /// * `collection_name` - which collection to use
    /// * `request` - [`ScrollRequest`]
    /// * `shard_selection` - which local shard to use
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn scroll(
        &self,
        collection_name: &str,
        request: ScrollRequest,
        shard_selection: Option<ShardId>,
    ) -> Result<ScrollResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .scroll_by(request, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    pub async fn update(
        &self,
        collection_name: &str,
        operation: CollectionUpdateOperations,
        shard_selection: Option<ShardId>,
        wait: bool,
    ) -> Result<UpdateResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        let result = match shard_selection {
            Some(shard_selection) => {
                collection
                    .update_from_peer(operation, shard_selection, wait)
                    .await
            }
            None => collection.update_from_client(operation, wait).await,
        };
        result.map_err(|err| err.into())
    }

    fn this_peer_id(&self) -> PeerId {
        self.this_peer_id
    }

    pub fn peer_address_by_id(&self) -> PeerAddressById {
        self.channel_service.id_to_address.read().clone()
    }

    pub fn collections_snapshot_sync(&self) -> consensus_state::CollectionsSnapshot {
        self.collection_management_runtime
            .block_on(self.collections_snapshot())
    }

    pub async fn collections_snapshot(&self) -> consensus_state::CollectionsSnapshot {
        let mut collections: HashMap<CollectionId, collection_state::State> = HashMap::new();
        for (id, collection) in self.collections.read().await.iter() {
            collections.insert(id.clone(), collection.state(self.this_peer_id()).await);
        }
        consensus_state::CollectionsSnapshot {
            collections,
            aliases: self.alias_persistence.read().await.state().clone(),
        }
    }

    pub fn apply_collections_snapshot(
        &self,
        data: consensus_state::CollectionsSnapshot,
    ) -> Result<(), StorageError> {
        self.collection_management_runtime.block_on(async {
            let mut collections = self.collections.write().await;
            for (id, state) in &data.collections {
                let collection = collections.get(id);
                match collection {
                    // Update state if collection present locally
                    Some(collection) => {
                        if &collection.state(self.this_peer_id()).await != state {
                            let proposal_sender = self.consensus_proposal_sender.clone();
                            // In some cases on state application it might be needed to abort the transfer
                            let abort_transfer = |transfer| {
                                if let Err(error) =
                                    proposal_sender.send(ConsensusOperations::abort_transfer(
                                        id.clone(),
                                        transfer,
                                        "sender was not up to date",
                                    ))
                                {
                                    log::error!(
                                        "Can't report transfer progress to consensus: {}",
                                        error
                                    )
                                };
                            };
                            collection
                                .apply_state(state.clone(), self.this_peer_id(), abort_transfer)
                                .await?;
                        }
                    }
                    // Create collection if not present locally
                    None => {
                        let collection_path = self.create_collection_path(id).await?;
                        let snapshots_path = self.create_snapshots_path(id).await?;
                        let shard_distribution = CollectionShardDistribution::from_shards_info(
                            self.this_peer_id,
                            state.shards.clone(),
                        );
                        let collection = Collection::new(
                            id.to_string(),
                            self.this_peer_id,
                            &collection_path,
                            &snapshots_path,
                            &state.config,
                            shard_distribution,
                            self.channel_service.clone(),
                            self.on_peer_failure_callback(id.to_string()),
                        )
                        .await?;
                        collections.validate_collection_not_exists(id).await?;
                        collections.insert(id.to_string(), collection);
                    }
                }
            }

            // Remove collections that are present locally but are not in the snapshot state
            for collection_name in collections.keys() {
                if !data.collections.contains_key(collection_name) {
                    log::debug!(
                        "Deleting collection {} because it is not part of the consensus snapshot",
                        collection_name
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

    pub async fn create_snapshot(
        &self,
        collection_name: &str,
    ) -> Result<SnapshotDescription, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        // We want to use tmp dir inside the storage, because it is possible, that
        // snapshot directory is mounted as network share and multiple writes to it could be slow
        let tmp_dir = Path::new(&self.storage_config.storage_path).join(SNAPSHOTS_TMP_DIR);
        tokio::fs::create_dir_all(&tmp_dir).await?;
        Ok(collection.create_snapshot(&tmp_dir).await?)
    }

    pub async fn suggest_shard_distribution(
        &self,
        op: &CreateCollectionOperation,
        suggested_shard_number: NonZeroU32,
    ) -> ShardDistributionProposal {
        let shard_number = op
            .create_collection
            .shard_number
            .and_then(NonZeroU32::new)
            .unwrap_or(suggested_shard_number);
        let mut known_peers_set: HashSet<_> = self
            .channel_service
            .id_to_address
            .read()
            .keys()
            .copied()
            .collect();
        known_peers_set.insert(self.this_peer_id());
        let known_peers: Vec<_> = known_peers_set.into_iter().collect();
        let replication_factor = op
            .create_collection
            .replication_factor
            .and_then(NonZeroU32::new)
            .unwrap_or_else(default_replication_factor);

        let shard_distribution =
            ShardDistributionProposal::new(shard_number, replication_factor, &known_peers);

        log::debug!(
            "Suggesting distribution for {} shards for collection '{}' among {} peers {:?}",
            shard_number,
            op.collection_name,
            known_peers.len(),
            shard_distribution.distribution
        );
        shard_distribution
    }

    pub async fn get_telemetry_data(&self) -> Vec<CollectionTelemetry> {
        let mut result = Vec::new();
        let all_collections = self.all_collections().await;
        for collection_name in &all_collections {
            if let Ok(collection) = self.get_collection(collection_name).await {
                if let Some(collection) = collection.get_telemetry_data().await {
                    result.push(collection);
                }
            }
        }
        result
    }

    pub async fn peer_has_shards(&self, peer_id: PeerId) -> bool {
        for collection in self.collections.read().await.values() {
            let state = collection.state(self.this_peer_id()).await;
            let peers_with_shards: HashSet<_> = state
                .shards
                .into_values()
                .flat_map(|shard_info| match shard_info {
                    ShardInfo::ReplicaSet { replicas } => replicas.into_keys().collect::<Vec<_>>(),
                    ShardInfo::Single(peer_id) => vec![peer_id],
                })
                .collect();
            if peers_with_shards.contains(&peer_id) {
                return true;
            }
        }
        false
    }

    fn peer_has_shards_sync(&self, peer_id: PeerId) -> bool {
        self.collection_management_runtime
            .block_on(self.peer_has_shards(peer_id))
    }
}

impl CollectionContainer for TableOfContent {
    fn perform_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        self.perform_collection_meta_op_sync(operation)
    }

    fn collections_snapshot(&self) -> consensus_state::CollectionsSnapshot {
        self.collections_snapshot_sync()
    }

    fn apply_collections_snapshot(
        &self,
        data: consensus_state::CollectionsSnapshot,
    ) -> Result<(), StorageError> {
        self.apply_collections_snapshot(data)
    }

    fn peer_has_shards(&self, peer_id: PeerId) -> bool {
        self.peer_has_shards_sync(peer_id)
    }

    fn remove_peer(&self, peer_id: PeerId) {
        if self.this_peer_id == peer_id {
            // We are detaching the current peer, so we need to remove all connections
            // Remove all peers from the channel service
            self.collection_management_runtime.block_on(async {
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
            });
        } else {
            // Remove link to some other peer
            self.collection_management_runtime
                .block_on(self.channel_service.remove_peer(peer_id));
        }
    }
}

// `TableOfContent` should not be dropped from async context.
impl Drop for TableOfContent {
    fn drop(&mut self) {
        self.collection_management_runtime.block_on(async {
            for (_, mut collection) in self.collections.write().await.drain() {
                collection.before_drop().await;
            }
        });
    }
}
