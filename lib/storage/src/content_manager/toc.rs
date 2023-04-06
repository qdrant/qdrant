use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, read_dir};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use collection::collection::{Collection, RequestShardTransfer};
use collection::collection_state;
use collection::config::{
    default_replication_factor, default_write_consistency_factor, CollectionConfig,
    CollectionParams,
};
use collection::operations::config_diff::DiffConfig;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::point_ops::WriteOrdering;
use collection::operations::snapshot_ops::SnapshotDescription;
use collection::operations::types::{
    AliasDescription, CollectionResult, CountRequest, CountResult, PointRequest, RecommendRequest,
    RecommendRequestBatch, Record, ScrollRequest, ScrollResult, SearchRequest, SearchRequestBatch,
    UpdateResult, VectorsConfig,
};
use collection::operations::CollectionUpdateOperations;
use collection::recommendations::{recommend_batch_by, recommend_by};
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::shard_transfer::{
    validate_transfer, validate_transfer_exists, ShardTransfer,
};
use collection::shards::{replica_set, CollectionId};
use collection::telemetry::CollectionTelemetry;
use segment::types::ScoredPoint;
use tokio::runtime::Runtime;
use tokio::sync::{RwLock, RwLockReadGuard};
use uuid::Uuid;

use super::collection_meta_ops::{
    CreateCollectionOperation, SetShardReplicaState, ShardTransferOperations,
    UpdateCollectionOperation,
};
use super::{consensus_manager, CollectionContainer};
use crate::content_manager::alias_mapping::AliasPersistence;
use crate::content_manager::collection_meta_ops::{
    AliasOperations, ChangeAliasesOperation, CollectionMetaOperations, CreateAlias,
    CreateAliasOperation, CreateCollection, DeleteAlias, DeleteAliasOperation, RenameAlias,
    RenameAliasOperation, UpdateCollection,
};
use crate::content_manager::collections_ops::{Checker, Collections};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::data_transfer::{populate_collection, transfer_indexes};
use crate::content_manager::errors::StorageError;
use crate::content_manager::shard_distribution::ShardDistributionProposal;
use crate::types::{PeerAddressById, StorageConfig};
use crate::ConsensusOperations;

pub const ALIASES_PATH: &str = "aliases";
pub const COLLECTIONS_DIR: &str = "collections";
pub const SNAPSHOTS_TMP_DIR: &str = "snapshots_tmp";
pub const FULL_SNAPSHOT_FILE_NAME: &str = "full-snapshot";
pub const DEFAULT_WRITE_LOCK_ERROR_MESSAGE: &str = "Write operations are forbidden";

/// The main object of the service. It holds all objects, required for proper functioning.
/// In most cases only one `TableOfContent` is enough for service. It is created only once during
/// the launch of the service.
pub struct TableOfContent {
    collections: Arc<RwLock<Collections>>,
    storage_config: Arc<StorageConfig>,
    search_runtime: Runtime,
    update_runtime: Runtime,
    general_runtime: Runtime,
    alias_persistence: RwLock<AliasPersistence>,
    pub this_peer_id: PeerId,
    channel_service: ChannelService,
    /// Backlink to the consensus, if none - single node mode
    consensus_proposal_sender: Option<OperationSender>,
    is_write_locked: AtomicBool,
    lock_error_message: parking_lot::Mutex<Option<String>>,
}

impl TableOfContent {
    /// PeerId does not change during execution so it is ok to copy it here.
    pub fn new(
        storage_config: &StorageConfig,
        search_runtime: Runtime,
        update_runtime: Runtime,
        general_runtime: Runtime,
        channel_service: ChannelService,
        this_peer_id: PeerId,
        consensus_proposal_sender: Option<OperationSender>,
    ) -> Self {
        let snapshots_path = Path::new(&storage_config.snapshots_path.clone()).to_owned();
        create_dir_all(&snapshots_path).expect("Can't create Snapshots directory");
        let collections_path = Path::new(&storage_config.storage_path).join(COLLECTIONS_DIR);
        create_dir_all(&collections_path).expect("Can't create Collections directory");
        let collection_paths =
            read_dir(&collections_path).expect("Can't read Collections directory");
        let mut collections: HashMap<String, Collection> = Default::default();
        for entry in collection_paths {
            let collection_path = entry
                .expect("Can't access of one of the collection files")
                .path();

            if !CollectionConfig::check(&collection_path) {
                log::warn!(
                    "Collection config is not found in the collection directory: {:?}, skipping",
                    collection_path
                );
                continue;
            }

            let collection_name = collection_path
                .file_name()
                .expect("Can't resolve a filename of one of the collection files")
                .to_str()
                .expect("A filename of one of the collection files is not a valid UTF-8")
                .to_string();
            let collection_snapshots_path =
                Self::collection_snapshots_path(&snapshots_path, &collection_name);
            create_dir_all(&collection_snapshots_path).unwrap_or_else(|e| {
                panic!("Can't create a directory for snapshot of {collection_name}: {e}")
            });
            log::info!("Loading collection: {}", collection_name);
            let collection = general_runtime.block_on(Collection::load(
                collection_name.clone(),
                this_peer_id,
                &collection_path,
                &collection_snapshots_path,
                storage_config.to_shared_storage_config().into(),
                channel_service.clone(),
                Self::change_peer_state_callback(
                    consensus_proposal_sender.clone(),
                    collection_name.clone(),
                    ReplicaState::Dead,
                    None,
                ),
                Self::request_shard_transfer_callback(
                    consensus_proposal_sender.clone(),
                    collection_name.clone(),
                ),
                Some(search_runtime.handle().clone()),
                Some(update_runtime.handle().clone()),
            ));

            collections.insert(collection_name, collection);
        }
        let alias_path = Path::new(&storage_config.storage_path).join(ALIASES_PATH);
        let alias_persistence =
            AliasPersistence::open(alias_path).expect("Can't open database by the provided config");
        TableOfContent {
            collections: Arc::new(RwLock::new(collections)),
            storage_config: Arc::new(storage_config.clone()),
            search_runtime,
            update_runtime,
            general_runtime,
            alias_persistence: RwLock::new(alias_persistence),
            this_peer_id,
            channel_service,
            consensus_proposal_sender,
            is_write_locked: AtomicBool::new(false),
            lock_error_message: parking_lot::Mutex::new(None),
        }
    }

    /// Return `true` if service is working in distributed mode.
    pub fn is_distributed(&self) -> bool {
        self.consensus_proposal_sender.is_some()
    }

    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        Path::new(&self.storage_config.storage_path)
            .join(COLLECTIONS_DIR)
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
            .map_err(|err| {
                StorageError::service_error(format!(
                    "Can't create directory for snapshots {collection_name}. Error: {err}"
                ))
            })?;

        Ok(snapshots_path)
    }

    async fn create_collection_path(&self, collection_name: &str) -> Result<PathBuf, StorageError> {
        let path = self.get_collection_path(collection_name);

        tokio::fs::create_dir_all(&path).await.map_err(|err| {
            StorageError::service_error(format!(
                "Can't create directory for collection {collection_name}. Error: {err}"
            ))
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
            write_consistency_factor,
            init_from,
            quantization_config,
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
            return Err(StorageError::bad_input(&format!(
                "Can't create collection with name {collection_name}. Alias with the same name already exists",
            )));
        }

        if let Some(init_from) = &init_from {
            self.check_collections_compatibility(&vectors, &init_from.collection)
                .await?;
        }

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

        let write_consistency_factor =
            write_consistency_factor.unwrap_or_else(|| default_write_consistency_factor().get());

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
            write_consistency_factor: NonZeroU32::new(write_consistency_factor).ok_or(
                StorageError::BadInput {
                    description: "`write_consistency_factor` cannot be 0".to_string(),
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

        let quantization_config = match quantization_config {
            None => self.storage_config.quantization.clone(),
            Some(diff) => Some(diff),
        };

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
            self.storage_config.to_shared_storage_config().into(),
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
            Some(self.search_runtime.handle().clone()),
            Some(self.update_runtime.handle().clone()),
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

        // Notify the collection is created and ready to use
        for shard_id in local_shards {
            self.on_peer_created(collection_name.to_string(), self.this_peer_id, shard_id)
                .await?;
        }

        if let Some(init_from) = init_from {
            self.run_data_initialization(init_from.collection, collection_name.to_string())
                .await;
        }

        Ok(true)
    }

    async fn check_collections_compatibility(
        &self,
        vectors: &VectorsConfig,
        source_collection: &CollectionId,
    ) -> Result<(), StorageError> {
        let collection = self.get_collection(source_collection).await?;
        let collection_vectors_schema = collection.state().await.config.params.vectors;
        if &collection_vectors_schema != vectors {
            return Err(StorageError::BadInput {
                description: format!("Cannot take data from collection with vectors schema {collection_vectors_schema:?} to collection with vectors schema {vectors:?}")
            });
        }
        Ok(())
    }

    pub async fn run_data_initialization(
        &self,
        from_collection: CollectionId,
        to_collection: CollectionId,
    ) {
        let collections = self.collections.clone();
        let this_peer_id = self.this_peer_id;
        self.general_runtime.spawn(async move {
            // Create indexes
            match transfer_indexes(
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
            match populate_collection(collections, &from_collection, &to_collection, this_peer_id)
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

    fn send_set_replica_state_proposal_op(
        proposal_sender: &OperationSender,
        collection_name: String,
        peer_id: PeerId,
        shard_id: ShardId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> Result<(), StorageError> {
        let operation = ConsensusOperations::set_replica_state(
            collection_name,
            shard_id,
            peer_id,
            state,
            from_state,
        );
        proposal_sender.send(operation)
    }

    fn send_remove_replica_proposal_op(
        proposal_sender: &OperationSender,
        collection_name: String,
        peer_id: PeerId,
        shard_id: ShardId,
    ) -> Result<(), StorageError> {
        let operation = ConsensusOperations::remove_replica(collection_name, shard_id, peer_id);
        proposal_sender.send(operation)
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
                        "Can't send proposal to abort transfer of shard {} of collection {}. Error: {}",
                        transfer.shard_id,
                        collection_name,
                        send_error
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
                        "Can't send proposal to complete transfer of shard {} of collection {}. Error: {}",
                        transfer.shard_id,
                        collection_name,
                        send_error
                    );
                }
            }
        })
    }

    fn change_peer_state_callback(
        proposal_sender: Option<OperationSender>,
        collection_name: String,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> replica_set::ChangePeerState {
        Arc::new(move |peer_id, shard_id| {
            if let Some(proposal_sender) = &proposal_sender {
                if let Err(send_error) = Self::send_set_replica_state_proposal_op(
                    proposal_sender,
                    collection_name.clone(),
                    peer_id,
                    shard_id,
                    state,
                    from_state,
                ) {
                    log::error!(
                        "Can't send proposal to deactivate replica on peer {} of shard {} of collection {}. Error: {}",
                        peer_id,
                        shard_id,
                        collection_name,
                        send_error
                    );
                }
            } else {
                log::error!("Can't send proposal to deactivate replica. Error: this is a single node deployment");
            }
        })
    }

    pub fn request_snapshot(&self) -> Result<(), StorageError> {
        let sender = match &self.consensus_proposal_sender {
            Some(sender) => sender,
            None => {
                return Err(StorageError::service_error(
                    "Qdrant is running in standalone mode",
                ))
            }
        };

        sender.send(ConsensusOperations::request_snapshot())?;

        Ok(())
    }

    pub fn send_set_replica_state_proposal(
        &self,
        collection_name: String,
        peer_id: PeerId,
        shard_id: ShardId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> Result<(), StorageError> {
        if let Some(operation_sender) = &self.consensus_proposal_sender {
            Self::send_set_replica_state_proposal_op(
                operation_sender,
                collection_name,
                peer_id,
                shard_id,
                state,
                from_state,
            )?;
        }
        Ok(())
    }

    fn request_shard_transfer_callback(
        proposal_sender: Option<OperationSender>,
        collection_name: String,
    ) -> RequestShardTransfer {
        Arc::new(move |shard_transfer| {
            if let Some(proposal_sender) = &proposal_sender {
                let collection_name = collection_name.clone();
                let to_peer = shard_transfer.to;
                let operation =
                    ConsensusOperations::start_transfer(collection_name.clone(), shard_transfer);
                if let Err(send_error) = proposal_sender.send(operation) {
                    log::error!(
                        "Can't send proposal to request shard transfer to peer {} of collection {}. Error: {}",
                        to_peer,
                        collection_name,
                        send_error
                    );
                }
            } else {
                log::error!("Can't send proposal to request shard transfer. Error: this is a single node deployment");
            }
        })
    }

    pub fn request_shard_transfer(
        &self,
        collection_name: String,
        shard_id: ShardId,
        from_peer: PeerId,
        to_peer: PeerId,
        sync: bool,
    ) -> Result<(), StorageError> {
        if let Some(proposal_sender) = &self.consensus_proposal_sender {
            let transfer_request = ShardTransfer {
                shard_id,
                from: from_peer,
                to: to_peer,
                sync,
            };
            let operation = ConsensusOperations::start_transfer(collection_name, transfer_request);
            proposal_sender.send(operation)?;
        }
        Ok(())
    }

    pub fn request_remove_replica(
        &self,
        collection_name: String,
        shard_id: ShardId,
        peer_id: PeerId,
    ) -> Result<(), StorageError> {
        if let Some(proposal_sender) = &self.consensus_proposal_sender {
            Self::send_remove_replica_proposal_op(
                proposal_sender,
                collection_name,
                peer_id,
                shard_id,
            )?;
        }
        Ok(())
    }

    async fn update_collection(
        &self,
        mut operation: UpdateCollectionOperation,
    ) -> Result<bool, StorageError> {
        let replica_changes = operation.take_shard_replica_changes();
        let UpdateCollection {
            optimizers_config,
            params,
        } = operation.update_collection;
        let collection = self.get_collection(&operation.collection_name).await?;
        if let Some(diff) = optimizers_config {
            collection.update_optimizer_params_from_diff(diff).await?
        }
        if let Some(diff) = params {
            collection.update_params_from_diff(diff).await?;
        }
        if let Some(changes) = replica_changes {
            collection.handle_replica_changes(changes).await?;
        }
        Ok(true)
    }

    async fn delete_collection(&self, collection_name: &str) -> Result<bool, StorageError> {
        if let Some(mut removed) = self.collections.write().await.remove(collection_name) {
            removed.before_drop().await;

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

    pub fn perform_collection_meta_op_sync(
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
                log::debug!("Creating collection {}", operation.collection_name);
                let distribution = match operation.take_distribution() {
                    None => CollectionShardDistribution::all_local(
                        operation.create_collection.shard_number,
                        self.this_peer_id,
                    ),
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
                log::debug!("Updating collection {}", operation.collection_name);
                self.update_collection(operation).await
            }
            CollectionMetaOperations::DeleteCollection(operation) => {
                log::debug!("Deleting collection {}", operation.0);
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
        }
    }

    pub async fn set_shard_replica_state(
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

    /// Cancels all transfers where the source peer is the current peer.
    pub async fn cancel_outgoing_all_transfers(&self, reason: &str) -> Result<(), StorageError> {
        let collections = self.collections.read().await;
        if let Some(proposal_sender) = &self.consensus_proposal_sender {
            for collection in collections.values() {
                for transfer in collection.get_outgoing_transfers(&self.this_peer_id).await {
                    let cancel_transfer =
                        ConsensusOperations::abort_transfer(collection.name(), transfer, reason);
                    proposal_sender.send(cancel_transfer)?;
                }
            }
        } else {
            log::error!("Can't cancel outgoing transfers, this is a single node deployment");
        }
        Ok(())
    }

    pub async fn handle_transfer(
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

                validate_transfer(&transfer, &all_peers, shard_state, &transfers)?;

                let collection_id_clone = collection_id.clone();
                let transfer_clone = transfer.clone();

                let on_finish_sender = proposal_sender.clone();
                let on_finish = async move {
                    let operation =
                        ConsensusOperations::finish_transfer(collection_id_clone, transfer_clone);

                    if let Err(error) = on_finish_sender.send(operation) {
                        log::error!("Can't report transfer progress to consensus: {}", error)
                    };
                };

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
                    .await?;
            }
            ShardTransferOperations::Finish(transfer) => {
                // Validate transfer exists to prevent double handling
                validate_transfer_exists(&transfer.key(), &collection.state().await.transfers)?;
                collection.finish_shard_transfer(transfer).await?;
            }
            ShardTransferOperations::Abort { transfer, reason } => {
                // Validate transfer exists to prevent double handling
                validate_transfer_exists(&transfer, &collection.state().await.transfers)?;
                log::warn!("Aborting shard transfer: {reason}");
                collection.abort_shard_transfer(transfer).await?;
            }
        };
        Ok(())
    }

    async fn get_collection_opt(
        &self,
        collection_name: String,
    ) -> Option<RwLockReadGuard<Collection>> {
        self.get_collection(&collection_name).await.ok()
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

    /// Initiate receiving shard.
    ///
    /// Fails if the collection does not exist
    pub async fn initiate_receiving_shard(
        &self,
        collection_name: String,
        shard_id: ShardId,
    ) -> Result<(), StorageError> {
        log::info!(
            "Initiating receiving shard {}:{}",
            collection_name,
            shard_id
        );
        let collection = self.get_collection(&collection_name).await?;
        collection.initiate_local_partial_shard(shard_id).await?;
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
        read_consistency: Option<ReadConsistency>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        recommend_by(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
        )
        .await
        .map_err(|err| err.into())
    }

    /// Recommend points in a batchig fashion using positive and negative example from the request
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
        read_consistency: Option<ReadConsistency>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        recommend_batch_by(
            request,
            &collection,
            |name| self.get_collection_opt(name),
            read_consistency,
        )
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
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search(request, read_consistency, shard_selection)
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
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search_batch(request, read_consistency, shard_selection)
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
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<Record>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .retrieve(request, read_consistency, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    /// List of all collections
    pub async fn all_collections(&self) -> Vec<String> {
        self.collections.read().await.keys().cloned().collect()
    }

    /// List of all collections
    pub fn all_collections_sync(&self) -> Vec<String> {
        self.general_runtime
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

    /// List of all aliases across all collections
    pub async fn list_aliases(&self) -> Result<Vec<AliasDescription>, StorageError> {
        let all_collections = self.all_collections().await;
        let mut aliases: Vec<AliasDescription> = Default::default();
        for collection_name in &all_collections {
            for alias in self.collection_aliases(collection_name).await? {
                aliases.push(AliasDescription {
                    alias_name: alias.to_string(),
                    collection_name: collection_name.to_string(),
                });
            }
        }

        Ok(aliases)
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
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> Result<ScrollResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .scroll_by(request, read_consistency, shard_selection)
            .await
            .map_err(|err| err.into())
    }

    pub async fn update(
        &self,
        collection_name: &str,
        operation: CollectionUpdateOperations,
        shard_selection: Option<ShardId>,
        wait: bool,
        ordering: WriteOrdering,
    ) -> Result<UpdateResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        let result = match shard_selection {
            Some(shard_selection) => {
                collection
                    .update_from_peer(operation, shard_selection, wait)
                    .await
            }
            None => {
                if operation.is_write_operation() {
                    self.check_write_lock()?;
                }
                collection
                    .update_from_client(operation, wait, ordering)
                    .await
            }
        };
        result.map_err(|err| err.into())
    }

    fn this_peer_id(&self) -> PeerId {
        self.this_peer_id
    }

    pub fn peer_address_by_id(&self) -> PeerAddressById {
        self.channel_service.id_to_address.read().clone()
    }

    pub fn collections_snapshot_sync(&self) -> consensus_manager::CollectionsSnapshot {
        self.general_runtime.block_on(self.collections_snapshot())
    }

    pub async fn collections_snapshot(&self) -> consensus_manager::CollectionsSnapshot {
        let mut collections: HashMap<CollectionId, collection_state::State> = HashMap::new();
        for (id, collection) in self.collections.read().await.iter() {
            collections.insert(id.clone(), collection.state().await);
        }
        consensus_manager::CollectionsSnapshot {
            collections,
            aliases: self.alias_persistence.read().await.state().clone(),
        }
    }

    pub fn apply_collections_snapshot(
        &self,
        data: consensus_manager::CollectionsSnapshot,
    ) -> Result<(), StorageError> {
        self.general_runtime.block_on(async {
            let mut collections = self.collections.write().await;

            for (id, state) in &data.collections {
                let collection_exists = collections.contains_key(id);

                // Create collection if not present locally
                if !collection_exists {
                    let collection_path = self.create_collection_path(id).await?;
                    let snapshots_path = self.create_snapshots_path(id).await?;
                    let shard_distribution =
                        CollectionShardDistribution::from_shards_info(state.shards.clone());
                    let collection = Collection::new(
                        id.to_string(),
                        self.this_peer_id,
                        &collection_path,
                        &snapshots_path,
                        &state.config,
                        self.storage_config.to_shared_storage_config().into(),
                        shard_distribution,
                        self.channel_service.clone(),
                        Self::change_peer_state_callback(
                            self.consensus_proposal_sender.clone(),
                            id.to_string(),
                            ReplicaState::Dead,
                            None,
                        ),
                        Self::request_shard_transfer_callback(
                            self.consensus_proposal_sender.clone(),
                            id.to_string(),
                        ),
                        Some(self.search_runtime.handle().clone()),
                        Some(self.update_runtime.handle().clone()),
                    )
                    .await?;
                    collections.validate_collection_not_exists(id).await?;
                    collections.insert(id.to_string(), collection);
                }

                let collection = match collections.get(id) {
                    Some(collection) => collection,
                    None => unreachable!(),
                };

                // Update collection state
                if &collection.state().await != state {
                    if let Some(proposal_sender) = self.consensus_proposal_sender.clone() {
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
                    } else {
                        log::error!("Can't apply state: single node mode");
                    }
                }

                // Mark local shards as dead (to initiate shard transfer),
                // if collection has been created during snapshot application
                if !collection_exists {
                    for shard_id in collection.get_local_shards().await {
                        collection
                            .set_shard_replica_state(
                                shard_id,
                                self.this_peer_id,
                                ReplicaState::Dead,
                                None,
                            )
                            .await?;
                    }
                }
            }

            // Remove collections that are present locally but are not in the snapshot state
            for collection_name in collections.keys() {
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

    pub async fn create_snapshot(
        &self,
        collection_name: &str,
    ) -> Result<SnapshotDescription, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        // We want to use tmp dir inside the storage, because it is possible, that
        // snapshot directory is mounted as network share and multiple writes to it could be slow
        let tmp_dir = Path::new(&self.storage_config.storage_path).join(SNAPSHOTS_TMP_DIR);
        tokio::fs::create_dir_all(&tmp_dir).await?;
        Ok(collection
            .create_snapshot(&tmp_dir, self.this_peer_id)
            .await?)
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
                result.push(collection.get_telemetry_data().await);
            }
        }
        result
    }

    pub async fn peer_has_shards(&self, peer_id: PeerId) -> bool {
        for collection in self.collections.read().await.values() {
            let state = collection.state().await;
            if state
                .shards
                .into_values()
                .flat_map(|shard_info| shard_info.replicas.into_keys())
                .any(|x| x == peer_id)
            {
                return true;
            }
        }
        false
    }

    pub fn set_locks(&self, is_write_locked: bool, error_message: Option<String>) {
        self.is_write_locked
            .store(is_write_locked, Ordering::Relaxed);
        *self.lock_error_message.lock() = error_message;
    }

    pub fn is_write_locked(&self) -> bool {
        self.is_write_locked.load(Ordering::Relaxed)
    }

    pub fn get_lock_error_message(&self) -> Option<String> {
        self.lock_error_message.lock().clone()
    }

    /// Returns an error if the write lock is set
    pub fn check_write_lock(&self) -> Result<(), StorageError> {
        if self.is_write_locked.load(Ordering::Relaxed) {
            return Err(StorageError::Locked {
                description: self
                    .lock_error_message
                    .lock()
                    .clone()
                    .unwrap_or_else(|| DEFAULT_WRITE_LOCK_ERROR_MESSAGE.to_string()),
            });
        }
        Ok(())
    }

    pub async fn remove_shards_at_peer(&self, peer_id: PeerId) -> Result<(), StorageError> {
        let collections = self.collections.read().await;
        for collection in collections.values() {
            collection.remove_shards_at_peer(peer_id).await?;
        }
        Ok(())
    }

    pub fn remove_shards_at_peer_sync(&self, peer_id: PeerId) -> Result<(), StorageError> {
        self.general_runtime
            .block_on(self.remove_shards_at_peer(peer_id))
    }
}

impl CollectionContainer for TableOfContent {
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
                    collection.name(),
                    ReplicaState::Active,
                    Some(ReplicaState::Initializing),
                );
                let convert_to_listener_callback = Self::change_peer_state_callback(
                    self.consensus_proposal_sender.clone(),
                    collection.name(),
                    ReplicaState::Listener,
                    Some(ReplicaState::Active),
                );
                let convert_from_listener_to_active_callback = Self::change_peer_state_callback(
                    self.consensus_proposal_sender.clone(),
                    collection.name(),
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
}

// `TableOfContent` should not be dropped from async context.
impl Drop for TableOfContent {
    fn drop(&mut self) {
        self.general_runtime.block_on(async {
            for (_, mut collection) in self.collections.write().await.drain() {
                collection.before_drop().await;
            }
        });
    }
}
