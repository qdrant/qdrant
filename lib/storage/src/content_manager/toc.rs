use std::cmp;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, remove_dir_all};
use std::num::NonZeroU32;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::sync::{RwLock, RwLockReadGuard};

use collection::config::{CollectionConfig, CollectionParams};
use collection::operations::config_diff::DiffConfig;
use collection::operations::types::{
    PointRequest, RecommendRequest, Record, ScrollRequest, ScrollResult, SearchRequest,
    UpdateResult,
};
use collection::operations::CollectionUpdateOperations;
use collection::{ChannelService, Collection, CollectionShardDistribution};
use segment::types::ScoredPoint;

use crate::content_manager::collection_meta_ops::CollectionMetaOperations::CreateCollectionDistributed;
use crate::content_manager::shard_distribution::ShardDistributionProposal;
use crate::content_manager::{
    alias_mapping::AliasPersistence,
    collection_meta_ops::{
        AliasOperations, ChangeAliasesOperation, CollectionMetaOperations, CreateAlias,
        CreateAliasOperation, CreateCollection, DeleteAlias, DeleteAliasOperation, RenameAlias,
        RenameAliasOperation, UpdateCollection,
    },
    collections_ops::{Checker, Collections},
    errors::StorageError,
};
use crate::types::{ClusterInfo, ClusterStatus, PeerInfo, RaftInfo, StorageConfig};
use crate::{
    content_manager::{
        consensus_ops::ConsensusOperations, raft_state::Persistent as PersistentRaftState,
    },
    types::PeerAddressById,
};
use api::grpc::transport_channel_pool::TransportChannelPool;
use collection::collection_manager::collection_managers::CollectionSearcher;
use collection::collection_manager::simple_collection_searcher::SimpleCollectionSearcher;
use collection::shard::ShardId;
use collection::PeerId;
pub use consensus::TableOfContentRef;
use raft::{
    eraftpb::{ConfChangeV2, Entry as RaftEntry, Snapshot as RaftSnapshot},
    RawNode,
};
use tokio::sync::oneshot;
use tonic::transport::{Channel, Uri};
use wal::Wal;

const COLLECTIONS_DIR: &str = "collections";
const COLLECTIONS_META_WAL_DIR: &str = "collections_meta_wal";
const DEFAULT_META_OP_WAIT: Duration = Duration::from_secs(10);

pub struct ConsensusEnabled {
    pub propose_sender: std::sync::mpsc::Sender<Vec<u8>>,
    pub first_peer: bool,
    pub transport_channel_pool: TransportChannelPool,
}

/// The main object of the service. It holds all objects, required for proper functioning.
/// In most cases only one `TableOfContent` is enough for service. It is created only once during
/// the launch of the service.
pub struct TableOfContent {
    collections: Arc<RwLock<Collections>>,
    storage_config: StorageConfig,
    search_runtime: Runtime,
    collection_management_runtime: Runtime,
    alias_persistence: RwLock<AliasPersistence>,
    segment_searcher: Box<dyn CollectionSearcher + Sync + Send>,
    consensus_op_wal: Arc<std::sync::Mutex<Wal>>,
    raft_state: Arc<std::sync::Mutex<PersistentRaftState>>,
    propose_sender: Option<std::sync::Mutex<std::sync::mpsc::Sender<Vec<u8>>>>,
    on_consensus_op_apply:
        std::sync::Mutex<HashMap<ConsensusOperations, oneshot::Sender<Result<bool, StorageError>>>>,
    transport_channel_pool: Arc<TransportChannelPool>,
    this_peer_id: PeerId,
}

impl TableOfContent {
    pub fn new(
        storage_config: &StorageConfig,
        search_runtime: Runtime,
        consensus_enabled: Option<ConsensusEnabled>,
    ) -> Self {
        let collections_path = Path::new(&storage_config.storage_path).join(&COLLECTIONS_DIR);
        let collection_management_runtime = Runtime::new().unwrap();

        create_dir_all(&collections_path).expect("Can't create Collections directory");

        let raft_state = PersistentRaftState::load_or_init(
            &storage_config.storage_path,
            consensus_enabled.as_ref().map(|ce| ce.first_peer),
        )
        .expect("Cannot initialize Raft persistent storage");

        let (propose_sender, transport_channel_pool) = match consensus_enabled {
            None => (None, Arc::new(TransportChannelPool::default())), // use a default empty pool as consensus is disabled
            Some(ce) => (
                Some(std::sync::Mutex::new(ce.propose_sender)),
                Arc::new(ce.transport_channel_pool),
            ),
        };

        let collection_paths =
            read_dir(&collections_path).expect("Can't read Collections directory");

        let mut collections: HashMap<String, Collection> = Default::default();
        let channel_service = ChannelService::new(
            raft_state.peer_address_by_id.clone(),
            transport_channel_pool.clone(),
        );
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

            let collection = collection_management_runtime.block_on(Collection::load(
                collection_name.clone(),
                &collection_path,
                channel_service.clone(),
            ));

            collections.insert(collection_name, collection);
        }

        let alias_path = Path::new(&storage_config.storage_path).join("aliases");

        let alias_persistence =
            AliasPersistence::open(alias_path).expect("Can't open database by the provided config");

        let collection_meta_wal = {
            let collections_meta_wal_path =
                Path::new(&storage_config.storage_path).join(&COLLECTIONS_META_WAL_DIR);
            create_dir_all(&collections_meta_wal_path)
                .expect("Can't create Collections meta Wal directory");
            Arc::new(std::sync::Mutex::new(
                Wal::open(collections_meta_wal_path).unwrap(),
            ))
        };

        TableOfContent {
            collections: Arc::new(RwLock::new(collections)),
            storage_config: storage_config.clone(),
            search_runtime,
            alias_persistence: RwLock::new(alias_persistence),
            segment_searcher: Box::new(SimpleCollectionSearcher::new()),
            collection_management_runtime,
            // PeerId does not change during execution so it is ok to copy it here.
            this_peer_id: raft_state.this_peer_id(),
            consensus_op_wal: collection_meta_wal,
            raft_state: Arc::new(std::sync::Mutex::new(raft_state)),
            propose_sender,
            on_consensus_op_apply: std::sync::Mutex::new(HashMap::new()),
            transport_channel_pool,
        }
    }

    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        Path::new(&self.storage_config.storage_path)
            .join(&COLLECTIONS_DIR)
            .join(collection_name)
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

    fn share_ip_to_address(&self) -> Result<Arc<std::sync::RwLock<PeerAddressById>>, StorageError> {
        // Done within a non-async function as the std::sync::Mutex is not Send
        let raft_state_guard = self.raft_state.lock()?;
        let ip_to_address = raft_state_guard.peer_address_by_id.clone();
        Ok(ip_to_address)
    }

    async fn create_collection(
        &self,
        collection_name: &str,
        operation: CreateCollection,
        collection_shard_distribution: CollectionShardDistribution,
    ) -> Result<bool, StorageError> {
        let CreateCollection {
            vector_size,
            distance,
            shard_number,
            hnsw_config: hnsw_config_diff,
            wal_config: wal_config_diff,
            optimizers_config: optimizers_config_diff,
        } = operation;

        self.collections
            .read()
            .await
            .validate_collection_not_exists(collection_name)
            .await?;

        let collection_path = self.create_collection_path(collection_name).await?;

        let collection_params = CollectionParams {
            vector_size,
            distance,
            shard_number: NonZeroU32::new(shard_number).ok_or(StorageError::BadInput {
                description: "`shard_number` cannot be 0".to_string(),
            })?,
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
        let ip_to_address = self.share_ip_to_address()?;
        let channel_service =
            ChannelService::new(ip_to_address, self.transport_channel_pool.clone());
        let collection = Collection::new(
            collection_name.to_string(),
            Path::new(&collection_path),
            &collection_config,
            collection_shard_distribution,
            channel_service,
        )
        .await?;

        let mut write_collections = self.collections.write().await;
        write_collections
            .validate_collection_not_exists(collection_name)
            .await?;
        write_collections.insert(collection_name.to_string(), collection);
        Ok(true)
    }

    async fn update_collection(
        &self,
        collection_name: &str,
        operation: UpdateCollection,
    ) -> Result<bool, StorageError> {
        match operation.optimizers_config {
            None => {}
            Some(new_optimizers_config) => {
                let collection = self.get_collection(collection_name).await?;
                collection
                    .update_optimizer_params_from_diff(new_optimizers_config)
                    .await?
            }
        }
        Ok(true)
    }

    async fn delete_collection(&self, collection_name: &str) -> Result<bool, StorageError> {
        if let Some(mut removed) = self.collections.write().await.remove(collection_name) {
            removed.before_drop().await;
            let path = self.get_collection_path(collection_name);
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

    /// If `wait_timeout` is not supplied - then default duration will be used.
    /// This function needs to be called from a runtime with timers enabled.
    #[allow(unused_variables)]
    pub async fn submit_collection_operation(
        &self,
        operation: CollectionMetaOperations,
        wait_timeout: Option<Duration>,
    ) -> Result<bool, StorageError> {
        // if distributed deployment is enabled
        if self.is_consensus_enabled() {
            let op = match operation {
                CollectionMetaOperations::CreateCollection(op) => {
                    let shard_number = op.create_collection.shard_number;
                    let known_peers: Vec<_> = self
                        .raft_state
                        .lock()?
                        .peer_address_by_id
                        .read()?
                        .keys()
                        .cloned()
                        .collect();
                    let known_collections = self.collections.read().await;
                    let known_shards: Vec<_> = known_collections
                        .iter()
                        .flat_map(|(_, col)| col.all_shards())
                        .collect();
                    let shard_distribution = ShardDistributionProposal::new(
                        shard_number,
                        self.this_peer_id,
                        &known_peers,
                        known_shards,
                    );
                    log::info!("Proposing distribution for {} shards for collection '{}' among {} peers {:?}", shard_number, op.collection_name, known_peers.len(), shard_distribution.distribution);
                    CreateCollectionDistributed(op, shard_distribution)
                }
                op => op,
            };
            self.propose_consensus_op(
                ConsensusOperations::CollectionMeta(Box::new(op)),
                wait_timeout,
            )
            .await
        } else {
            self.perform_collection_meta_op(operation).await
        }
    }

    pub async fn propose_consensus_op(
        &self,
        operation: ConsensusOperations,
        wait_timeout: Option<Duration>,
    ) -> Result<bool, StorageError> {
        let propose_sender = match &self.propose_sender {
            Some(sender) => sender,
            None => {
                return Err(StorageError::ServiceError {
                    description:
                        "Cannot submit consensus operation proposal: no sender supplied to ToC"
                            .to_string(),
                });
            }
        };
        let serialized = serde_cbor::to_vec(&operation)?;
        let (sender, receiver) = oneshot::channel();
        self.on_consensus_op_apply.lock()?.insert(operation, sender);
        propose_sender.lock()?.send(serialized)?;
        let wait_timeout = wait_timeout.unwrap_or(DEFAULT_META_OP_WAIT);
        tokio::time::timeout(wait_timeout, receiver)
            .await
            .map_err(
                |_: tokio::time::error::Elapsed| StorageError::ServiceError {
                    description: format!(
                        "Waiting for consensus operation commit failed. Timeout set at: {} seconds",
                        wait_timeout.as_secs_f64()
                    ),
                },
                // ?? - forwards 2 possible errors: sender dropped, operation failed
            )??
    }

    async fn perform_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError> {
        match operation {
            CollectionMetaOperations::CreateCollectionDistributed(operation, distribution) => {
                let local = distribution.local_shards_for(self.this_peer_id);
                let remote = distribution.remote_shards_for(self.this_peer_id);
                let collection_shard_distribution =
                    CollectionShardDistribution::Distribution { local, remote };
                self.create_collection(
                    &operation.collection_name,
                    operation.create_collection,
                    collection_shard_distribution,
                )
                .await
            }
            CollectionMetaOperations::CreateCollection(operation) => {
                self.create_collection(
                    &operation.collection_name,
                    operation.create_collection,
                    CollectionShardDistribution::AllLocal,
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
        }
    }

    pub async fn get_collection<'a>(
        &'a self,
        collection_name: &str,
    ) -> Result<RwLockReadGuard<'a, Collection>, StorageError> {
        let read_collection = self.collections.read().await;
        let real_collection_name = self.resolve_name(collection_name).await?;
        // resolve_name already checked collection existence, unwrap is safe here
        Ok(RwLockReadGuard::map(read_collection, |collection| {
            collection.get(&real_collection_name).unwrap()
        }))
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
            .recommend_by(
                request,
                self.segment_searcher.deref(),
                self.search_runtime.handle(),
                shard_selection,
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
        shard_selection: Option<ShardId>,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search(
                request,
                self.segment_searcher.as_ref(),
                self.search_runtime.handle(),
                shard_selection,
            )
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
            .retrieve(request, self.segment_searcher.as_ref(), shard_selection)
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
            .scroll_by(request, self.segment_searcher.deref(), shard_selection)
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

    fn is_consensus_enabled(&self) -> bool {
        self.propose_sender.is_some()
    }

    pub async fn cluster_status(&self) -> Result<ClusterStatus, StorageError> {
        match self.is_consensus_enabled() {
            true => {
                let hard_state = self.hard_state()?;
                let peers = self
                    .peer_address_by_id()?
                    .into_iter()
                    .map(|(peer_id, uri)| {
                        (
                            peer_id,
                            PeerInfo {
                                uri: uri.to_string(),
                            },
                        )
                    })
                    .collect();
                let pending_operations = self.raft_state.lock()?.unapplied_entities_count();
                Ok(ClusterStatus::Enabled(ClusterInfo {
                    peer_id: self.this_peer_id,
                    peers,
                    raft_info: RaftInfo {
                        term: hard_state.term,
                        commit: hard_state.commit,
                        pending_operations,
                    },
                }))
            }
            false => Ok(ClusterStatus::Disabled),
        }
    }

    pub fn consensus_op_entry(&self, raft_entry_id: u64) -> raft::Result<RaftEntry> {
        // Raft entries are expected to have index starting from 1
        if raft_entry_id < 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        let first_entry = self
            .first_consensus_op_wal_entry()
            .map_err(consensus::raft_error_other)?
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?;
        let first_wal_index = self
            .consensus_op_wal
            .lock()
            .map_err(consensus::raft_error_other)?
            .first_index();
        // Due to snapshots there might be different offsets between wal index and raft entry index
        let offset = first_entry.index - first_wal_index;
        <RaftEntry as prost::Message>::decode(
            self.consensus_op_wal
                .lock()
                .map_err(consensus::raft_error_other)?
                .entry(raft_entry_id - offset)
                .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?
                .as_ref(),
        )
        .map_err(consensus::raft_error_other)
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.this_peer_id
    }

    async fn state_snapshot(&self) -> raft::Result<consensus::SnapshotData> {
        let collections: HashMap<collection::CollectionId, collection::State> = self
            .collections
            .read()
            .await
            .iter()
            .map(|(id, collection)| (id.clone(), collection.state(self.this_peer_id())))
            .collect();
        Ok(consensus::SnapshotData {
            collections,
            aliases: self.alias_persistence.read().await.state().clone(),
            address_by_id: self
                .raft_state
                .lock()
                .map_err(consensus::raft_error_other)?
                .peer_address_by_id()
                .map_err(consensus::raft_error_other)?,
        })
    }

    pub fn apply_normal_entry(&self, entry: &RaftEntry) -> Result<bool, StorageError> {
        let operation: ConsensusOperations = entry.try_into()?;
        let on_apply = self.on_consensus_op_apply.lock()?.remove(&operation);
        let result = match operation {
            ConsensusOperations::CollectionMeta(operation) => self
                .collection_management_runtime
                .block_on(self.perform_collection_meta_op(*operation)),
            ConsensusOperations::AddPeer(peer_id, uri) => self
                .add_peer(
                    peer_id,
                    uri.parse().map_err(|err| StorageError::ServiceError {
                        description: format!("Failed to parse Uri: {err}"),
                    })?,
                )
                .map(|()| true),
        };
        if let Some(on_apply) = on_apply {
            if on_apply.send(result.clone()).is_err() {
                log::warn!("Failed to notify on consensus operation completion: channel receiver is dropped")
            }
        }
        result
    }

    pub fn apply_conf_change_entry(
        &self,
        entry: &RaftEntry,
        raw_node: &mut RawNode<TableOfContentRef>,
    ) -> Result<(), StorageError> {
        let change: ConfChangeV2 = prost::Message::decode(entry.get_data())?;
        let conf_state = raw_node.apply_conf_change(&change)?;
        self.raft_state
            .lock()?
            .apply_state_update(|state| state.conf_state = conf_state)?;
        Ok(())
    }

    pub fn set_unapplied_entries(
        &self,
        first_index: u64,
        last_index: u64,
    ) -> Result<(), raft::Error> {
        self.raft_state
            .lock()
            .map_err(consensus::raft_error_other)?
            .set_unapplied_entries(first_index, last_index)
            .map_err(consensus::raft_error_other)
    }

    pub fn apply_entries(
        &self,
        raw_node: &mut RawNode<TableOfContentRef>,
    ) -> Result<(), raft::Error> {
        use raft::eraftpb::EntryType;

        loop {
            let unapplied_index = self
                .raft_state
                .lock()
                .map_err(consensus::raft_error_other)?
                .current_unapplied_entry();
            let entry_index = match unapplied_index {
                Some(index) => index,
                None => break,
            };
            log::info!("Applying committed entry with index {entry_index}");
            let entry = self.consensus_op_entry(entry_index)?;
            if entry.data.is_empty() {
                // Empty entry, when the peer becomes Leader it will send an empty entry.
            } else {
                match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        let operation_result = self.apply_normal_entry(&entry);
                        match operation_result {
                            Ok(result) => log::info!(
                                "Successfully applied consensus operation entry. Index: {}. Result: {result}",
                                entry.index
                            ),
                            Err(err) => {
                                log::error!("Failed to apply collection meta operation entry with error: {err}")
                            }
                         }
                    }
                    EntryType::EntryConfChangeV2 => {
                        match self.apply_conf_change_entry(&entry, raw_node) {
                            Ok(()) => log::info!(
                                "Successfully applied configuration change entry. Index: {}.",
                                entry.index
                            ),
                            Err(err) => {
                                log::error!(
                                    "Failed to apply configuration change entry with error: {err}"
                                )
                            }
                        }
                    }
                    ty => log::error!("Failed to apply entry: unsupported entry type {ty:?}"),
                }
            }

            self.raft_state
                .lock()
                .map_err(consensus::raft_error_other)?
                .entry_applied()
                .map_err(consensus::raft_error_other)?;
        }
        Ok(())
    }

    pub fn append_entries(&self, entries: Vec<RaftEntry>) -> Result<(), StorageError> {
        use prost::Message;

        let mut wal_guard = self.consensus_op_wal.lock()?;
        for entry in entries {
            log::debug!("Appending entry: {entry:?}");
            let mut buf = vec![];
            entry.encode(&mut buf)?;
            wal_guard.append(&buf)?;
        }
        Ok(())
    }

    pub fn first_consensus_op_wal_entry(&self) -> Result<Option<RaftEntry>, StorageError> {
        let wal_guard = self.consensus_op_wal.lock()?;
        let first_index = wal_guard.first_index();
        let entry = wal_guard
            .entry(first_index)
            .map(|entry| <RaftEntry as prost::Message>::decode(entry.as_ref()));
        Ok(entry.transpose()?)
    }

    pub fn last_consensus_op_wal_entry(&self) -> Result<Option<RaftEntry>, StorageError> {
        let wal_guard = self.consensus_op_wal.lock()?;
        let last_index = wal_guard.last_index();
        let entry = wal_guard
            .entry(last_index)
            .map(|entry| <RaftEntry as prost::Message>::decode(entry.as_ref()));
        Ok(entry.transpose()?)
    }

    pub fn apply_snapshot(&self, snapshot: &RaftSnapshot) -> Result<(), StorageError> {
        let meta = snapshot.get_metadata();
        if raft::Storage::first_index(self)? > meta.index {
            return Err(StorageError::ServiceError {
                description: "Snapshot out of date".to_string(),
            });
        }
        self.consensus_op_wal.lock()?.clear()?;
        self.raft_state.lock()?.apply_state_update(move |state| {
            state.conf_state = meta.get_conf_state().clone();
            state.hard_state.term = cmp::max(state.hard_state.term, meta.term);
            state.hard_state.commit = meta.index
        })?;
        self.apply_snapshot_data(snapshot.get_data().try_into()?)
    }

    fn apply_snapshot_data(&self, data: consensus::SnapshotData) -> Result<(), StorageError> {
        // Apply peer addresses
        self.raft_state
            .lock()?
            .set_peer_address_by_id(data.address_by_id)?;

        self.collection_management_runtime.block_on(async {
            let mut collections = self.collections.write().await;
            let ip_to_address = self.share_ip_to_address()?;
            let channel_service =
                ChannelService::new(ip_to_address, self.transport_channel_pool.clone());
            for (id, state) in &data.collections {
                let collection = collections.get_mut(id);
                match collection {
                    // Update state if collection present locally
                    Some(collection) => {
                        if &collection.state(self.this_peer_id()) != state {
                            collection
                                .apply_state(
                                    state.clone(),
                                    self.this_peer_id(),
                                    &self.get_collection_path(&collection.name()),
                                    channel_service.clone(),
                                )
                                .await?;
                        }
                    }
                    // Create collection if not present locally
                    None => {
                        let collection_path = self.create_collection_path(id).await?;
                        let shard_distribution = CollectionShardDistribution::from_shard_to_peer(
                            self.this_peer_id,
                            &state.shard_to_peer,
                        );
                        let collection = Collection::new(
                            id.to_string(),
                            Path::new(&collection_path),
                            &state.config,
                            shard_distribution,
                            channel_service.clone(),
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
                    log::info!(
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

    pub fn set_hard_state(&self, hard_state: raft::eraftpb::HardState) -> Result<(), StorageError> {
        self.raft_state
            .lock()?
            .apply_state_update(move |state| state.hard_state = hard_state)
    }

    pub fn hard_state(&self) -> Result<raft::eraftpb::HardState, StorageError> {
        Ok(self.raft_state.lock()?.state().hard_state.clone())
    }

    pub fn set_commit_index(&self, index: u64) -> Result<(), StorageError> {
        self.raft_state
            .lock()?
            .apply_state_update(|state| state.hard_state.commit = index)
    }

    pub fn peer_address_by_id(&self) -> Result<PeerAddressById, StorageError> {
        self.raft_state.lock()?.peer_address_by_id()
    }

    pub fn add_peer(&self, peer_id: PeerId, uri: Uri) -> Result<(), StorageError> {
        self.raft_state.lock()?.insert_peer(peer_id, uri)
    }

    /// A pool will be created if no channels exist for this address.
    pub async fn get_or_create_pooled_channel(&self, uri: &Uri) -> Result<Channel, StorageError> {
        self.transport_channel_pool
            .get_or_create_pooled_channel(uri)
            .await
            .map_err(|err| StorageError::ServiceError {
                description: format!(
                    "Can't create pooled transport channel for {}. Error: {}",
                    uri, err
                ),
            })
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

mod consensus {
    use std::{collections::HashMap, ops::Deref, sync::Arc};

    use collection::CollectionId;
    use raft::{eraftpb::Entry as RaftEntry, storage::Storage as RaftStorage, RaftState};
    use serde::{Deserialize, Serialize};

    use crate::{content_manager::alias_mapping::AliasMapping, types::PeerAddressById};

    use super::TableOfContent;

    fn wal_entries(
        toc: &TableOfContent,
        low: u64,
        high: u64,
        max_size: Option<u64>,
    ) -> raft::Result<Vec<RaftEntry>> {
        (low..high)
            .take(max_size.unwrap_or(high - low + 1) as usize)
            .map(|id| toc.consensus_op_entry(id))
            .collect()
    }

    impl RaftStorage for TableOfContent {
        fn initial_state(&self) -> raft::Result<RaftState> {
            Ok(self
                .raft_state
                .lock()
                .map_err(raft_error_other)?
                .state()
                .clone())
        }

        fn entries(
            &self,
            low: u64,
            high: u64,
            max_size: impl Into<Option<u64>>,
        ) -> raft::Result<Vec<RaftEntry>> {
            let max_size: Option<_> = max_size.into();
            wal_entries(self, low, high, max_size)
        }

        fn term(&self, idx: u64) -> raft::Result<u64> {
            {
                let raft_state = self.raft_state.lock().map_err(raft_error_other)?;
                if idx == raft_state.state().hard_state.commit {
                    return Ok(raft_state.state().hard_state.term);
                }
            }
            Ok(self.consensus_op_entry(idx)?.term)
        }

        fn first_index(&self) -> raft::Result<u64> {
            let index = match self
                .first_consensus_op_wal_entry()
                .map_err(raft_error_other)?
            {
                Some(entry) => entry.index,
                None => {
                    self.raft_state
                        .lock()
                        .map_err(raft_error_other)?
                        .state()
                        .hard_state
                        .commit
                        + 1
                }
            };
            Ok(index)
        }

        fn last_index(&self) -> raft::Result<u64> {
            let index = match self
                .last_consensus_op_wal_entry()
                .map_err(raft_error_other)?
            {
                Some(entry) => entry.index,
                None => {
                    self.raft_state
                        .lock()
                        .map_err(raft_error_other)?
                        .state()
                        .hard_state
                        .commit
                }
            };
            Ok(index)
        }

        fn snapshot(&self, request_index: u64) -> raft::Result<raft::eraftpb::Snapshot> {
            let snapshot = self
                .collection_management_runtime
                .block_on(self.state_snapshot())?;
            let raft_state = self
                .raft_state
                .lock()
                .map_err(raft_error_other)?
                .state()
                .clone();
            if raft_state.hard_state.commit >= request_index {
                Ok(raft::eraftpb::Snapshot {
                    data: serde_cbor::to_vec(&snapshot).map_err(raft_error_other)?,
                    metadata: Some(raft::eraftpb::SnapshotMetadata {
                        conf_state: Some(raft_state.conf_state),
                        index: raft_state.hard_state.commit,
                        term: raft_state.hard_state.term,
                    }),
                })
            } else {
                Err(raft::Error::Store(
                    raft::StorageError::SnapshotTemporarilyUnavailable,
                ))
            }
        }
    }

    #[derive(Clone)]
    pub struct TableOfContentRef(Arc<TableOfContent>);

    impl From<Arc<TableOfContent>> for TableOfContentRef {
        fn from(arc: Arc<TableOfContent>) -> Self {
            Self(arc)
        }
    }

    impl Deref for TableOfContentRef {
        type Target = TableOfContent;

        fn deref(&self) -> &Self::Target {
            &*self.0
        }
    }

    impl RaftStorage for TableOfContentRef {
        fn initial_state(&self) -> raft::Result<raft::RaftState> {
            self.0.initial_state()
        }

        fn entries(
            &self,
            low: u64,
            high: u64,
            max_size: impl Into<Option<u64>>,
        ) -> raft::Result<Vec<raft::eraftpb::Entry>> {
            self.0.entries(low, high, max_size)
        }

        fn term(&self, idx: u64) -> raft::Result<u64> {
            self.0.term(idx)
        }

        fn first_index(&self) -> raft::Result<u64> {
            self.0.first_index()
        }

        fn last_index(&self) -> raft::Result<u64> {
            self.0.last_index()
        }

        fn snapshot(&self, request_index: u64) -> raft::Result<raft::eraftpb::Snapshot> {
            self.0.snapshot(request_index)
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct SnapshotData {
        pub collections: HashMap<CollectionId, collection::State>,
        pub aliases: AliasMapping,
        #[serde(with = "crate::serialize_peer_addresses")]
        pub address_by_id: PeerAddressById,
    }

    impl TryFrom<&[u8]> for SnapshotData {
        type Error = serde_cbor::Error;

        fn try_from(bytes: &[u8]) -> Result<SnapshotData, Self::Error> {
            serde_cbor::from_slice(bytes)
        }
    }

    #[derive(thiserror::Error, Debug)]
    #[error("{0}")]
    struct StrError(String);

    pub fn raft_error_other(e: impl std::error::Error) -> raft::Error {
        raft::Error::Store(raft::StorageError::Other(Box::new(StrError(e.to_string()))))
    }
}
