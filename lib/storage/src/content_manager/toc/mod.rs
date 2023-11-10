mod collection_container;
mod collection_meta_ops;
mod create_collection;
mod locks;
mod point_ops;
mod snapshots;
mod temp_directories;
pub mod transfer;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, read_dir};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::qdrant_internal_client::QdrantInternalClient;
use api::grpc::qdrant::WaitOnConsensusCommitRequest;
use api::grpc::transport_channel_pool::AddTimeout;
use collection::collection::{Collection, RequestShardTransfer};
use collection::config::{default_replication_factor, CollectionConfig};
use collection::operations::types::*;
use collection::shards::channel_service::ChannelService;
use collection::shards::replica_set;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use collection::telemetry::CollectionTelemetry;
use futures::future::try_join_all;
use futures::Future;
use segment::common::cpu::get_num_cpus;
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, Semaphore};
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tonic::Status;

use self::transfer::ShardTransferDispatcher;
use crate::content_manager::alias_mapping::AliasPersistence;
use crate::content_manager::collection_meta_ops::CreateCollectionOperation;
use crate::content_manager::collections_ops::{Checker, Collections};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::errors::StorageError;
use crate::content_manager::shard_distribution::ShardDistributionProposal;
use crate::types::{PeerAddressById, StorageConfig};
use crate::ConsensusOperations;

pub const ALIASES_PATH: &str = "aliases";
pub const COLLECTIONS_DIR: &str = "collections";
pub const FULL_SNAPSHOT_FILE_NAME: &str = "full-snapshot";

/// The main object of the service. It holds all objects, required for proper functioning.
/// In most cases only one `TableOfContent` is enough for service. It is created only once during
/// the launch of the service.
pub struct TableOfContent {
    collections: Arc<RwLock<Collections>>,
    pub(super) storage_config: Arc<StorageConfig>,
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
    /// Prevent DDoS of too many concurrent updates in distributed mode.
    /// One external update usually triggers multiple internal updates, which breaks internal
    /// timings. For example, the health check timing and consensus timing.
    ///
    /// If not defined - no rate limiting is applied.
    update_rate_limiter: Option<Semaphore>,
    /// A lock to prevent concurrent collection creation.
    /// Effectively, this lock ensures that `create_collection` is called sequentially.
    collection_create_lock: Mutex<()>,
    /// Dispatcher for shard transfer to access consensus.
    shard_transfer_dispatcher: parking_lot::Mutex<Option<ShardTransferDispatcher>>,
}

impl TableOfContent {
    /// PeerId does not change during execution so it is ok to copy it here.
    #[allow(clippy::too_many_arguments)]
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
        if let Some(path) = storage_config.temp_path.as_deref() {
            let temp_path = Path::new(path);
            create_dir_all(temp_path).expect("Can't create temporary files directory");
        }
        let collection_paths =
            read_dir(&collections_path).expect("Can't read Collections directory");
        let mut collections: HashMap<String, Collection> = Default::default();
        let is_distributed = consensus_proposal_sender.is_some();
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
                storage_config
                    .to_shared_storage_config(is_distributed)
                    .into(),
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

        let rate_limiter = match storage_config.performance.update_rate_limit {
            Some(limit) => Some(Semaphore::new(limit)),
            None => {
                if consensus_proposal_sender.is_some() {
                    // Auto adjust the rate limit in distributed mode.
                    // Select number of working threads as a guess.
                    let limit = max(get_num_cpus(), 2);
                    log::debug!(
                        "Auto adjusting update rate limit to {} parallel update requests",
                        limit
                    );
                    Some(Semaphore::new(limit))
                } else {
                    None
                }
            }
        };

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
            update_rate_limiter: rate_limiter,
            collection_create_lock: Default::default(),
            shard_transfer_dispatcher: Default::default(),
        }
    }

    /// Return `true` if service is working in distributed mode.
    pub fn is_distributed(&self) -> bool {
        self.consensus_proposal_sender.is_some()
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_config.storage_path
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

    pub async fn get_collection(
        &self,
        collection_name: &str,
    ) -> Result<RwLockReadGuard<Collection>, StorageError> {
        let read_collection = self.collections.read().await;

        let real_collection_name = {
            let alias_persistence = self.alias_persistence.read().await;
            Self::resolve_name(collection_name, &read_collection, &alias_persistence).await?
        };
        // resolve_name already checked collection existence, unwrap is safe here
        Ok(RwLockReadGuard::map(read_collection, |collection| {
            collection.get(&real_collection_name).unwrap()
        }))
    }

    async fn get_collection_opt(
        &self,
        collection_name: String,
    ) -> Option<RwLockReadGuard<Collection>> {
        self.get_collection(&collection_name).await.ok()
    }

    /// Finds the original name of the collection
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection or alias to resolve
    /// * `collections` - A reference to the collections map
    /// * `aliases` - A reference to the aliases storage
    ///
    /// # Result
    ///
    /// If the collection exists - return its name
    /// If alias exists - returns the original collection name
    /// If neither exists - returns [`StorageError`]
    async fn resolve_name(
        collection_name: &str,
        collections: &Collections,
        aliases: &AliasPersistence,
    ) -> Result<String, StorageError> {
        let alias_collection_name = aliases.get(collection_name);

        let resolved_name = match alias_collection_name {
            None => collection_name.to_string(),
            Some(resolved_alias) => resolved_alias,
        };
        collections
            .validate_collection_exists(&resolved_name)
            .await?;
        Ok(resolved_name)
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

    /// Initiate receiving shard.
    ///
    /// Fails if the collection does not exist
    pub async fn initiate_receiving_shard(
        &self,
        collection_name: String,
        shard_id: ShardId,
    ) -> Result<(), StorageError> {
        // TODO: Ensure cancel safety!

        log::info!(
            "Initiating receiving shard {}:{}",
            collection_name,
            shard_id
        );

        // TODO: Ensure cancel safety!
        let initiate_shard_transfer_future = self
            .get_collection(&collection_name)
            .await?
            .initiate_shard_transfer(shard_id);
        initiate_shard_transfer_future.await?;
        Ok(())
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

    fn this_peer_id(&self) -> PeerId {
        self.this_peer_id
    }

    async fn create_collection_path(&self, collection_name: &str) -> Result<PathBuf, StorageError> {
        let path = self.get_collection_path(collection_name);

        if path.exists() {
            if CollectionConfig::check(&path) {
                return Err(StorageError::bad_input(format!(
                    "Can't create collection with name {collection_name}. Collection data already exists at {path}",
                    collection_name = collection_name,
                    path = path.display(),
                )));
            } else {
                // Collection doesn't have a valid config, remove it
                log::debug!(
                    "Removing invalid collection path {path} from storage",
                    path = path.display(),
                );
                tokio::fs::remove_dir_all(&path).await.map_err(|err| {
                    StorageError::service_error(format!(
                        "Can't clear directory for collection {collection_name}. Error: {err}"
                    ))
                })?;
            }
        }

        tokio::fs::create_dir_all(&path).await.map_err(|err| {
            StorageError::service_error(format!(
                "Can't create directory for collection {collection_name}. Error: {err}"
            ))
        })?;

        Ok(path)
    }

    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        Path::new(&self.storage_config.storage_path)
            .join(COLLECTIONS_DIR)
            .join(collection_name)
    }

    /// Wait until all other known peers reach the given commit
    ///
    /// # Errors
    ///
    /// This errors if:
    /// - any of the peers is not on the same term
    /// - waiting takes longer than the specified timeout
    /// - any of the peers cannot be reached
    pub async fn await_commit_on_all_peers(
        &self,
        commit: u64,
        term: u64,
        timeout: Duration,
    ) -> Result<(), StorageError> {
        let requests = self
            .peer_address_by_id()
            .keys()
            .filter(|id| **id != self.this_peer_id)
            // The collective timeout at the bottom of this function handles actually timing out.
            // Since an explicit timeout must be given here as well, it is multiplied by two to
            // give the collective timeout some space.
            .map(|peer_id| self.await_commit_on_peer(*peer_id, commit, term, timeout * 2))
            .collect::<Vec<_>>();
        let responses = try_join_all(requests);

        // Handle requests with timeout
        tokio::time::timeout(timeout, responses)
            .await
            .map(|_| ())
            .map_err(|_elapsed| StorageError::Timeout {
                description: "Failed to wait for consensus commit on all peers, timed out.".into(),
            })
    }

    fn peer_address_by_id(&self) -> PeerAddressById {
        self.channel_service.id_to_address.read().clone()
    }

    /// Wait until the given peer reaches the given commit
    ///
    /// # Errors
    ///
    /// This errors if the given peer is on a different term. Also errors if the peer cannot be reached.
    async fn await_commit_on_peer(
        &self,
        peer_id: PeerId,
        commit: u64,
        term: u64,
        timeout: Duration,
    ) -> Result<(), StorageError> {
        let response = self
            .with_qdrant_client(peer_id, |mut client| async move {
                let request = WaitOnConsensusCommitRequest {
                    commit: commit as i64,
                    term: term as i64,
                    timeout: timeout.as_secs() as i64,
                };
                client
                    .wait_on_consensus_commit(tonic::Request::new(request))
                    .await
            })
            .await
            .map_err(|err| {
                StorageError::service_error(format!(
                    "Failed to wait for consensus commit on peer {peer_id}: {err}"
                ))
            })?
            .into_inner();

        // Create error if wait request failed
        if !response.ok {
            return Err(StorageError::service_error(format!(
                "Failed to wait for consensus commit on peer {peer_id}, has diverged commit/term or timed out."
            )));
        }
        Ok(())
    }

    async fn with_qdrant_client<T, O: Future<Output = Result<T, Status>>>(
        &self,
        peer_id: PeerId,
        f: impl Fn(QdrantInternalClient<InterceptedService<Channel, AddTimeout>>) -> O,
    ) -> Result<T, CollectionError> {
        let address = self
            .channel_service
            .id_to_address
            .read()
            .get(&peer_id)
            .ok_or_else(|| CollectionError::service_error("Address for peer ID is not found."))?
            .clone();
        self.channel_service
            .channel_pool
            .with_channel(&address, |channel| {
                let client = QdrantInternalClient::new(channel);
                let client = client.max_decoding_message_size(usize::MAX);
                f(client)
            })
            .await
            .map_err(Into::into)
    }

    /// Insert dispatcher into table of contents for shard transfer.
    pub fn with_shard_transfer_dispatcher(&self, dispatcher: ShardTransferDispatcher) {
        self.shard_transfer_dispatcher.lock().replace(dispatcher);
    }
}
