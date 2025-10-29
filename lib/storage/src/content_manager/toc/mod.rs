mod collection_container;
mod collection_meta_ops;
mod create_collection;
pub mod dispatcher;
mod point_ops;
mod point_ops_internal;
pub mod request_hw_counter;
mod snapshots;
mod telemetry;
mod temp_directories;
pub mod transfer;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use api::rest::models::HardwareUsage;
use collection::collection::{Collection, RequestShardTransfer};
use collection::config::{
    CollectionConfigInternal, default_replication_factor, default_shard_number,
};
use collection::operations::types::*;
use collection::shards::channel_service::ChannelService;
use collection::shards::replica_set::{AbortShardTransfer, ReplicaState};
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::{CollectionId, replica_set};
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwSharedDrain;
use common::cpu::get_num_cpus;
use dashmap::DashMap;
use fs_err as fs;
use fs_err::tokio as tokio_fs;
use segment::data_types::collection_defaults::CollectionConfigDefaults;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, Semaphore};

use self::dispatcher::TocDispatcher;
use crate::ConsensusOperations;
use crate::content_manager::alias_mapping::AliasPersistence;
use crate::content_manager::collection_meta_ops::CreateCollectionOperation;
use crate::content_manager::collections_ops::{Checker, Collections};
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::errors::StorageError;
use crate::content_manager::shard_distribution::ShardDistributionProposal;
use crate::rbac::{Access, AccessRequirements, CollectionPass};
use crate::types::StorageConfig;

pub const ALIASES_PATH: &str = "aliases";
pub const COLLECTIONS_DIR: &str = "collections";
pub const FULL_SNAPSHOT_FILE_NAME: &str = "full-snapshot";

/// The main object of the service. It holds all objects, required for proper functioning.
///
/// In most cases only one `TableOfContent` is enough for service. It is created only once during
/// the launch of the service.
pub struct TableOfContent {
    collections: Arc<RwLock<Collections>>,
    pub(crate) storage_config: Arc<StorageConfig>,
    search_runtime: Runtime,
    update_runtime: Runtime,
    general_runtime: Runtime,
    /// Global CPU budget in number of cores for all optimization tasks.
    /// Assigns CPU permits to tasks to limit overall resource utilization.
    optimizer_resource_budget: ResourceBudget,
    alias_persistence: RwLock<AliasPersistence>,
    pub this_peer_id: PeerId,
    channel_service: ChannelService,
    /// Backlink to the consensus, if none - single node mode
    consensus_proposal_sender: Option<OperationSender>,
    /// Dispatcher for access to table of contents and consensus, if none - single node mode
    toc_dispatcher: parking_lot::Mutex<Option<TocDispatcher>>,
    /// Prevent DDoS of too many concurrent updates in distributed mode.
    /// One external update usually triggers multiple internal updates, which breaks internal
    /// timings. For example, the health check timing and consensus timing.
    ///
    /// If not defined - no rate limiting is applied.
    update_rate_limiter: Option<Semaphore>,
    /// A lock to prevent concurrent collection creation.
    /// Effectively, this lock ensures that `create_collection` is called sequentially.
    collection_create_lock: Mutex<()>,
    /// Aggregation of all hardware measurements for each alias or collection config.
    collection_hw_metrics: DashMap<CollectionId, HwSharedDrain>,
}

impl TableOfContent {
    /// PeerId does not change during execution so it is ok to copy it here.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_config: &StorageConfig,
        search_runtime: Runtime,
        update_runtime: Runtime,
        general_runtime: Runtime,
        optimizer_resource_budget: ResourceBudget,
        channel_service: ChannelService,
        this_peer_id: PeerId,
        consensus_proposal_sender: Option<OperationSender>,
    ) -> Self {
        let collections_path = Path::new(&storage_config.storage_path).join(COLLECTIONS_DIR);
        fs::create_dir_all(&collections_path).expect("Can't create Collections directory");
        if let Some(path) = storage_config.temp_path.as_deref() {
            let temp_path = Path::new(path);
            fs::create_dir_all(temp_path).expect("Can't create temporary files directory");
        }
        let collection_paths =
            fs::read_dir(&collections_path).expect("Can't read Collections directory");
        let mut collections: HashMap<String, Collection> = Default::default();
        let is_distributed = consensus_proposal_sender.is_some();
        for entry in collection_paths {
            let collection_path = entry
                .expect("Can't access of one of the collection files")
                .path();

            if !CollectionConfigInternal::check(&collection_path) {
                log::warn!(
                    "Collection config is not found in the collection directory: {}, skipping",
                    collection_path.display(),
                );
                continue;
            }

            let collection_name = collection_path
                .file_name()
                .expect("Can't resolve a filename of one of the collection files")
                .to_str()
                .expect("A filename of one of the collection files is not a valid UTF-8")
                .to_string();

            let snapshots_path = Path::new(&storage_config.snapshots_path.clone()).to_owned();
            let collection_snapshots_path =
                Self::collection_snapshots_path(&snapshots_path, &collection_name);

            log::info!("Loading collection: {collection_name}");
            let collection = general_runtime.block_on(Collection::load(
                collection_name.clone(),
                this_peer_id,
                &collection_path,
                &collection_snapshots_path,
                storage_config
                    .to_shared_storage_config(is_distributed)
                    .into(),
                channel_service.clone(),
                Self::change_peer_from_state_callback(
                    consensus_proposal_sender.clone(),
                    collection_name.clone(),
                    ReplicaState::Dead,
                ),
                Self::request_shard_transfer_callback(
                    consensus_proposal_sender.clone(),
                    collection_name.clone(),
                ),
                Self::abort_shard_transfer_callback(
                    consensus_proposal_sender.clone(),
                    collection_name.clone(),
                ),
                Some(search_runtime.handle().clone()),
                Some(update_runtime.handle().clone()),
                optimizer_resource_budget.clone(),
                storage_config.optimizers_overwrite.clone(),
            ));

            collections.insert(collection_name, collection);
        }
        let alias_path = Path::new(&storage_config.storage_path).join(ALIASES_PATH);
        let alias_persistence = AliasPersistence::open(&alias_path)
            .expect("Can't open database by the provided config");

        let rate_limiter = match storage_config.performance.update_rate_limit {
            Some(limit) => Some(Semaphore::new(limit)),
            None => {
                if consensus_proposal_sender.is_some() {
                    // Auto adjust the rate limit in distributed mode.
                    // Select number of working threads as a guess.
                    let limit = max(get_num_cpus(), 2);
                    log::debug!(
                        "Auto adjusting update rate limit to {limit} parallel update requests"
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
            optimizer_resource_budget,
            alias_persistence: RwLock::new(alias_persistence),
            this_peer_id,
            channel_service,
            consensus_proposal_sender,
            toc_dispatcher: Default::default(),
            update_rate_limiter: rate_limiter,
            collection_create_lock: Default::default(),
            collection_hw_metrics: DashMap::new(),
        }
    }

    /// Return `true` if service is working in distributed mode.
    pub fn is_distributed(&self) -> bool {
        self.consensus_proposal_sender.is_some()
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_config.storage_path
    }

    /// List of all collections to which the user has access
    pub async fn all_collections(&self, access: &Access) -> Vec<CollectionPass<'static>> {
        self.all_collections_with_access_requirements(access, AccessRequirements::new())
            .await
    }

    pub async fn all_collections_whole_access(
        &self,
        access: &Access,
    ) -> Vec<CollectionPass<'static>> {
        self.all_collections_with_access_requirements(access, AccessRequirements::new().whole())
            .await
    }

    async fn all_collections_with_access_requirements(
        &self,
        access: &Access,
        access_requirements: AccessRequirements,
    ) -> Vec<CollectionPass<'static>> {
        self.collections
            .read()
            .await
            .keys()
            .filter_map(|name| {
                access
                    .check_collection_access(name, access_requirements)
                    .ok()
                    .map(|pass| pass.into_static())
            })
            .collect()
    }

    /// List of all collections
    pub fn all_collections_sync(&self) -> Vec<String> {
        self.general_runtime
            .block_on(self.collections.read())
            .keys()
            .cloned()
            .collect()
    }

    /// Same as `get_collection`, but does not check access rights.
    /// Intended for internal use only.
    ///
    /// **Do no make public**
    async fn get_collection_unchecked(
        &self,
        collection_name: &str,
    ) -> Result<RwLockReadGuard<'_, Collection>, StorageError> {
        let read_collection = self.collections.read().await;

        let real_collection_name = {
            let alias_persistence = self.alias_persistence.read().await;
            Self::resolve_name(collection_name, &read_collection, &alias_persistence)?
        };
        // resolve_name already checked collection existence, unwrap is safe here
        Ok(RwLockReadGuard::map(read_collection, |collection| {
            collection.get(&real_collection_name).unwrap() // TODO: WTF!?
        }))
    }

    pub async fn get_collection(
        &self,
        collection: &CollectionPass<'_>,
    ) -> Result<RwLockReadGuard<'_, Collection>, StorageError> {
        self.get_collection_unchecked(collection.name()).await
    }

    async fn get_collection_opt(
        &self,
        collection_name: String,
    ) -> Option<RwLockReadGuard<'_, Collection>> {
        self.get_collection_unchecked(&collection_name).await.ok()
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
    fn resolve_name(
        collection_name: &str,
        collections: &Collections,
        aliases: &AliasPersistence,
    ) -> Result<String, StorageError> {
        let alias_collection_name = aliases.get(collection_name);

        let resolved_name = match alias_collection_name {
            None => collection_name.to_string(),
            Some(resolved_alias) => resolved_alias,
        };
        collections.validate_collection_exists(&resolved_name)?;
        Ok(resolved_name)
    }

    /// List of all aliases for a given collection
    pub async fn collection_aliases(
        &self,
        collection_pass: &CollectionPass<'_>,
        access: &Access,
    ) -> Result<Vec<String>, StorageError> {
        let mut result = self
            .alias_persistence
            .read()
            .await
            .collection_aliases(collection_pass.name());
        result.retain(|alias| {
            access
                .check_collection_access(alias, AccessRequirements::new())
                .is_ok()
        });
        Ok(result)
    }

    /// List of all aliases across all collections
    pub async fn list_aliases(
        &self,
        access: &Access,
    ) -> Result<Vec<AliasDescription>, StorageError> {
        let all_collections = self.all_collections(access).await;
        let mut aliases: Vec<AliasDescription> = Default::default();
        for collection_pass in &all_collections {
            for alias in self.collection_aliases(collection_pass, access).await? {
                aliases.push(AliasDescription {
                    alias_name: alias.to_string(),
                    collection_name: collection_pass.to_string(),
                });
            }
        }

        Ok(aliases)
    }

    pub fn suggest_shard_distribution(
        &self,
        op: &CreateCollectionOperation,
        collection_defaults: Option<&CollectionConfigDefaults>,
        number_of_peers: usize,
    ) -> ShardDistributionProposal {
        let non_zero_number_of_peers =
            NonZeroU32::new(number_of_peers as u32).expect("NUmber of peers must be at least 1");

        let suggested_shard_number = collection_defaults
            .map(|cd| cd.get_shard_number(number_of_peers as u32))
            .map(|x| NonZeroU32::new(x).expect("Shard number must be at least 1"))
            .unwrap_or_else(|| default_shard_number().saturating_mul(non_zero_number_of_peers));

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

        let suggested_replication_factor = collection_defaults
            .and_then(|cd| cd.replication_factor)
            .and_then(NonZeroU32::new)
            .unwrap_or_else(default_replication_factor);

        let replication_factor = op
            .create_collection
            .replication_factor
            .and_then(NonZeroU32::new)
            .unwrap_or(suggested_replication_factor);

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

        log::info!("Initiating receiving shard {collection_name}:{shard_id}");

        // TODO: Ensure cancel safety!
        let initiate_shard_transfer_future = self
            .get_collection_unchecked(&collection_name)
            .await?
            .initiate_shard_transfer(shard_id);
        initiate_shard_transfer_future.await?;
        Ok(())
    }

    pub fn request_snapshot(&self) -> Result<(), StorageError> {
        self.get_consensus_proposal_sender()?
            .send(ConsensusOperations::request_snapshot())?;

        Ok(())
    }

    pub async fn update_cluster_metadata(
        &self,
        key: String,
        value: serde_json::Value,
        wait: bool,
    ) -> Result<(), StorageError> {
        let operation = ConsensusOperations::UpdateClusterMetadata { key, value };

        if wait {
            let dispatcher = self.toc_dispatcher.lock().clone().ok_or_else(|| {
                StorageError::service_error("Qdrant is running in standalone mode")
            })?;
            dispatcher
                .consensus_state()
                .propose_consensus_op_with_await(operation, None)
                .await
                .map_err(|err| {
                    StorageError::service_error(format!("Failed to propose and confirm metadata update operation through consensus: {err}"))
                })?;
        } else {
            self.get_consensus_proposal_sender()?.send(operation)?;
        }

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

    /// Cancels all transfers related to the current peer.
    ///
    /// Transfers whehre this peer is the source or the target will be cancelled.
    pub async fn cancel_related_transfers(&self, reason: &str) -> Result<(), StorageError> {
        let collections = self.collections.read().await;
        if let Some(proposal_sender) = &self.consensus_proposal_sender {
            for collection in collections.values() {
                for transfer in collection.get_related_transfers(self.this_peer_id).await {
                    let cancel_transfer =
                        ConsensusOperations::abort_transfer(collection.name(), transfer, reason);
                    proposal_sender.send(cancel_transfer)?;
                }
            }
        } else {
            log::error!(
                "Can't cancel transfers related to this node, this is a single node deployment"
            );
        }
        Ok(())
    }

    fn change_peer_state_callback(
        proposal_sender: Option<OperationSender>,
        collection_name: String,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> replica_set::ChangePeerState {
        let callback =
            Self::change_peer_from_state_callback(proposal_sender, collection_name, state);
        Arc::new(move |peer_id, shard_id| callback(peer_id, shard_id, from_state))
    }

    fn change_peer_from_state_callback(
        proposal_sender: Option<OperationSender>,
        collection_name: String,
        state: ReplicaState,
    ) -> replica_set::ChangePeerFromState {
        Arc::new(move |peer_id, shard_id, from_state| {
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
                        "Can't send proposal to deactivate replica on peer {peer_id} of shard {shard_id} of collection {collection_name}. Error: {send_error}",
                    );
                }
            } else {
                log::error!(
                    "Can't send proposal to deactivate replica. Error: this is a single node deployment",
                );
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
                        "Can't send proposal to request shard transfer to peer {to_peer} of collection {collection_name}. Error: {send_error}"
                    );
                }
            } else {
                log::error!(
                    "Can't send proposal to request shard transfer. Error: this is a single node deployment",
                );
            }
        })
    }

    fn abort_shard_transfer_callback(
        proposal_sender: Option<OperationSender>,
        collection_name: String,
    ) -> AbortShardTransfer {
        Arc::new(move |shard_transfer, reason| {
            if let Some(proposal_sender) = &proposal_sender {
                let shard_id = shard_transfer.shard_id;
                let from = shard_transfer.from;
                let to = shard_transfer.to;

                let operation = ConsensusOperations::abort_transfer(
                    collection_name.clone(),
                    shard_transfer,
                    reason,
                );

                if let Err(send_error) = proposal_sender.send(operation) {
                    log::error!(
                        "Can't send proposal to abort \
                         {collection_name}:{shard_id} / {from} -> {to} shard transfer: \
                         {send_error}",
                    );
                }
            } else {
                log::error!(
                    "Can't send proposal to abort shard transfer: \
                     this is a single node deployment",
                );
            }
        })
    }

    fn this_peer_id(&self) -> PeerId {
        self.this_peer_id
    }

    async fn create_collection_path(&self, collection_name: &str) -> Result<PathBuf, StorageError> {
        let path = self.get_collection_path(collection_name);

        if path.exists() {
            if CollectionConfigInternal::check(&path) {
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
                tokio_fs::remove_dir_all(&path).await.map_err(|err| {
                    StorageError::service_error(format!(
                        "Can't clear directory for collection {collection_name}. Error: {err}"
                    ))
                })?;
            }
        }

        tokio_fs::create_dir_all(&path).await.map_err(|err| {
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

    fn get_consensus_proposal_sender(&self) -> Result<&OperationSender, StorageError> {
        self.consensus_proposal_sender
            .as_ref()
            .ok_or_else(|| StorageError::service_error("Qdrant is running in standalone mode"))
    }

    /// Insert dispatcher for access to table of contents and consensus.
    pub fn with_toc_dispatcher(&self, dispatcher: TocDispatcher) {
        self.toc_dispatcher.lock().replace(dispatcher);
    }

    pub fn get_channel_service(&self) -> &ChannelService {
        &self.channel_service
    }

    /// Gets a copy of hardware metrics for all collections that have been collected from operations on this node.
    /// This copy is intentional to prevent 'uncontrolled' modifications of the DashMap, which doesn't need to be mutable for modifications.
    pub fn all_hw_metrics(&self) -> HashMap<String, HardwareUsage> {
        self.collection_hw_metrics
            .iter()
            .map(|i| {
                let key = i.key().to_string();
                let hw_usage = HardwareUsage {
                    cpu: i.get_cpu(),
                    payload_io_read: i.get_payload_io_read(),
                    payload_io_write: i.get_payload_io_write(),
                    payload_index_io_read: i.get_payload_index_io_read(),
                    payload_index_io_write: i.get_payload_index_io_write(),
                    vector_io_read: i.get_vector_io_read(),
                    vector_io_write: i.get_vector_io_write(),
                };
                (key, hw_usage)
            })
            .collect()
    }

    pub fn general_runtime_handle(&self) -> &Handle {
        self.general_runtime.handle()
    }
}
