use std::collections::HashSet;
use std::sync::atomic::{self, AtomicBool, AtomicU64};
use std::sync::Arc;

use api::grpc::transport_channel_pool::TransportChannelPool;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::ShardId;
use collection::shards::CollectionId;
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use storage::types::PeerAddressById;
use tokio::{runtime, sync, task};

pub struct Ready {
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    peer_address_by_id: PeerAddressById,
    transport_channel_pool: Arc<TransportChannelPool>,
    runtime: runtime::Handle,
    cluster_commit_index_task: sync::Mutex<Option<task::JoinHandle<u64>>>,
    cluster_commit_index: AtomicU64,
    is_consensus_ready: AtomicBool,
    unhealthy_shards: sync::RwLock<Option<HashSet<Shard>>>,
}

impl Ready {
    pub fn new(
        toc: Arc<TableOfContent>,
        consensus_state: ConsensusStateRef,
        peer_address_by_id: PeerAddressById,
        transport_channel_pool: Arc<TransportChannelPool>,
        runtime: runtime::Handle,
    ) -> Self {
        Self {
            toc,
            consensus_state,
            peer_address_by_id,
            transport_channel_pool,
            runtime,
            cluster_commit_index_task: Default::default(),
            cluster_commit_index: Default::default(),
            is_consensus_ready: Default::default(),
            unhealthy_shards: Default::default(),
        }
    }

    pub async fn check_ready(&self) -> bool {
        if self.check_consensus_ready().await {
            self.check_shards_health().await
        } else {
            false
        }
    }

    async fn check_consensus_ready(&self) -> bool {
        if self.is_consensus_ready.load(atomic::Ordering::Relaxed) {
            return true;
        }

        let Some(cluster_commit_index) = self.cluster_commit_index().await else {
            return false;
        };

        if cluster_commit_index > self.commit_index() {
            return false;
        }

        self.is_consensus_ready
            .store(true, atomic::Ordering::Relaxed);
        true
    }

    async fn cluster_commit_index(&self) -> Option<u64> {
        if let Some(cluster_commit_index) = self.get_cluster_commit_index() {
            return Some(cluster_commit_index);
        }

        self.check_task().await
    }

    async fn check_task(&self) -> Option<u64> {
        let mut task = self.cluster_commit_index_task.lock().await;

        if let Some(cluster_commit_index) = self.get_cluster_commit_index() {
            return Some(cluster_commit_index);
        }

        let mut result = None;

        let is_task_finished = task.as_ref().map_or(false, |task| task.is_finished());

        if is_task_finished {
            if let Some(task) = task.take() {
                result = Some(task.await);
            }
        }

        match result {
            Some(Ok(cluster_commit_index)) => {
                // A bit of extra safety...
                assert!(task.is_none());

                self.cluster_commit_index
                    .store(cluster_commit_index, atomic::Ordering::Relaxed);
                Some(cluster_commit_index)
            }

            Some(Err(err)) => {
                log::error!("ClusterCommitIndex task failed: {err}");

                // A bit of extra safety...
                assert!(task.is_none());

                self.spawn_task(task.into()).await;
                None
            }

            None => {
                log::warn!("ClusterCommitIndex task is not spawned");

                if task.is_none() {
                    self.spawn_task(task.into()).await;
                }

                None
            }
        }
    }

    fn get_cluster_commit_index(&self) -> Option<u64> {
        let cluster_commit_index = self.cluster_commit_index.load(atomic::Ordering::Relaxed);

        if cluster_commit_index > 0 {
            Some(cluster_commit_index)
        } else {
            None
        }
    }

    async fn spawn_task(&self, task: Option<sync::MutexGuard<'_, Option<task::JoinHandle<u64>>>>) {
        let mut task = match task {
            Some(task) => task,
            None => self.cluster_commit_index_task.lock().await,
        };

        if task.is_none() {
            task.insert(self.runtime.spawn(self.task().exec()));
        } else {
            log::warn!("ClusterCommitIndex task is already spawned");
        }
    }

    fn task(&self) -> ClusterCommitIndex {
        ClusterCommitIndex {
            peer_address_by_id: self.peer_address_by_id.clone(),
            transport_channel_pool: self.transport_channel_pool.clone(),
        }
    }

    fn commit_index(&self) -> u64 {
        // TODO: Blocking call in async context!?
        self.consensus_state
            .persistent
            .read()
            .state
            .hard_state
            .commit
    }

    async fn check_shards_health(&self) -> bool {
        let (is_healthy, is_initialized) = {
            let unhealthy_shards = self.unhealthy_shards.read().await;

            let is_healthy = unhealthy_shards
                .as_ref()
                .map_or(false, |unhealthy_shards| unhealthy_shards.is_empty());

            let is_initialized = unhealthy_shards.is_some();

            (is_healthy, is_initialized)
        };

        if is_healthy {
            return true;
        }

        if !is_initialized {
            self.initialize_unhealthy_shards().await
        } else {
            self.remove_healthy_shards().await
        }
    }

    async fn initialize_unhealthy_shards(&self) -> bool {
        // Collect "unhealthy" shards
        let shards = filter(self.shards().await, false);

        let mut unhealthy_shards = self.unhealthy_shards.write().await;

        // TODO: Clarify somehow!? 😭

        if let Some(unhealthy_shards) = unhealthy_shards.as_ref() {
            log::warn!("unhealthy shards list is already initialized");
            return unhealthy_shards.is_empty();
        }

        unhealthy_shards.insert(shards.collect()).is_empty()
    }

    async fn remove_healthy_shards(&self) -> bool {
        // Collect "healthy" shards
        let shards = filter(self.shards().await, true);

        let mut unhealthy_shards = self.unhealthy_shards.write().await;

        let Some(unhealthy_shards) = unhealthy_shards.as_mut() else {
            log::warn!("unhealthy shards list is not initialized");
            return false;
        };

        if unhealthy_shards.is_empty() {
            return true;
        }

        for shard in shards {
            unhealthy_shards.remove(&shard);
        }

        unhealthy_shards.is_empty()
    }

    async fn shards(&self) -> Vec<(Shard, bool)> {
        let this_peer_id = self.toc.this_peer_id;
        let collections = self.toc.all_collections().await;

        let mut shards = Vec::new();

        for collection in &collections {
            let state = match self.toc.get_collection(collection).await {
                Ok(collection) => collection.state().await,
                Err(_) => continue,
            };

            for (&shard, info) in state.shards.iter() {
                let Some(&state) = info.replicas.get(&this_peer_id) else {
                    continue;
                };

                shards.push((Shard::new(collection, shard), is_ready(state)));
            }
        }

        shards
    }
}

#[derive(Clone)]
struct ClusterCommitIndex {
    peer_address_by_id: PeerAddressById,
    transport_channel_pool: Arc<TransportChannelPool>,
}

impl ClusterCommitIndex {
    pub async fn exec(self) -> u64 {
        todo!()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Shard {
    collection: CollectionId,
    shard: ShardId,
}

impl Shard {
    pub fn new(collection: impl Into<CollectionId>, shard: ShardId) -> Self {
        Self {
            collection: collection.into(),
            shard,
        }
    }
}

fn is_ready(state: ReplicaState) -> bool {
    matches!(state, ReplicaState::Active | ReplicaState::Listener) // TODO: `ReplicaState::Initializing`?
}

fn filter(
    shards: impl IntoIterator<Item = (Shard, bool)>,
    filter: bool,
) -> impl Iterator<Item = Shard> {
    shards.into_iter().filter_map(move |(shard, is_ready)| {
        if is_ready == filter {
            Some(shard)
        } else {
            None
        }
    })
}
