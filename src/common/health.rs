use std::cmp;
use std::collections::HashSet;
use std::future::{self, Future};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::qdrant::qdrant_internal_client::QdrantInternalClient;
use api::grpc::qdrant::{GetCommitIndexRequest, GetCommitIndexResponse};
use api::grpc::transport_channel_pool::{self, TransportChannelPool};
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::ShardId;
use collection::shards::CollectionId;
use futures::stream::FuturesUnordered;
use futures::{StreamExt as _, TryStreamExt as _};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use tokio::sync::broadcast;
use tokio::{runtime, sync, task, time};

pub struct Ready {
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    runtime: runtime::Handle,
    ready: Arc<AtomicBool>,
    task: sync::RwLock<Option<task::JoinHandle<()>>>,
    check_ready: broadcast::Sender<()>,
}

impl Ready {
    pub fn new(
        toc: Arc<TableOfContent>,
        consensus_state: ConsensusStateRef,
        runtime: runtime::Handle,
    ) -> Self {
        let (check_ready, _) = broadcast::channel(1);

        Self {
            toc,
            consensus_state,
            runtime,
            ready: Default::default(),
            task: Default::default(),
            check_ready,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(atomic::Ordering::Relaxed)
    }

    pub async fn check_ready(&self) {
        if !self.is_ready() {
            self.notify_task().await;
        } else {
            self.cleanup_task().await;
        }
    }

    async fn notify_task(&self) {
        let Err(err) = self.check_ready.send(()) else {
            return;
        };

        if self.is_ready() {
            return;
        }

        log::warn!("Ready::notify_task: failed to send message to ReadyTask: {err}");
        self.spawn_task().await;
    }

    async fn spawn_task(&self) {
        if self.is_running().await {
            log::warn!("Ready::spawn_task: ReadyTask is already running");
            return;
        }

        let Some(mut task) = self.try_cleanup_task().await else {
            return;
        };

        if self.is_ready() {
            log::error!("TODO");
            return;
        }

        *task = Some(self.runtime.spawn(self.task().exec()));
    }

    async fn cleanup_task(&self) {
        let (is_running, is_spawned) = self.state().await;

        if is_running || !is_spawned {
            if is_running {
                log::warn!("Ready::cleanup_task: ReadyTask is still running");
            }

            return;
        }

        self.try_cleanup_task().await;
    }

    async fn try_cleanup_task(
        &self,
    ) -> Option<sync::RwLockWriteGuard<'_, Option<task::JoinHandle<()>>>> {
        let mut task = self.task.write().await;

        if is_task_running(task.as_ref()) {
            log::debug!("Ready::try_cleanup_task: ReadyTask is running");
            return None;
        }

        let result = match task.take() {
            Some(task) => task.await,
            None => Ok(()),
        };

        if let Err(err) = result {
            log::error!("ReadyTask failed: {err}");
        }

        Some(task)
    }

    async fn state(&self) -> (bool, bool) {
        task_state(self.task.read().await.as_ref())
    }

    async fn is_running(&self) -> bool {
        is_task_running(self.task.read().await.as_ref())
    }

    fn task(&self) -> Task {
        Task {
            ready: self.ready.clone(),
            toc: self.toc.clone(),
            consensus_state: self.consensus_state.clone(),
            check_ready: self.check_ready.subscribe(),
        }
    }
}

fn task_state<T>(task: Option<&task::JoinHandle<T>>) -> (bool, bool) {
    (is_task_running(task), is_task_spawned(task))
}

fn is_task_running<T>(task: Option<&task::JoinHandle<T>>) -> bool {
    task.map_or(false, |task| !task.is_finished())
}

fn is_task_spawned<T>(task: Option<&task::JoinHandle<T>>) -> bool {
    task.is_some()
}

pub struct Task {
    ready: Arc<AtomicBool>,
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    check_ready: broadcast::Receiver<()>,
}

impl Task {
    pub async fn exec(mut self) {
        let transport_channel_pool = self.toc.get_channel_service().channel_pool.clone();

        // TODO: Make more efficient and resilient? 🤔
        let cluster_commit_index = loop {
            let peer_address_by_id = self.consensus_state.peer_address_by_id();

            // TODO: Handle single-node cluster!
            if peer_address_by_id.is_empty() {
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            // TODO: Limit parallelism? (E.g., `StreamExt::buffer_unordered`)
            let requests: FuturesUnordered<_> = peer_address_by_id
                .values()
                .map(|uri| get_commit_index(&transport_channel_pool, uri))
                .collect();

            // TODO: Handle single-node cluster!
            let required_commit_indices_count = cmp::min(peer_address_by_id.len() / 2, 1);

            let mut requests = requests
                .inspect_err(|err| log::error!("GetCommitIndex request failed: {err}"))
                .filter_map(|res| future::ready(res.ok()));

            let mut commit_indices: Vec<_> = (&mut requests)
                .take(required_commit_indices_count)
                .collect()
                .await;

            let timeout = Instant::now() + Duration::from_secs(1);

            while let Ok(Some(resp)) = time::timeout_at(timeout.into(), requests.next()).await {
                commit_indices.push(resp);
            }

            if commit_indices.len() < required_commit_indices_count {
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let cluster_commit_index = commit_indices
                .into_iter()
                .map(|resp| resp.into_inner().commit)
                .max();

            if let Some(cluster_commit_index) = cluster_commit_index {
                break cluster_commit_index as _;
            }
        };

        while self.commit_index() < cluster_commit_index {
            if !self.check_ready().await {
                return;
            }
        }

        let mut unhealthy_shards = self.unhealthy_shards().await;

        while !unhealthy_shards.is_empty() {
            if !self.check_ready().await {
                return;
            }

            let current_unhealthy_shards = self.unhealthy_shards().await;
            unhealthy_shards.retain(|shard| current_unhealthy_shards.contains(shard));
        }

        self.ready.store(true, atomic::Ordering::Relaxed);
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

    async fn check_ready(&mut self) -> bool {
        for _ in 0..10 {
            match self.check_ready.recv().await {
                Ok(()) => return true,
                Err(broadcast::error::RecvError::Closed) => return false,
                Err(broadcast::error::RecvError::Lagged(_)) => (),
            }
        }

        log::warn!("TODO");
        true
    }

    async fn unhealthy_shards(&self) -> HashSet<Shard> {
        let this_peer_id = self.toc.this_peer_id;
        let collections = self.toc.all_collections().await;

        let mut unhealthy_shards = HashSet::new();

        for collection in &collections {
            let state = match self.toc.get_collection(collection).await {
                Ok(collection) => collection.state().await,
                Err(_) => continue,
            };

            for (&shard, info) in state.shards.iter() {
                let Some(state) = info.replicas.get(&this_peer_id) else {
                    continue;
                };

                if is_shard_ready(state) {
                    continue;
                }

                unhealthy_shards.insert(Shard::new(collection, shard));
            }
        }

        unhealthy_shards
    }
}

fn get_commit_index<'a>(
    transport_channel_pool: &'a TransportChannelPool,
    uri: &'a tonic::transport::Uri,
) -> impl Future<Output = GetCommitIndexResult> + 'a {
    transport_channel_pool.with_channel_timeout(
        uri,
        |channel| async {
            let mut client = QdrantInternalClient::new(channel);
            let mut request = tonic::Request::new(GetCommitIndexRequest {});
            request.set_timeout(Duration::from_secs(10));
            client.get_commit_index(request).await
        },
        Some(Duration::from_secs(10)),
        2,
    )
}

type GetCommitIndexResult = Result<
    tonic::Response<GetCommitIndexResponse>,
    transport_channel_pool::RequestError<tonic::Status>,
>;

fn is_shard_ready(state: &ReplicaState) -> bool {
    matches!(state, ReplicaState::Active | ReplicaState::Listener) // TODO: `ReplicaState::Initializing`?
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
