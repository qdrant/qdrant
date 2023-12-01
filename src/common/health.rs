use std::collections::HashSet;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::qdrant_internal_client::QdrantInternalClient;
use api::grpc::qdrant::GetCommitIndexRequest;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::ShardId;
use collection::shards::CollectionId;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use tokio::sync::broadcast;
use tokio::{runtime, sync, task};

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

    pub async fn check_ready(&self) -> bool {
        let is_ready = self.is_ready();

        if !is_ready {
            self.notify_task().await;
        } else {
            self.cleanup_task().await;
        }

        is_ready
    }

    fn is_ready(&self) -> bool {
        self.ready.load(atomic::Ordering::Relaxed)
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
    task.map_or(false, |task| task.is_finished())
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
            let mut requests = FuturesUnordered::new();

            for (_, uri) in &peer_address_by_id {
                let request = transport_channel_pool.with_channel_timeout(
                    &uri,
                    |channel| async {
                        let mut client = QdrantInternalClient::new(channel);
                        let mut request = tonic::Request::new(GetCommitIndexRequest {});
                        request.set_timeout(Duration::from_secs(10));
                        client.get_commit_index(request).await
                    },
                    Some(Duration::from_secs(10)),
                    2,
                );

                requests.push(request);
            }

            let mut commit_indices = Vec::new();

            while let Some(result) = requests.next().await {
                match result {
                    Ok(resp) => commit_indices.push(resp),
                    Err(err) => log::error!("GetCommiIndex request failed: {err}"),
                }
            }

            if commit_indices.len() < peer_address_by_id.len() / 2 {
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

        loop {
            match self.check_ready.recv().await {
                Ok(()) => (),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            }

            // TODO: Blocking call in async context!?
            let commit_index = self
                .consensus_state
                .persistent
                .read()
                .state
                .hard_state
                .commit;

            if commit_index >= cluster_commit_index {
                break;
            }
        }

        let mut unhealthy_shards: HashSet<_> = filter_shards(self.shards().await, false).collect();

        loop {
            match self.check_ready.recv().await {
                Ok(()) => (),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            }

            for shard in filter_shards(self.shards().await, true) {
                unhealthy_shards.remove(&shard);

                if unhealthy_shards.is_empty() {
                    break;
                }
            }

            if unhealthy_shards.is_empty() {
                break;
            }
        }

        self.ready.store(true, atomic::Ordering::Relaxed);
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

                shards.push((Shard::new(collection, shard), is_shard_ready(state)));
            }
        }

        shards
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

fn is_shard_ready(state: ReplicaState) -> bool {
    matches!(state, ReplicaState::Active | ReplicaState::Listener) // TODO: `ReplicaState::Initializing`?
}

fn filter_shards(
    shards: impl IntoIterator<Item = (Shard, bool)>,
    is_ready_filter: bool,
) -> impl Iterator<Item = Shard> {
    shards.into_iter().filter_map(move |(shard, is_ready)| {
        if is_ready == is_ready_filter {
            Some(shard)
        } else {
            None
        }
    })
}
