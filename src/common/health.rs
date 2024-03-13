use std::collections::HashSet;
use std::future::{self, Future};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;
use std::{panic, thread};

use api::grpc::qdrant::qdrant_internal_client::QdrantInternalClient;
use api::grpc::qdrant::{GetConsensusCommitRequest, GetConsensusCommitResponse};
use api::grpc::transport_channel_pool::{self, TransportChannelPool};
use collection::shards::shard::ShardId;
use collection::shards::CollectionId;
use common::defaults;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use tokio::{runtime, sync, time};

const READY_CHECK_TIMEOUT: Duration = Duration::from_millis(500);
const GET_CONSENSUS_COMMITS_RETRIES: usize = 2;

/// Structure used to process health checks like `/readyz` endpoints.
pub struct HealthChecker {
    // The state of the health checker.
    // Once set to `true`, it should not change back to `false`.
    // Initially set to `false`.
    is_ready: Arc<AtomicBool>,
    // The signal that notifies that state has changed.
    // Comes from the health checker task.
    is_ready_signal: Arc<sync::Notify>,
    // Signal to the health checker task, that the API was called.
    // Used to drive the health checker task and avoid constant polling.
    check_ready_signal: Arc<sync::Notify>,
    cancel: cancel::DropGuard,
}

impl HealthChecker {
    pub fn spawn(
        toc: Arc<TableOfContent>,
        consensus_state: ConsensusStateRef,
        runtime: runtime::Handle,
        wait_for_bootstrap: bool,
    ) -> Self {
        let task = Task {
            toc,
            consensus_state,
            is_ready: Default::default(),
            is_ready_signal: Default::default(),
            check_ready_signal: Default::default(),
            cancel: Default::default(),
            wait_for_bootstrap,
        };

        let health_checker = Self {
            is_ready: task.is_ready.clone(),
            is_ready_signal: task.is_ready_signal.clone(),
            check_ready_signal: task.check_ready_signal.clone(),
            cancel: task.cancel.clone().drop_guard(),
        };

        let task = runtime.spawn(task.exec());
        drop(task); // drop `JoinFuture` explicitly to make clippy happy

        health_checker
    }

    pub async fn check_ready(&self) -> bool {
        if self.is_ready() {
            return true;
        }

        self.notify_task().await;
        self.wait_ready().await
    }

    pub fn is_ready(&self) -> bool {
        self.is_ready.load(atomic::Ordering::Relaxed)
    }

    pub async fn notify_task(&self) {
        self.check_ready_signal.notify_one();
    }

    async fn wait_ready(&self) -> bool {
        let is_ready_signal = self.is_ready_signal.notified();

        if self.is_ready() {
            return true;
        }

        time::timeout(READY_CHECK_TIMEOUT, is_ready_signal)
            .await
            .is_ok()
    }
}

pub struct Task {
    toc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    // Shared state with the health checker
    // Once set to `true`, it should not change back to `false`.
    is_ready: Arc<AtomicBool>,
    // Used to notify the health checker service that the state has changed.
    is_ready_signal: Arc<sync::Notify>,
    // Driver signal for the health checker task
    // Once received, the task should proceed with an attempt to check the state.
    // Usually comes from the API call, but can be triggered by the task itself.
    check_ready_signal: Arc<sync::Notify>,
    cancel: cancel::CancellationToken,
    wait_for_bootstrap: bool,
}

impl Task {
    pub async fn exec(mut self) {
        while let Err(err) = self.exec_catch_unwind().await {
            let message = common::panic::downcast_str(&err).unwrap_or("");
            let separator = if !message.is_empty() { ": " } else { "" };

            log::error!("HealthChecker task panicked, retrying{separator}{message}",);
        }
    }

    async fn exec_catch_unwind(&mut self) -> thread::Result<()> {
        panic::AssertUnwindSafe(self.exec_cancel())
            .catch_unwind()
            .await
    }

    async fn exec_cancel(&mut self) {
        let _ = cancel::future::cancel_on_token(self.cancel.clone(), self.exec_impl()).await;
    }

    async fn exec_impl(&mut self) {
        // Wait until node joins cluster for the first time
        //
        // If this is a new deployment and `--bootstrap` CLI parameter was specified...
        if self.wait_for_bootstrap {
            // Check if this is the only node in the cluster
            while self.consensus_state.peer_count() <= 1 {
                // If cluster is empty, make another attempt to check
                // after we receive another call to `/readyz`
                //
                // Wait for `/readyz` signal
                self.check_ready_signal.notified().await;
            }
        }

        // Artificial simulate signal from `/readyz` endpoint
        // as if it was already called by the user.
        // This allows to check the happy path without waiting for the first call.
        self.check_ready_signal.notify_one();

        // Get *cluster* commit index, or check if this is the only node in the cluster
        let Some(cluster_commit_index) = self.cluster_commit_index().await else {
            self.set_ready();
            return;
        };

        // Check if *local* commit index >= *cluster* commit index...
        while self.commit_index() < cluster_commit_index {
            // If not:
            //
            // - Check if this is the only node in the cluster
            let Some(_) = self.cluster_commit_index().await else {
                self.set_ready();
                return;
            };

            // TODO: Do we want to update `cluster_commit_index` here?
            //
            // I.e.:
            // - If we *don't* update `cluster_commit_index`, then we will only wait till the node
            //   catch up with the cluster commit index *at the moment the node has been started*
            // - If we *do* update `cluster_commit_index`, then we will keep track of cluster
            //   commit index updates and wait till the node *completely* catch up with the leader,
            //   which might be hard (if not impossible) in some situations
        }

        // Collect "unhealthy" shards list
        let mut unhealthy_shards = self.unhealthy_shards().await;

        // Check if all shards are "healthy"...
        while !unhealthy_shards.is_empty() {
            // If not:
            //
            // - Wait for `/readyz` signal
            self.check_ready_signal.notified().await;

            // - Refresh "unhealthy" shards list
            let current_unhealthy_shards = self.unhealthy_shards().await;

            // - Check if any shards "healed" since last check
            unhealthy_shards.retain(|shard| current_unhealthy_shards.contains(shard));
        }

        self.set_ready();
    }

    async fn cluster_commit_index(&self) -> Option<u64> {
        // Wait for `/readyz` signal
        self.check_ready_signal.notified().await;

        // Check if there is only 1 node in the cluster
        if self.consensus_state.peer_count() <= 1 {
            return None;
        }

        // Get *cluster* commit index
        let this_peer_id = self.toc.this_peer_id;
        let transport_channel_pool = &self.toc.get_channel_service().channel_pool;

        let peer_address_by_id = self.consensus_state.peer_address_by_id();

        let mut requests = peer_address_by_id
            .iter()
            .filter_map(|(&peer_id, uri)| {
                if peer_id != this_peer_id {
                    Some(get_consensus_commit(transport_channel_pool, uri))
                } else {
                    None
                }
            })
            .collect::<FuturesUnordered<_>>()
            .inspect_err(|err| log::error!("GetConsensusCommit request failed: {err}"))
            .filter_map(|res| future::ready(res.ok()));

        // Raft commits consensus operation, after majority of nodes persisted it.
        //
        // This means, if we check the majority of nodes (e.g., `total nodes / 2 + 1`), at least one
        // of these nodes will *always* have an up-to-date commit index. And so, the highest commit
        // index among majority of nodes *is* the cluster commit index.
        //
        // Our current node *is* one of the cluster nodes, so it's enough to query `total nodes / 2`
        // *additional* nodes, to get cluster commit index.
        //
        // The check goes like this:
        // - Either at least one of the "additional" nodes return a *higher* commit index, which
        //   means our node is *not* up-to-date, and we have to wait to reach this commit index
        // - Or *all* of them return *lower* commit index, which means current node is *already*
        //   up-to-date, and `/readyz` check will pass to the next step
        //
        // Example:
        //
        // Total nodes: 2
        // Required: 2 / 2 = 1
        //
        // Total nodes: 3
        // Required: 3 / 2 = 1
        //
        // Total nodes: 4
        // Required: 4 / 2 = 2
        //
        // Total nodes: 5
        // Required: 5 / 2 = 2
        let sufficient_commit_indices_count = peer_address_by_id.len() / 2;

        // *Wait* for `total nodex / 2` successful responses...
        let mut commit_indices: Vec<_> = (&mut requests)
            .take(sufficient_commit_indices_count)
            .collect()
            .await;

        // ...and also collect any additional responses, that we might have *already* received
        while let Ok(Some(resp)) = time::timeout(Duration::ZERO, requests.next()).await {
            commit_indices.push(resp);
        }

        // Find the maximum commit index among all responses.
        //
        // Note, that we progress even if most (or even *all*) requests failed (e.g., because all
        // other nodes are unavailable or they don't support `GetConsensusCommit` gRPC API).
        //
        // So this check is not 100% reliable and can give a false-positive result!
        let cluster_commit_index = commit_indices
            .into_iter()
            .map(|resp| resp.into_inner().commit)
            .max()
            .unwrap_or(0);

        Some(cluster_commit_index as _)
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

                if state.is_active_or_listener() {
                    continue;
                }

                unhealthy_shards.insert(Shard::new(collection, shard));
            }
        }

        unhealthy_shards
    }

    fn set_ready(&self) {
        self.is_ready.store(true, atomic::Ordering::Relaxed);
        self.is_ready_signal.notify_waiters();
    }
}

fn get_consensus_commit<'a>(
    transport_channel_pool: &'a TransportChannelPool,
    uri: &'a tonic::transport::Uri,
) -> impl Future<Output = GetConsensusCommitResult> + 'a {
    transport_channel_pool.with_channel_timeout(
        uri,
        |channel| async {
            let mut client = QdrantInternalClient::new(channel);
            let mut request = tonic::Request::new(GetConsensusCommitRequest {});
            request.set_timeout(defaults::CONSENSUS_META_OP_WAIT);
            client.get_consensus_commit(request).await
        },
        Some(defaults::CONSENSUS_META_OP_WAIT),
        GET_CONSENSUS_COMMITS_RETRIES,
    )
}

type GetConsensusCommitResult = Result<
    tonic::Response<GetConsensusCommitResponse>,
    transport_channel_pool::RequestError<tonic::Status>,
>;

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
