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
use itertools::Itertools;
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;
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
        runtime: &runtime::Handle,
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

        self.notify_task();
        self.wait_ready().await
    }

    pub fn is_ready(&self) -> bool {
        self.is_ready.load(atomic::Ordering::Relaxed)
    }

    pub fn notify_task(&self) {
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
    pub async fn exec(self) {
        while let Err(err) = self.exec_catch_unwind().await {
            let message = common::panic::downcast_str(&err).unwrap_or("");
            let separator = if !message.is_empty() { ": " } else { "" };

            log::error!("HealthChecker task panicked, retrying{separator}{message}",);
        }
    }

    async fn exec_catch_unwind(&self) -> thread::Result<()> {
        panic::AssertUnwindSafe(self.exec_cancel())
            .catch_unwind()
            .await
    }

    async fn exec_cancel(&self) {
        let _ = cancel::future::cancel_on_token(self.cancel.clone(), self.exec_impl()).await;
    }

    async fn exec_impl(&self) {
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

        // Get estimate of current cluster commit so we can wait for it
        let Some(mut cluster_commit_index) = self.cluster_commit_index(true).await else {
            self.set_ready();
            return;
        };

        // Wait until local peer has reached cluster commit
        loop {
            while self.commit_index() < cluster_commit_index {
                // Wait for `/readyz` signal
                self.check_ready_signal.notified().await;

                // Ensure we're not the only peer left
                if self.consensus_state.peer_count() <= 1 {
                    self.set_ready();
                    return;
                }
            }

            match self.cluster_commit_index(false).await {
                // If cluster commit is still the same, we caught up and we're done
                Some(new_index) if cluster_commit_index == new_index => break,
                // Cluster commit is newer, update it and wait again
                Some(new_index) => cluster_commit_index = new_index,
                // Failed to get cluster commit, assume we're done
                None => break,
            }
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

    /// Get the highest consensus commit across cluster peers
    ///
    /// If `one_peer` is true the first fetched commit is returned. It may not necessarily be the
    /// latest commit.
    async fn cluster_commit_index(&self, one_peer: bool) -> Option<u64> {
        // Wait for `/readyz` signal
        self.check_ready_signal.notified().await;

        // Check if there is only 1 node in the cluster
        if self.consensus_state.peer_count() <= 1 {
            return None;
        }

        // Get *cluster* commit index
        let peer_address_by_id = self.consensus_state.peer_address_by_id();
        let transport_channel_pool = &self.toc.get_channel_service().channel_pool;
        let this_peer_id = self.toc.this_peer_id;
        let this_peer_uri = peer_address_by_id.get(&this_peer_id);

        let mut requests = peer_address_by_id
            .values()
            // Do not get the current commit from ourselves
            .filter(|&uri| Some(uri) != this_peer_uri)
            // Historic peers might use the same URLs as our current peers, request each URI once
            .unique()
            .map(|uri| get_consensus_commit(transport_channel_pool, uri))
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
        let sufficient_commit_indices_count = if !one_peer {
            peer_address_by_id.len() / 2
        } else {
            1
        };

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
            .last_applied_entry()
            .unwrap_or(0)
    }

    /// List shards that are unhealthy, which may undergo automatic recovery.
    ///
    /// Shards in resharding state are not considered unhealthy and are excluded here.
    /// They require an external driver to make them active or to drop them.
    async fn unhealthy_shards(&self) -> HashSet<Shard> {
        let this_peer_id = self.toc.this_peer_id;
        let collections = self
            .toc
            .all_collections(&Access::full("For health check"))
            .await;

        let mut unhealthy_shards = HashSet::new();

        for collection_pass in &collections {
            let state = match self.toc.get_collection(collection_pass).await {
                Ok(collection) => collection.state().await,
                Err(_) => continue,
            };

            for (&shard, info) in state.shards.iter() {
                let Some(state) = info.replicas.get(&this_peer_id) else {
                    continue;
                };

                if state.is_active_or_listener_or_resharding() {
                    continue;
                }

                unhealthy_shards.insert(Shard::new(collection_pass.name(), shard));
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
