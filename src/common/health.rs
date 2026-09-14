use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicBool};
use std::time::Duration;
use std::{panic, thread};

use collection::shards::CollectionId;
use collection::shards::shard::ShardId;
use futures::FutureExt as _;
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;
use tokio::{runtime, sync, time};

const READY_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

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
    _cancel: cancel::DropGuard,
}

impl HealthChecker {
    pub fn spawn(
        toc: Arc<TableOfContent>,
        consensus_state: ConsensusStateRef,
        runtime: &runtime::Handle,
    ) -> Self {
        let task = Task {
            toc,
            consensus_state,
            is_ready: Default::default(),
            is_ready_signal: Default::default(),
            check_ready_signal: Default::default(),
            cancel: Default::default(),
        };

        let health_checker = Self {
            is_ready: task.is_ready.clone(),
            is_ready_signal: task.is_ready_signal.clone(),
            check_ready_signal: task.check_ready_signal.clone(),
            _cancel: task.cancel.clone().drop_guard(),
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
        // Wait until this peer has caught up with the consensus commit of the cluster, see
        // `ConsensusStateRef::spawn_consensus_catch_up`
        while !self.consensus_state.is_consensus_caught_up.check_ready() {
            // Wait for `/readyz` signal
            self.check_ready_signal.notified().await;
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
            let Ok(collection) = self.toc.get_collection(collection_pass).await else {
                continue;
            };

            let shards_holder = collection.shards_holder();
            let shards_holder = shards_holder.read().await;

            for (shard, replica_set) in shards_holder.get_shards() {
                let Some(state) = replica_set.peer_state(this_peer_id) else {
                    continue;
                };

                if state.is_healthy() {
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
