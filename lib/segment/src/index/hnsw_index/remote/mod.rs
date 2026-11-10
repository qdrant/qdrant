//! Remote indexing: the HNSW graph of one vector of a segment under construction is built by
//! an external indexing service, which may be shared between clusters and may have a GPU.
//!
//! Core side: `GraphBuilder::Remote` registers a task in [`registry::REMOTE_INDEXING_TASKS`],
//! submits it through the [`RemoteIndexer`] registered by the server, and waits for the graph.
//! The server exposes the registry to the indexer over internal gRPC.
//!
//! Indexer side: [`executor`] rebuilds a storage from what it reads and runs the same graph
//! builders as core. [`loopback`] wires both sides in-process, for tests and debugging.
//!
//! `lib/segment` has no gRPC: the server registers the transport as a [`RemoteIndexer`].

pub mod executor;
pub mod layout;
pub mod loopback;
pub mod metrics;
pub mod registry;
pub mod wire;

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use uuid::Uuid;

pub use self::registry::{TaskScope, TaskToken};
pub use crate::index::hnsw_index::entry_points::EntryPoint;

/// What to do when the remote build fails: the indexer is unreachable, reports a failure, the
/// lease expires, or the graph is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnFailure {
    /// Build locally right away.
    Local,
    /// Submit again with a new token, up to `max_retries` times, then build locally.
    Retry,
}

#[derive(Debug, Clone)]
pub struct RemoteIndexingConfig {
    /// Go remote only if the vector data of the vector name is at least this large (D11).
    pub min_index_size_bytes: usize,
    /// Lease of a running task: it is given up if the indexer does not call within this
    /// time (D6).
    pub heartbeat_timeout: Duration,
    /// Lease of a submitted task until the indexer fetches it with `GetTask`. Separate from
    /// `heartbeat_timeout`, because nothing heartbeats a task waiting in a queue.
    pub queue_timeout: Duration,
    pub on_failure: OnFailure,
    /// Resubmissions with `OnFailure::Retry`.
    pub max_retries: u32,
}

/// A task as sent to the entry point (§4.8).
#[derive(Debug, Clone)]
pub struct TaskSubmission {
    pub task_id: Uuid,
    pub token: TaskToken,
    pub scope: TaskScope,
    /// Size of the vector data and number of points, for scheduling and billing.
    pub vector_bytes: u64,
    pub point_count: u64,
    /// Graph formats core reads (D9).
    pub graph_format_versions: Vec<u32>,
    /// 0 for the first submission, then one more per retry.
    pub attempt: u32,
}

/// Hands tasks to the indexing service. Implemented by the server (gRPC or HTTP entry
/// points) and by [`loopback::LoopbackIndexer`].
pub trait RemoteIndexer: Send + Sync {
    fn config(&self) -> &RemoteIndexingConfig;

    /// Hand `task` to the entry point. Called from the optimizer thread, which may block, but
    /// never from an async context: implementations must not use the caller's runtime.
    /// Must return within bounded time.
    fn submit(&self, task: &TaskSubmission) -> Result<(), String>;
}

static REMOTE_INDEXER: RwLock<Option<Arc<dyn RemoteIndexer>>> = RwLock::new(None);

thread_local! {
    static THREAD_REMOTE_INDEXER: RefCell<Option<Arc<dyn RemoteIndexer>>> = const { RefCell::new(None) };
}

/// Register the process-wide indexer, at startup. `None` turns remote indexing off.
pub fn set_remote_indexer(indexer: Option<Arc<dyn RemoteIndexer>>) {
    *REMOTE_INDEXER.write() = indexer;
}

/// The indexer for builds on this thread: the one set by [`with_thread_remote_indexer`], or
/// the process-wide one.
pub fn remote_indexer() -> Option<Arc<dyn RemoteIndexer>> {
    THREAD_REMOTE_INDEXER
        .with_borrow(|indexer| indexer.clone())
        .or_else(|| REMOTE_INDEXER.read().clone())
}

/// Run `f` with `indexer` used by builds on this thread only. For tests, which share the
/// process with other builds.
pub fn with_thread_remote_indexer<T>(indexer: Arc<dyn RemoteIndexer>, f: impl FnOnce() -> T) -> T {
    struct Reset(Option<Arc<dyn RemoteIndexer>>);
    impl Drop for Reset {
        fn drop(&mut self) {
            THREAD_REMOTE_INDEXER.set(self.0.take());
        }
    }
    let _reset = Reset(THREAD_REMOTE_INDEXER.replace(Some(indexer)));
    f()
}
