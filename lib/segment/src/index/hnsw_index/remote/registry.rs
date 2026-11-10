//! Process-wide registry of remote indexing tasks (§4.4).
//!
//! The optimizer thread registers a task and waits on it; the `IndexingTask` gRPC service
//! serves the indexer through the same registry, by token. A token gives access to one vector
//! of one segment under construction only, and is revoked on any terminal state.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use atomic_refcell::AtomicRefCell;
use bitvec::vec::BitVec;
use common::types::PointOffsetType;
use parking_lot::{Condvar, Mutex, RwLock};
use rand::RngExt as _;
use uuid::Uuid;

use super::metrics::REMOTE_INDEXING_METRICS;
use super::wire::{BlockPlan, GraphLimits, RemoteGraphParams, WireGraph};
use common::generic_consts::Random;

use crate::common::operation_error::OperationError;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// One-time task token: 256 random bits. Sent as hex in a metadata header.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskToken([u8; 32]);

impl TaskToken {
    pub fn random() -> Self {
        let mut bytes = [0u8; 32];
        rand::rng().fill(&mut bytes);
        Self(bytes)
    }

    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn from_hex(hex: &str) -> Option<Self> {
        let hex = hex.trim();
        if hex.len() != 64 || !hex.is_ascii() {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
        }
        Some(Self(bytes))
    }
}

/// Never print the token itself.
impl fmt::Debug for TaskToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TaskToken({}..)", &self.to_hex()[..6])
    }
}

/// Why a request on a token is refused.
#[derive(Debug, thiserror::Error)]
pub enum RemoteTaskError {
    /// Unknown, revoked or finished token. Says nothing more, so tokens cannot be probed.
    #[error("invalid or revoked indexing task token")]
    PermissionDenied,
    #[error("invalid request: {0}")]
    InvalidArgument(String),
    #[error("{0}")]
    Internal(String),
}

impl From<OperationError> for RemoteTaskError {
    fn from(err: OperationError) -> Self {
        RemoteTaskError::Internal(err.to_string())
    }
}

/// What a task gives access to, for logs and billing.
#[derive(Debug, Clone)]
pub struct TaskScope {
    /// The vector index directory of the segment under construction.
    pub index_path: PathBuf,
    pub vector_name: String,
}

/// What the indexer reads with `GetTask`.
#[derive(Debug, Clone)]
pub struct TaskDescription {
    pub task_id: Uuid,
    pub params: RemoteGraphParams,
    pub block_plan: Arc<BlockPlan>,
}

/// Progress reported with a heartbeat.
#[derive(Debug, Clone, Default)]
pub struct TaskProgress {
    pub phase: String,
    pub done: u64,
    pub total: u64,
}

/// Reply to a heartbeat (D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatAction {
    Continue,
    Cancel,
}

/// How a task ended, as seen by the waiting builder.
#[derive(Debug)]
pub enum TaskOutcome {
    /// The indexer uploaded a graph that passed validation.
    Graph(WireGraph),
    /// The indexer reported a failure.
    Failed(String),
    /// The uploaded graph did not pass validation.
    Rejected(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskStatus {
    /// Submitted, the indexer has not called `GetTask` yet.
    Queued,
    /// The indexer is working on it.
    Running,
    /// Terminal: the outcome is in `TaskInner::outcome`.
    Finished,
}

struct TaskInner {
    status: TaskStatus,
    /// Renewed by every call from the indexer (D6).
    last_seen: Instant,
    progress: TaskProgress,
    outcome: Option<TaskOutcome>,
    queued_at: Instant,
    started_at: Option<Instant>,
}

/// State of one registered task. Owned by the registry and by the builder's [`TaskHandle`].
pub struct TaskState {
    task_id: Uuid,
    scope: TaskScope,
    description: TaskDescription,
    /// Read-only: writes to the segment go to the proxies while it is built (§4.4).
    /// Taken on revoke, under the write lock, which waits for reads in flight: the segment
    /// builder must be the only owner again once the task ends (graph-inline finalization).
    vector_storage: RwLock<Option<Arc<AtomicRefCell<VectorStorageEnum>>>>,
    /// One flag per point: set if the point is not linked (deleted, or without vector).
    deleted: Arc<BitVec>,
    /// Files the indexer may download, by the name it asks for. Nothing else is readable.
    files: HashMap<String, PathBuf>,
    inner: Mutex<TaskInner>,
    finished: Condvar,
    bytes_served: AtomicU64,
    /// Done counter of the progress phase shown in the optimizations view.
    progress_counter: Mutex<Option<Arc<AtomicU64>>>,
}

impl TaskState {
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }

    pub fn scope(&self) -> &TaskScope {
        &self.scope
    }

    pub fn bytes_served(&self) -> u64 {
        self.bytes_served.load(Ordering::Relaxed)
    }

    fn touch(&self) {
        self.inner.lock().last_seen = Instant::now();
    }
}

/// Why a token was revoked, kept for a while so that a late heartbeat gets `CANCEL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tombstone {
    /// Core gave up on the task: stopped, timed out, or the builder is gone.
    Cancelled,
    /// The task finished: uploaded or failed.
    Finished,
}

/// How long revoked tokens answer heartbeats with `CANCEL` instead of `PERMISSION_DENIED`.
const TOMBSTONE_TTL: Duration = Duration::from_secs(600);

/// Max size of one `ReadVectors` chunk.
pub const READ_CHUNK_BYTES: usize = 1 << 20;

#[derive(Default)]
pub struct TaskRegistry {
    tasks: Mutex<HashMap<TaskToken, Arc<TaskState>>>,
    tombstones: Mutex<HashMap<TaskToken, (Instant, Tombstone)>>,
}

/// The registry of this process.
pub static REMOTE_INDEXING_TASKS: LazyLock<TaskRegistry> = LazyLock::new(TaskRegistry::default);

/// What the builder registers.
pub struct NewTask {
    pub scope: TaskScope,
    pub params: RemoteGraphParams,
    pub block_plan: Arc<BlockPlan>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub deleted: Arc<BitVec>,
    /// Files served by `read_file`: name sent to the indexer → path on disk.
    pub files: Vec<(String, PathBuf)>,
}

impl TaskRegistry {
    /// Register a task under a new token. The task is revoked when the handle is dropped.
    pub fn register(&'static self, task: NewTask) -> TaskHandle {
        let NewTask {
            scope,
            params,
            block_plan,
            vector_storage,
            deleted,
            files,
        } = task;
        let task_id = Uuid::new_v4();
        let now = Instant::now();
        let state = Arc::new(TaskState {
            task_id,
            scope,
            description: TaskDescription {
                task_id,
                params,
                block_plan,
            },
            vector_storage: RwLock::new(Some(vector_storage)),
            deleted,
            files: files.into_iter().collect(),
            inner: Mutex::new(TaskInner {
                status: TaskStatus::Queued,
                last_seen: now,
                progress: TaskProgress::default(),
                outcome: None,
                queued_at: now,
                started_at: None,
            }),
            finished: Condvar::new(),
            bytes_served: AtomicU64::new(0),
            progress_counter: Mutex::new(None),
        });
        let token = TaskToken::random();
        self.tasks.lock().insert(token, state.clone());
        REMOTE_INDEXING_METRICS.tasks_registered.fetch_add(1, Ordering::Relaxed);
        TaskHandle {
            registry: self,
            token,
            state,
        }
    }

    fn get(&self, token: &TaskToken) -> Result<Arc<TaskState>, RemoteTaskError> {
        self.tasks
            .lock()
            .get(token)
            .cloned()
            .ok_or(RemoteTaskError::PermissionDenied)
    }

    fn revoke(&self, token: &TaskToken, reason: Tombstone) {
        let removed = self.tasks.lock().remove(token);
        if let Some(state) = removed {
            // Waits for reads in flight
            state.vector_storage.write().take();
            let mut tombstones = self.tombstones.lock();
            let now = Instant::now();
            tombstones.retain(|_, (at, _)| now.duration_since(*at) < TOMBSTONE_TTL);
            tombstones.insert(*token, (now, reason));
        }
    }

    /// Number of live tasks.
    pub fn len(&self) -> usize {
        self.tasks.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // --- Indexer side, called by the gRPC service ---

    /// `GetTask`: the task parameters and the block plan. Marks the task as running.
    pub fn get_task(&self, token: &TaskToken) -> Result<TaskDescription, RemoteTaskError> {
        let state = self.get(token)?;
        let mut inner = state.inner.lock();
        let now = Instant::now();
        inner.last_seen = now;
        if inner.status == TaskStatus::Queued {
            inner.status = TaskStatus::Running;
            inner.started_at = Some(now);
            REMOTE_INDEXING_METRICS
                .queued_time_ms
                .fetch_add(now.duration_since(inner.queued_at).as_millis() as u64, Ordering::Relaxed);
        }
        Ok(state.description.clone())
    }

    /// `ReadVectors`: storage-native bytes of the vectors from `start`, until `end` or about
    /// [`READ_CHUNK_BYTES`]. An empty entry means the point has no vector. Returns the vectors
    /// and the offset to continue from.
    ///
    /// Reads from storage, which may do IO: call from a blocking thread.
    pub fn read_vectors(
        &self,
        token: &TaskToken,
        start: PointOffsetType,
        end: PointOffsetType,
    ) -> Result<(Vec<Vec<u8>>, PointOffsetType), RemoteTaskError> {
        let state = self.get(token)?;
        state.touch();
        let point_count = state.description.params.point_count as PointOffsetType;
        let end = end.min(point_count);
        if start > end {
            return Err(RemoteTaskError::InvalidArgument(format!(
                "range {start}..{end} out of 0..{point_count}"
            )));
        }
        // Serving the indexer is not a request: nothing to measure
        let _scope = common::ambient::unmeasured_guard(common::reason::reason(
            "remote indexing reads, not a request",
        ));
        let storage_guard = state.vector_storage.read();
        let Some(storage) = storage_guard.as_ref() else {
            return Err(RemoteTaskError::PermissionDenied);
        };
        let storage = storage.borrow();
        let mut vectors = Vec::new();
        let mut size = 0;
        let mut next = start;
        while next < end && size < READ_CHUNK_BYTES {
            let bytes = if state.deleted[next as usize] {
                // Not linked: no need to send it
                Vec::new()
            } else {
                storage
                    .vector_bytes_opt::<Random>(next)?
                    .unwrap_or_default()
            };
            size += bytes.len().max(1);
            vectors.push(bytes);
            next += 1;
        }
        state.bytes_served.fetch_add(size as u64, Ordering::Relaxed);
        REMOTE_INDEXING_METRICS
            .bytes_served
            .fetch_add(size as u64, Ordering::Relaxed);
        Ok((vectors, next))
    }

    /// `ReadFile`: up to `max_len` bytes of a file of the task, from `offset`. Empty at the
    /// end of the file. Only the files registered with the task can be read.
    ///
    /// Reads from disk: call from a blocking thread.
    pub fn read_file(
        &self,
        token: &TaskToken,
        name: &str,
        offset: u64,
        max_len: usize,
    ) -> Result<Vec<u8>, RemoteTaskError> {
        use std::io::{Read as _, Seek as _, SeekFrom};

        let state = self.get(token)?;
        state.touch();
        let Some(path) = state.files.get(name) else {
            return Err(RemoteTaskError::InvalidArgument(format!(
                "no file {name:?} in this task"
            )));
        };
        let io = |err: std::io::Error| RemoteTaskError::Internal(format!("{name}: {err}"));
        let mut file = fs_err::File::open(path).map_err(io)?;
        file.seek(SeekFrom::Start(offset)).map_err(io)?;
        let mut data = Vec::with_capacity(max_len.min(READ_CHUNK_BYTES));
        file.take(max_len as u64)
            .read_to_end(&mut data)
            .map_err(io)?;
        state
            .bytes_served
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        REMOTE_INDEXING_METRICS
            .bytes_served
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    /// `ReadDeleted`: one flag per point, set if the point must not be linked.
    pub fn read_deleted(&self, token: &TaskToken) -> Result<Arc<BitVec>, RemoteTaskError> {
        let state = self.get(token)?;
        state.touch();
        Ok(state.deleted.clone())
    }

    /// `Heartbeat`: renew the lease and report progress (D6). A revoked task answers `CANCEL`
    /// for a while, so the indexer stops promptly.
    pub fn heartbeat(
        &self,
        token: &TaskToken,
        progress: TaskProgress,
    ) -> Result<HeartbeatAction, RemoteTaskError> {
        let state = match self.get(token) {
            Ok(state) => state,
            Err(err) => {
                return match self.tombstones.lock().get(token) {
                    Some((_, Tombstone::Cancelled)) => Ok(HeartbeatAction::Cancel),
                    Some((_, Tombstone::Finished)) | None => Err(err),
                };
            }
        };
        REMOTE_INDEXING_METRICS.heartbeats.fetch_add(1, Ordering::Relaxed);
        if let Some(counter) = state.progress_counter.lock().as_ref() {
            counter.store(progress.done, Ordering::Relaxed);
        }
        let mut inner = state.inner.lock();
        inner.last_seen = Instant::now();
        inner.progress = progress;
        Ok(HeartbeatAction::Continue)
    }

    /// `UploadGraph`: validate the graph and hand it to the waiting builder. Revokes the token
    /// whatever the outcome: a rejected graph is not retried on the same token.
    pub fn upload(&self, token: &TaskToken, graph: WireGraph) -> Result<(), RemoteTaskError> {
        let state = self.get(token)?;
        let limits = GraphLimits::new(
            &state.description.params,
            &state.description.block_plan,
            &state.deleted,
        );
        let _scope = common::ambient::unmeasured_guard(common::reason::reason(
            "remote indexing upload, not a request",
        ));
        let validation = graph.validate(&limits);
        let size = graph.size_bytes();
        let outcome = match &validation {
            Ok(()) => {
                REMOTE_INDEXING_METRICS
                    .graph_bytes_received
                    .fetch_add(size as u64, Ordering::Relaxed);
                TaskOutcome::Graph(graph)
            }
            Err(err) => {
                REMOTE_INDEXING_METRICS
                    .validation_failures
                    .fetch_add(1, Ordering::Relaxed);
                log::warn!(
                    "Remote indexing task {} uploaded an invalid graph: {err}",
                    state.task_id,
                );
                TaskOutcome::Rejected(err.to_string())
            }
        };
        self.finish(token, &state, outcome);
        validation.map_err(|err| RemoteTaskError::InvalidArgument(err.to_string()))
    }

    /// `Fail`: the indexer gives up on the task.
    pub fn fail(&self, token: &TaskToken, reason: String) -> Result<(), RemoteTaskError> {
        let state = self.get(token)?;
        log::warn!(
            "Remote indexing task {} failed on the indexer: {reason}",
            state.task_id,
        );
        self.finish(token, &state, TaskOutcome::Failed(reason));
        Ok(())
    }

    fn finish(&self, token: &TaskToken, state: &TaskState, outcome: TaskOutcome) {
        {
            let mut inner = state.inner.lock();
            if inner.status == TaskStatus::Finished {
                return;
            }
            inner.status = TaskStatus::Finished;
            inner.outcome = Some(outcome);
        }
        self.revoke(token, Tombstone::Finished);
        state.finished.notify_all();
    }
}

/// Why waiting on a task ended without an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitInterrupt {
    /// The `stopped` flag was set.
    Stopped,
    /// No call from the indexer within the lease.
    LeaseExpired,
    /// The indexer did not fetch the task within the queue lease.
    QueueTimeout,
}

/// The builder's side of a task. Revokes the token when dropped.
pub struct TaskHandle {
    registry: &'static TaskRegistry,
    token: TaskToken,
    state: Arc<TaskState>,
}

impl TaskHandle {
    pub fn token(&self) -> TaskToken {
        self.token
    }

    pub fn task_id(&self) -> Uuid {
        self.state.task_id
    }

    pub fn state(&self) -> &Arc<TaskState> {
        &self.state
    }

    /// Mirror heartbeat progress into this counter.
    pub fn set_progress_counter(&self, counter: Arc<AtomicU64>) {
        *self.state.progress_counter.lock() = Some(counter);
    }

    /// Block until the task finishes, `should_stop` returns true, or no call from the indexer
    /// arrives within the lease (D6: there is no hard deadline): `queue_lease` until the
    /// indexer fetches the task, `lease` after. Wakes up every `poll` to check.
    pub fn wait(
        &self,
        queue_lease: Duration,
        lease: Duration,
        poll: Duration,
        should_stop: impl Fn() -> bool,
    ) -> Result<TaskOutcome, WaitInterrupt> {
        let mut inner = self.state.inner.lock();
        loop {
            if let Some(outcome) = inner.outcome.take() {
                if let Some(started) = inner.started_at {
                    REMOTE_INDEXING_METRICS
                        .running_time_ms
                        .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                }
                return Ok(outcome);
            }
            if should_stop() {
                return Err(WaitInterrupt::Stopped);
            }
            match inner.status {
                TaskStatus::Queued if inner.last_seen.elapsed() > queue_lease => {
                    return Err(WaitInterrupt::QueueTimeout);
                }
                TaskStatus::Running if inner.last_seen.elapsed() > lease => {
                    return Err(WaitInterrupt::LeaseExpired);
                }
                TaskStatus::Queued | TaskStatus::Running | TaskStatus::Finished => {}
            }
            self.state.finished.wait_for(&mut inner, poll);
        }
    }

    /// Last progress reported by the indexer.
    pub fn progress(&self) -> TaskProgress {
        self.state.inner.lock().progress.clone()
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        // Only still registered if the builder gave up: stopped, lease expired, or error
        self.registry.revoke(&self.token, Tombstone::Cancelled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::vectors::VectorRef;
    use crate::index::hnsw_index::remote::wire::{
        VECTOR_REPRESENTATION_STORAGE_NATIVE, VectorSource,
    };
    use crate::types::{Distance, VectorStorageDatatype};
    use crate::vector_storage::VectorStorage as _;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;

    fn new_task(point_count: usize) -> (NewTask, Arc<AtomicRefCell<VectorStorageEnum>>) {
        let mut storage = new_volatile_dense_vector_storage(2, Distance::Dot);
        for i in 0..point_count {
            let v = [i as f32, 1.0];
            storage
                .insert_vector(i as PointOffsetType, VectorRef::from(&v[..]))
                .unwrap();
        }
        let storage = Arc::new(AtomicRefCell::new(storage));
        let mut deleted = BitVec::repeat(false, point_count);
        deleted.set(1, true);
        let task = NewTask {
            scope: TaskScope {
                index_path: PathBuf::from("/tmp/x"),
                vector_name: String::new(),
            },
            params: RemoteGraphParams {
                vector_name: String::new(),
                dim: 2,
                datatype: VectorStorageDatatype::Float32,
                distance: Distance::Dot,
                multivector_config: None,
                m: 4,
                m0: 8,
                ef_construct: 8,
                entry_points_num: 1,
                payload_m: 4,
                payload_m0: 8,
                point_count,
                indexed_point_count: point_count - 1,
                vector_representation: VECTOR_REPRESENTATION_STORAGE_NATIVE.to_string(),
                vector_source: VectorSource::Storage,
                files: Vec::new(),
            },
            block_plan: Arc::new(BlockPlan::default()),
            vector_storage: storage.clone(),
            deleted: Arc::new(deleted),
            files: Vec::new(),
        };
        (task, storage)
    }

    #[test]
    fn token_lifecycle() {
        let (task, storage) = new_task(5);
        let handle = REMOTE_INDEXING_TASKS.register(task);
        let token = handle.token();
        assert_eq!(Arc::strong_count(&storage), 2);

        let description = REMOTE_INDEXING_TASKS.get_task(&token).unwrap();
        assert_eq!(description.task_id, handle.task_id());
        let (vectors, next) = REMOTE_INDEXING_TASKS.read_vectors(&token, 0, 100).unwrap();
        assert_eq!(next, 5);
        assert_eq!(vectors.len(), 5);
        assert_eq!(vectors[0].len(), 8);
        assert!(vectors[1].is_empty(), "deleted points are not served");
        assert_eq!(
            REMOTE_INDEXING_TASKS
                .heartbeat(&token, TaskProgress::default())
                .unwrap(),
            HeartbeatAction::Continue
        );

        // Unknown tokens learn nothing
        let other = TaskToken::random();
        assert!(matches!(
            REMOTE_INDEXING_TASKS.get_task(&other),
            Err(RemoteTaskError::PermissionDenied)
        ));
        assert!(matches!(
            REMOTE_INDEXING_TASKS.heartbeat(&other, TaskProgress::default()),
            Err(RemoteTaskError::PermissionDenied)
        ));

        // Core gives up: heartbeat says CANCEL, everything else is denied, storage released
        drop(handle);
        assert_eq!(Arc::strong_count(&storage), 1);
        assert_eq!(
            REMOTE_INDEXING_TASKS
                .heartbeat(&token, TaskProgress::default())
                .unwrap(),
            HeartbeatAction::Cancel
        );
        assert!(matches!(
            REMOTE_INDEXING_TASKS.read_vectors(&token, 0, 1),
            Err(RemoteTaskError::PermissionDenied)
        ));
        assert!(matches!(
            REMOTE_INDEXING_TASKS.upload(&token, WireGraph::default()),
            Err(RemoteTaskError::PermissionDenied)
        ));
    }

    #[test]
    fn finished_token_is_denied() {
        let (task, _storage) = new_task(3);
        let handle = REMOTE_INDEXING_TASKS.register(task);
        let token = handle.token();
        REMOTE_INDEXING_TASKS
            .fail(&token, "test".to_string())
            .unwrap();
        let outcome = handle.wait(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_millis(10),
            || false,
        );
        assert!(matches!(outcome, Ok(TaskOutcome::Failed(_))));
        assert!(matches!(
            REMOTE_INDEXING_TASKS.heartbeat(&token, TaskProgress::default()),
            Err(RemoteTaskError::PermissionDenied)
        ));
        assert!(matches!(
            REMOTE_INDEXING_TASKS.fail(&token, "again".to_string()),
            Err(RemoteTaskError::PermissionDenied)
        ));
    }

    #[test]
    fn lease_expires_without_calls() {
        let (task, _storage) = new_task(3);
        let handle = REMOTE_INDEXING_TASKS.register(task);
        let poll = Duration::from_millis(10);
        let short = Duration::from_millis(50);
        let long = Duration::from_secs(5);
        // Queued: the queue lease applies
        let outcome = handle.wait(short, long, poll, || false);
        assert!(matches!(outcome, Err(WaitInterrupt::QueueTimeout)));
        let outcome = handle.wait(long, long, poll, || true);
        assert!(matches!(outcome, Err(WaitInterrupt::Stopped)));
        // Fetched: the heartbeat lease applies
        REMOTE_INDEXING_TASKS.get_task(&handle.token()).unwrap();
        let outcome = handle.wait(long, short, poll, || false);
        assert!(matches!(outcome, Err(WaitInterrupt::LeaseExpired)));
    }

    #[test]
    fn token_hex_roundtrip() {
        let token = TaskToken::random();
        let hex = token.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(TaskToken::from_hex(&hex), Some(token));
        assert_eq!(TaskToken::from_hex("zz"), None);
        assert_eq!(TaskToken::from_hex(&"g".repeat(64)), None);
        assert!(!format!("{token:?}").contains(&hex));
    }
}
