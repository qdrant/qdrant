//! In-process indexer (§7 step 3): serves tasks from the registry directly, on a thread of its
//! own, with the same [`run_task`] the indexing service runs. For tests and debugging; can
//! inject the failures the protocol must survive.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bitvec::vec::BitVec;
use common::types::PointOffsetType;
use parking_lot::Mutex;

use super::executor::{
    FetchedTask, RunOptions, RunOutcome, RunReport, SourceError, SourceResult, TaskSource, run_task,
};
use super::registry::{
    HeartbeatAction, READ_CHUNK_BYTES, REMOTE_INDEXING_TASKS, RemoteTaskError, TaskProgress,
    TaskToken,
};
use super::wire::{SUPPORTED_GRAPH_FORMAT_VERSIONS, WireGraph};
use super::{RemoteIndexer, RemoteIndexingConfig, SubmitError, TaskSubmission};

/// What the loopback indexer does with a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopbackBehavior {
    /// Build and upload.
    Build,
    /// Refuse the submission, as an unreachable entry point would.
    RefuseSubmit,
    /// Refuse the submission for good, as an indexer does with a too large task.
    RefusePermanently,
    /// Accept the submission and never call core: the lease expires.
    Ignore,
    /// Fetch the task, then report a failure.
    Fail,
    /// Build, then upload a graph with an out-of-range link.
    Corrupt,
    /// Fetch the task and keep heartbeating without finishing, until cancelled.
    Stall,
    /// Leave the task queued for a while, as a busy indexer does, then build it.
    Queue(Duration),
}

/// The source of a task, reading the registry of this process.
pub struct RegistrySource {
    pub token: TaskToken,
}

fn source_err(err: RemoteTaskError) -> SourceError {
    match err {
        RemoteTaskError::PermissionDenied => SourceError::PermissionDenied,
        RemoteTaskError::InvalidArgument(e) | RemoteTaskError::Internal(e) => SourceError::Other(e),
    }
}

impl TaskSource for RegistrySource {
    fn get_task(&self) -> SourceResult<FetchedTask> {
        let description = REMOTE_INDEXING_TASKS
            .get_task(&self.token)
            .map_err(source_err)?;
        Ok(FetchedTask {
            task_id: description.task_id,
            params: description.params,
            block_plan: (*description.block_plan).clone(),
            graph_format_versions: SUPPORTED_GRAPH_FORMAT_VERSIONS.to_vec(),
        })
    }

    fn read_vectors(
        &self,
        start: PointOffsetType,
        end: PointOffsetType,
        sink: &mut dyn FnMut(PointOffsetType, &[u8]) -> SourceResult<()>,
    ) -> SourceResult<()> {
        let mut next = start;
        while next < end {
            let (vectors, after) = REMOTE_INDEXING_TASKS
                .read_vectors(&self.token, next, end)
                .map_err(source_err)?;
            for (i, bytes) in vectors.iter().enumerate() {
                sink(next + i as PointOffsetType, bytes)?;
            }
            if after == next {
                break;
            }
            next = after;
        }
        Ok(())
    }

    fn read_deleted(&self) -> SourceResult<BitVec> {
        REMOTE_INDEXING_TASKS
            .read_deleted(&self.token)
            .map(|deleted| (*deleted).clone())
            .map_err(source_err)
    }

    fn read_file(
        &self,
        name: &str,
        sink: &mut dyn FnMut(&[u8]) -> SourceResult<()>,
    ) -> SourceResult<()> {
        let mut offset = 0u64;
        loop {
            let chunk = REMOTE_INDEXING_TASKS
                .read_file(&self.token, name, offset, READ_CHUNK_BYTES)
                .map_err(source_err)?;
            if chunk.is_empty() {
                return Ok(());
            }
            offset += chunk.len() as u64;
            sink(&chunk)?;
        }
    }

    fn heartbeat(&self, progress: TaskProgress) -> SourceResult<HeartbeatAction> {
        REMOTE_INDEXING_TASKS
            .heartbeat(&self.token, progress)
            .map_err(source_err)
    }

    fn upload(&self, graph: WireGraph) -> SourceResult<()> {
        REMOTE_INDEXING_TASKS
            .upload(&self.token, graph)
            .map_err(source_err)
    }

    fn fail(&self, reason: &str) -> SourceResult<()> {
        REMOTE_INDEXING_TASKS
            .fail(&self.token, reason.to_string())
            .map_err(source_err)
    }
}

/// A source that corrupts the uploaded graph.
struct CorruptingSource(RegistrySource);

impl TaskSource for CorruptingSource {
    fn get_task(&self) -> SourceResult<FetchedTask> {
        self.0.get_task()
    }
    fn read_vectors(
        &self,
        start: PointOffsetType,
        end: PointOffsetType,
        sink: &mut dyn FnMut(PointOffsetType, &[u8]) -> SourceResult<()>,
    ) -> SourceResult<()> {
        self.0.read_vectors(start, end, sink)
    }
    fn read_deleted(&self) -> SourceResult<BitVec> {
        self.0.read_deleted()
    }
    fn read_file(
        &self,
        name: &str,
        sink: &mut dyn FnMut(&[u8]) -> SourceResult<()>,
    ) -> SourceResult<()> {
        self.0.read_file(name, sink)
    }
    fn heartbeat(&self, progress: TaskProgress) -> SourceResult<HeartbeatAction> {
        self.0.heartbeat(progress)
    }
    fn upload(&self, mut graph: WireGraph) -> SourceResult<()> {
        let point_count = graph.levels.len() as PointOffsetType;
        match graph.links.first_mut() {
            Some(link) => *link = point_count + 1,
            None => graph.levels.push(0),
        }
        self.0.upload(graph)
    }
    fn fail(&self, reason: &str) -> SourceResult<()> {
        self.0.fail(reason)
    }
}

/// Runs submitted tasks on threads of this process.
pub struct LoopbackIndexer {
    config: RemoteIndexingConfig,
    /// Behavior per attempt: attempt `i` uses entry `i`, or the last one.
    behaviors: Vec<LoopbackBehavior>,
    options: RunOptions,
    work_dir: PathBuf,
    shutdown: Arc<AtomicBool>,
    /// Number of submissions received.
    pub submissions: AtomicUsize,
    /// Outcomes of finished runs, in order.
    pub outcomes: Mutex<Vec<RunOutcome>>,
    /// Reports of the runs that went through `run_task`, in order.
    pub reports: Mutex<Vec<RunReport>>,
    /// PiPNN's minimum segment size on the build threads, see [`Self::with_pipnn_min_points`].
    #[cfg(feature = "testing")]
    pipnn_min_points: Option<usize>,
}

impl LoopbackIndexer {
    pub fn new(config: RemoteIndexingConfig, behaviors: Vec<LoopbackBehavior>) -> Self {
        Self {
            config,
            behaviors,
            options: RunOptions {
                num_threads: 2,
                heartbeat_interval: Duration::from_millis(100),
                read_batch: 1000,
            },
            work_dir: std::env::temp_dir(),
            shutdown: Arc::new(AtomicBool::new(false)),
            submissions: AtomicUsize::new(0),
            outcomes: Mutex::new(Vec::new()),
            reports: Mutex::new(Vec::new()),
            #[cfg(feature = "testing")]
            pipnn_min_points: None,
        }
    }

    /// Build with PiPNN from `min_points` points, so that small test segments take the
    /// builder real (large) remote builds take on the indexer.
    #[cfg(feature = "testing")]
    pub fn with_pipnn_min_points(mut self, min_points: usize) -> Self {
        self.pipnn_min_points = Some(min_points);
        self
    }

    fn behavior(&self, attempt: usize) -> LoopbackBehavior {
        self.behaviors
            .get(attempt)
            .or(self.behaviors.last())
            .copied()
            .unwrap_or(LoopbackBehavior::Build)
    }

    /// Wait for `n` runs to finish, for tests.
    pub fn wait_outcomes(&self, n: usize, timeout: Duration) -> Vec<RunOutcome> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let outcomes = self.outcomes.lock().clone();
            if outcomes.len() >= n || std::time::Instant::now() > deadline {
                return outcomes;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl RemoteIndexer for Arc<LoopbackIndexer> {
    fn config(&self) -> &RemoteIndexingConfig {
        &self.config
    }

    fn submit(&self, task: &TaskSubmission) -> Result<(), SubmitError> {
        let attempt = self.submissions.fetch_add(1, Ordering::SeqCst);
        let behavior = self.behavior(attempt);
        let token = task.token;
        let this = self.clone();
        match behavior {
            LoopbackBehavior::RefuseSubmit => {
                return Err(SubmitError::Transient("loopback refuses".to_string()));
            }
            LoopbackBehavior::RefusePermanently => {
                return Err(SubmitError::Permanent(
                    "loopback: task too large".to_string(),
                ));
            }
            LoopbackBehavior::Ignore => {
                this.outcomes
                    .lock()
                    .push(RunOutcome::Dropped("ignored".to_string()));
                return Ok(());
            }
            LoopbackBehavior::Build
            | LoopbackBehavior::Fail
            | LoopbackBehavior::Corrupt
            | LoopbackBehavior::Stall
            | LoopbackBehavior::Queue(_) => {}
        }
        std::thread::spawn(move || {
            let run = || this.run(behavior, token);
            #[cfg(feature = "testing")]
            let outcome = match this.pipnn_min_points {
                Some(min_points) => {
                    crate::index::hnsw_index::hnsw::with_pipnn_min_points(min_points, run)
                }
                None => run(),
            };
            #[cfg(not(feature = "testing"))]
            let outcome = run();
            this.outcomes.lock().push(outcome);
        });
        Ok(())
    }
}

impl LoopbackIndexer {
    /// One submitted task, on its own thread.
    fn run(self: &Arc<Self>, behavior: LoopbackBehavior, token: TaskToken) -> RunOutcome {
        let this = self;
        let source = RegistrySource { token };
        {
            match behavior {
                LoopbackBehavior::Build => {
                    let report =
                        run_task(&source, &this.options, &this.work_dir, None, &this.shutdown);
                    this.reports.lock().push(report.clone());
                    report.outcome
                }
                LoopbackBehavior::Queue(delay) => {
                    std::thread::sleep(delay);
                    run_task(&source, &this.options, &this.work_dir, None, &this.shutdown).outcome
                }
                LoopbackBehavior::Corrupt => {
                    let source = CorruptingSource(source);
                    run_task(&source, &this.options, &this.work_dir, None, &this.shutdown).outcome
                }
                LoopbackBehavior::Fail => match source.get_task() {
                    Ok(_) => {
                        let _ = source.fail("injected failure");
                        RunOutcome::Failed("injected failure".to_string())
                    }
                    Err(err) => RunOutcome::Dropped(err.to_string()),
                },
                LoopbackBehavior::Stall => {
                    let _ = source.get_task();
                    loop {
                        std::thread::sleep(this.options.heartbeat_interval);
                        match source.heartbeat(TaskProgress {
                            phase: "stalled".to_string(),
                            done: 0,
                            total: 0,
                        }) {
                            Ok(HeartbeatAction::Continue) => {}
                            Ok(HeartbeatAction::Cancel) => {
                                break RunOutcome::Dropped("cancelled by core".to_string());
                            }
                            Err(err) => break RunOutcome::Dropped(err.to_string()),
                        }
                    }
                }
                LoopbackBehavior::RefuseSubmit
                | LoopbackBehavior::RefusePermanently
                | LoopbackBehavior::Ignore => unreachable!(),
            }
        }
    }
}
