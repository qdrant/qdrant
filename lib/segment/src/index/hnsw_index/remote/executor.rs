//! Indexer side: run one task against a [`TaskSource`], which is core's `IndexingTask` service
//! over gRPC in the indexing service, or the registry itself in [`super::loopback`].
//!
//! The graph is built by the same builders as a local build, on a storage recreated from the
//! storage-native bytes core serves, so a remote graph has the quality of a local one.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bitvec::vec::BitVec;
use common::progress_tracker::{ProgressTree, new_progress_tracker};
use common::types::PointOffsetType;
use uuid::Uuid;

use super::layout::RemoteVectorLayout;
use super::registry::{HeartbeatAction, TaskProgress};
use super::wire::{
    BlockPlan, GRAPH_FORMAT_VERSION_V1, RemoteGraphParams, VECTOR_REPRESENTATION_STORAGE_NATIVE,
    VectorSource, WireGraph,
};
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorRef};
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{IdTracker as _, IdTrackerEnum};
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::hnsw::build_requested_graph;
use crate::types::ExtendedPointId;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage as _, VectorStorageEnum, VectorStorageRead as _};

/// A task as the indexer reads it with `GetTask`.
#[derive(Debug, Clone)]
pub struct FetchedTask {
    pub task_id: Uuid,
    pub params: RemoteGraphParams,
    pub block_plan: BlockPlan,
    /// Graph formats core reads (D9).
    pub graph_format_versions: Vec<u32>,
}

/// Error of a [`TaskSource`] call.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SourceError {
    /// Core does not know the token (anymore): the task is gone, drop it silently (§5).
    #[error("task token rejected by core")]
    PermissionDenied,
    #[error("{0}")]
    Other(String),
}

pub type SourceResult<T> = Result<T, SourceError>;

/// Core's side of one task, as seen by the indexer.
pub trait TaskSource: Sync {
    fn get_task(&self) -> SourceResult<FetchedTask>;

    /// Feed the vectors in `start..end` to `sink`, in order. Empty bytes: no vector.
    fn read_vectors(
        &self,
        start: PointOffsetType,
        end: PointOffsetType,
        sink: &mut dyn FnMut(PointOffsetType, &[u8]) -> SourceResult<()>,
    ) -> SourceResult<()>;

    /// One flag per point, set if it is not to be linked.
    fn read_deleted(&self) -> SourceResult<BitVec>;

    /// Feed the content of the task file `name` to `sink`, in order.
    fn read_file(
        &self,
        name: &str,
        sink: &mut dyn FnMut(&[u8]) -> SourceResult<()>,
    ) -> SourceResult<()>;

    fn heartbeat(&self, progress: TaskProgress) -> SourceResult<HeartbeatAction>;

    fn upload(&self, graph: WireGraph) -> SourceResult<()>;

    fn fail(&self, reason: &str) -> SourceResult<()>;
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub num_threads: usize,
    pub heartbeat_interval: Duration,
    /// Points per `ReadVectors` call.
    pub read_batch: u32,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            num_threads: common::defaults::thread_count_for_hnsw(common::cpu::get_num_cpus()),
            heartbeat_interval: Duration::from_secs(5),
            read_batch: 65536,
        }
    }
}

/// How a task ended on the indexer, with stage timings for metrics and billing (§4.9).
#[derive(Debug, Clone)]
pub struct RunReport {
    pub task_id: Option<Uuid>,
    pub outcome: RunOutcome,
    pub point_count: usize,
    pub vector_bytes_read: u64,
    pub read_time: Duration,
    pub build_time: Duration,
    pub upload_time: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    Uploaded,
    /// Core cancelled the task, or forgot it: nothing to report back.
    Dropped(String),
    /// The task failed; `Fail` was sent to core.
    Failed(String),
    /// The indexer shuts down; `Fail("shutdown")` was sent, core retries or builds locally.
    Shutdown,
}

/// Run one task: fetch, read, build, upload, heartbeating all along. `shutdown` is the
/// process-wide stop flag (SIGTERM). `work_dir` holds the temporary vector storage.
pub fn run_task(
    source: &dyn TaskSource,
    options: &RunOptions,
    work_dir: &Path,
    gpu_device: Option<&LockedGpuDevice<'_>>,
    shutdown: &AtomicBool,
) -> RunReport {
    let mut report = RunReport {
        task_id: None,
        outcome: RunOutcome::Dropped(String::new()),
        point_count: 0,
        vector_bytes_read: 0,
        read_time: Duration::ZERO,
        build_time: Duration::ZERO,
        upload_time: Duration::ZERO,
    };

    // Indexing work is not a request: nothing to measure
    let _scope = common::ambient::unmeasured_guard(common::reason::reason(
        "remote indexing task, not a request",
    ));

    let task = match source.get_task() {
        Ok(task) => task,
        Err(err) => {
            report.outcome = RunOutcome::Dropped(format!("GetTask: {err}"));
            return report;
        }
    };
    report.task_id = Some(task.task_id);
    report.point_count = task.params.point_count;

    if let Err(reason) = check_task(&task) {
        let _ = source.fail(&reason);
        report.outcome = RunOutcome::Failed(reason);
        return report;
    }

    let (progress_view, progress) = new_progress_tracker();
    let cancelled = AtomicBool::new(false);
    let done = AtomicBool::new(false);
    let stopped = AtomicBool::new(false);

    let result = std::thread::scope(|scope| {
        // Stops the heartbeats however the build ends, panics included: a dead build must
        // not keep the lease alive
        struct Done<'a>(&'a AtomicBool);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }
        let done_guard = Done(&done);

        // Heartbeats renew the lease during every stage (D6)
        scope.spawn(|| {
            let mut last = Instant::now();
            while !done.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(50));
                if shutdown.load(Ordering::Relaxed) {
                    stopped.store(true, Ordering::Relaxed);
                }
                if last.elapsed() < options.heartbeat_interval {
                    continue;
                }
                last = Instant::now();
                let tree = progress_view.snapshot("remote_build");
                match source.heartbeat(task_progress(&tree)) {
                    Ok(HeartbeatAction::Continue) => {}
                    Ok(HeartbeatAction::Cancel) | Err(SourceError::PermissionDenied) => {
                        cancelled.store(true, Ordering::Relaxed);
                        stopped.store(true, Ordering::Relaxed);
                    }
                    Err(SourceError::Other(err)) => {
                        // Core may be briefly unreachable: keep going, the lease decides
                        log::warn!("Heartbeat for task {} failed: {err}", task.task_id);
                    }
                }
            }
        });

        let handoff = common::ambient::current();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _scope = handoff.enter_guard();
            build(
                source,
                &task,
                options,
                work_dir,
                gpu_device,
                &stopped,
                &progress,
                &mut report,
            )
        }))
        .unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            Err(SourceError::Other(format!("indexer panicked: {message}")))
        });
        drop(done_guard);
        result
    });

    report.outcome = match result {
        Ok(graph) => {
            let timer = Instant::now();
            let uploaded = source.upload(graph);
            report.upload_time = timer.elapsed();
            match uploaded {
                Ok(()) => RunOutcome::Uploaded,
                Err(SourceError::PermissionDenied) => {
                    RunOutcome::Dropped("token revoked before upload".to_string())
                }
                Err(SourceError::Other(err)) => RunOutcome::Failed(format!("upload: {err}")),
            }
        }
        Err(_) if cancelled.load(Ordering::Relaxed) => {
            RunOutcome::Dropped("cancelled by core".to_string())
        }
        Err(_) if shutdown.load(Ordering::Relaxed) => {
            let _ = source.fail("shutdown");
            RunOutcome::Shutdown
        }
        Err(SourceError::PermissionDenied) => RunOutcome::Dropped("token revoked".to_string()),
        Err(SourceError::Other(reason)) => {
            let _ = source.fail(&reason);
            RunOutcome::Failed(reason)
        }
    };
    report
}

/// Reject tasks this indexer cannot serve (D9).
fn check_task(task: &FetchedTask) -> Result<(), String> {
    if !task
        .graph_format_versions
        .contains(&GRAPH_FORMAT_VERSION_V1)
    {
        return Err(format!(
            "no common graph format: core reads {:?}, indexer writes {GRAPH_FORMAT_VERSION_V1}",
            task.graph_format_versions,
        ));
    }
    if task.params.vector_representation != VECTOR_REPRESENTATION_STORAGE_NATIVE {
        return Err(format!(
            "unsupported vector representation {}",
            task.params.vector_representation,
        ));
    }
    task.block_plan
        .validate(task.params.point_count)
        .map_err(|err| err.to_string())
}

/// The deepest running phase, with its counters.
fn task_progress(tree: &ProgressTree) -> TaskProgress {
    fn deepest<'a>(tree: &'a ProgressTree, path: &mut Vec<&'a str>) -> Option<&'a ProgressTree> {
        for child in &tree.children {
            if child.started_at.is_some() && child.finished_at.is_none() {
                path.push(&child.name);
                return deepest(child, path).or(Some(child));
            }
        }
        None
    }
    let mut path = Vec::new();
    match deepest(tree, &mut path) {
        Some(node) => TaskProgress {
            phase: path.join("/"),
            done: node.done.unwrap_or(0),
            total: node.total.unwrap_or(0),
        },
        None => TaskProgress {
            phase: "reading".to_string(),
            done: 0,
            total: 0,
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn build(
    source: &dyn TaskSource,
    task: &FetchedTask,
    options: &RunOptions,
    work_dir: &Path,
    gpu_device: Option<&LockedGpuDevice<'_>>,
    stopped: &AtomicBool,
    progress: &common::progress_tracker::ProgressTracker,
    report: &mut RunReport,
) -> SourceResult<WireGraph> {
    let params = &task.params;
    let other =
        |err: crate::common::operation_error::OperationError| SourceError::Other(err.to_string());

    let timer = Instant::now();
    let deleted = source.read_deleted()?;
    if deleted.len() != params.point_count {
        return Err(SourceError::Other(format!(
            "deleted mask has {} points, expected {}",
            deleted.len(),
            params.point_count,
        )));
    }

    let layout = RemoteVectorLayout {
        dim: params.dim,
        datatype: params.datatype,
        distance: params.distance,
        multivector_config: params.multivector_config,
    };
    let storage_dir = tempfile::Builder::new()
        .prefix("remote-build-")
        .tempdir_in(work_dir)
        .map_err(|err| SourceError::Other(format!("temp dir: {err}")))?;

    let mut bytes_read = 0u64;
    let (storage, quantized) = match params.vector_source {
        VectorSource::Storage => {
            let storage = read_storage(
                source,
                params,
                &layout,
                &deleted,
                storage_dir.path(),
                options,
                stopped,
                &mut bytes_read,
            )?;
            (storage, None)
        }
        VectorSource::Quantized => {
            let quantized_dir = storage_dir.path().join("quantized");
            for file in &params.files {
                bytes_read +=
                    download_file(source, &quantized_dir, &file.name, file.size, stopped)?;
            }
            // Scalar and binary: the quantized vectors are all the build reads, the storage
            // only carries the point count, the deleted flags and the vector parameters
            let placeholder = layout.placeholder_storage(&deleted);
            let quantized =
                QuantizedVectors::load_copied(&placeholder, &quantized_dir).map_err(other)?;
            let storage = if quantized.builds_without_originals() {
                placeholder
            } else {
                // TurboQuant: queries are encoded from vectors, which are reconstructed from
                // the codes, as a TurboQuant vector storage does. Built here, not sent.
                dequantized_storage(
                    params,
                    &layout,
                    &quantized,
                    &deleted,
                    &storage_dir.path().join("dequantized"),
                    stopped,
                )?
            };
            (storage, Some(quantized))
        }
    };
    if storage.total_vector_count() != params.point_count {
        return Err(SourceError::Other(format!(
            "storage has {} vectors, expected {}",
            storage.total_vector_count(),
            params.point_count,
        )));
    }

    // Every offset maps to a point: deletion is on the vectors
    let mut id_tracker = InMemoryIdTracker::new();
    for point in 0..params.point_count as PointOffsetType {
        id_tracker
            .set_link(ExtendedPointId::NumId(u64::from(point)), point)
            .map_err(other)?;
    }
    let id_tracker = IdTrackerEnum::InMemoryIdTracker(id_tracker);
    report.vector_bytes_read = bytes_read;
    report.read_time = timer.elapsed();

    let timer = Instant::now();
    let mut rng = rand::rng();
    let graph = build_requested_graph(
        params,
        &task.block_plan,
        &storage,
        quantized.as_ref(),
        &id_tracker,
        options.num_threads,
        gpu_device,
        &mut rng,
        stopped,
        progress,
    )
    .map_err(other)?;
    report.build_time = timer.elapsed();
    Ok(WireGraph::from_graph_layers_builder(&graph))
}

/// Read the vectors core serves into a new storage of `layout` in `dir`, and delete the
/// points not to link.
#[allow(clippy::too_many_arguments)]
fn read_storage(
    source: &dyn TaskSource,
    params: &RemoteGraphParams,
    layout: &RemoteVectorLayout,
    deleted: &BitVec,
    dir: &Path,
    options: &RunOptions,
    stopped: &AtomicBool,
    bytes_read: &mut u64,
) -> SourceResult<VectorStorageEnum> {
    let other =
        |err: crate::common::operation_error::OperationError| SourceError::Other(err.to_string());
    let mut storage = layout.create_storage(dir).map_err(other)?;
    // Points without a vector keep their offset with a zero vector, and are deleted below.
    // Inserted as a vector rather than bytes: TurboQuant storages encode it.
    let zeros = vec![0.0; params.dim];
    let placeholder = match params.multivector_config {
        Some(_) => VectorRef::from(TypedMultiDenseVectorRef::new(&zeros, params.dim)),
        None => VectorRef::from(&zeros[..]),
    };

    let mut start = 0;
    while (start as usize) < params.point_count {
        if stopped.load(Ordering::Relaxed) {
            return Err(SourceError::Other("stopped".to_string()));
        }
        let end = (start + options.read_batch).min(params.point_count as PointOffsetType);
        let mut expected = start;
        source.read_vectors(start, end, &mut |offset, bytes| {
            if offset != expected {
                return Err(SourceError::Other(format!(
                    "vector {offset} out of order, expected {expected}"
                )));
            }
            expected += 1;
            *bytes_read += bytes.len() as u64;
            let inserted = if bytes.is_empty() {
                storage.insert_vector(offset, placeholder)
            } else {
                storage.insert_vector_bytes(offset, bytes)
            };
            inserted.map_err(|err| SourceError::Other(err.to_string()))
        })?;
        if expected != end {
            return Err(SourceError::Other(format!(
                "core sent vectors up to {expected}, expected {end}"
            )));
        }
        start = end;
    }
    for point in deleted.iter_ones() {
        storage
            .delete_vector(point as PointOffsetType)
            .map_err(other)?;
    }
    Ok(storage)
}

/// A storage of `layout` in `dir` with the vectors reconstructed from `quantized`, and the
/// points not to link deleted.
fn dequantized_storage(
    params: &RemoteGraphParams,
    layout: &RemoteVectorLayout,
    quantized: &QuantizedVectors,
    deleted: &BitVec,
    dir: &Path,
    stopped: &AtomicBool,
) -> SourceResult<VectorStorageEnum> {
    let other =
        |err: crate::common::operation_error::OperationError| SourceError::Other(err.to_string());
    if params.multivector_config.is_some() {
        return Err(SourceError::Other(
            "cannot reconstruct multivectors from quantized vectors".to_string(),
        ));
    }
    let mut storage = layout.create_storage(dir).map_err(other)?;
    let zeros = vec![0.0; params.dim];
    for point in 0..params.point_count as PointOffsetType {
        if point % 1024 == 0 && stopped.load(Ordering::Relaxed) {
            return Err(SourceError::Other("stopped".to_string()));
        }
        let vector = if deleted[point as usize] {
            None
        } else {
            Some(quantized.dequantize_vector(point).ok_or_else(|| {
                SourceError::Other("quantization cannot reconstruct vectors".to_string())
            })?)
        };
        let vector = vector.as_deref().unwrap_or(&zeros);
        storage
            .insert_vector(point, VectorRef::from(vector))
            .map_err(other)?;
    }
    for point in deleted.iter_ones() {
        storage
            .delete_vector(point as PointOffsetType)
            .map_err(other)?;
    }
    Ok(storage)
}

/// Download one file of the task into `dir`. Returns its size.
fn download_file(
    source: &dyn TaskSource,
    dir: &Path,
    name: &str,
    expected_size: u64,
    stopped: &AtomicBool,
) -> SourceResult<u64> {
    use std::io::Write as _;

    // Names come from core: keep them inside `dir`
    let relative = Path::new(name);
    if name.is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(SourceError::Other(format!("invalid file name {name:?}")));
    }
    let path = dir.join(relative);
    let io = |err: std::io::Error| SourceError::Other(format!("{name}: {err}"));
    if let Some(parent) = path.parent() {
        fs_err::create_dir_all(parent).map_err(io)?;
    }
    let mut file = std::io::BufWriter::new(fs_err::File::create(&path).map_err(io)?);
    let mut size = 0u64;
    source.read_file(name, &mut |chunk| {
        if stopped.load(Ordering::Relaxed) {
            return Err(SourceError::Other("stopped".to_string()));
        }
        size += chunk.len() as u64;
        file.write_all(chunk).map_err(io)
    })?;
    file.flush().map_err(io)?;
    if size != expected_size {
        return Err(SourceError::Other(format!(
            "{name}: got {size} bytes, expected {expected_size}"
        )));
    }
    Ok(size)
}

/// Process-wide default directory for temporary storages.
pub fn default_work_dir() -> std::path::PathBuf {
    std::env::temp_dir()
}
