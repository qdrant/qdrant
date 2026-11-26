//! Indexer side: run one task against a [`TaskSource`], which is core's `IndexingTask` service
//! over gRPC in the indexing service, or the registry itself in [`super::loopback`].
//!
//! The graph is built by the same builders as a local build, on a storage recreated from the
//! storage-native bytes core serves, so a remote graph has the quality of a local one.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::bitvec::BitVec;
use common::progress_tracker::{ProgressTree, new_progress_tracker};
use common::types::PointOffsetType;
use uuid::Uuid;

use super::blocks::{BlocksBatch, StreamedBlock};
use super::layout::RemoteVectorLayout;
use super::registry::{HeartbeatAction, TaskProgress};
use super::wire::{
    GRAPH_FORMAT_VERSION_V1, RemoteGraphParams, VECTOR_REPRESENTATION_STORAGE_NATIVE, VectorSource,
    WireGraph,
};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorRef};
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{IdTracker as _, IdTrackerEnum};
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::hnsw::build_requested_graph;
use crate::types::ExtendedPointId;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::quantized::quantized_vectors::remote::RemoteQuantizedBuilder;
use crate::vector_storage::{VectorStorage as _, VectorStorageEnum, VectorStorageRead as _};

/// A task as the indexer reads it with `GetTask`.
#[derive(Debug, Clone)]
pub struct FetchedTask {
    pub task_id: Uuid,
    pub params: RemoteGraphParams,
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

    /// Feed the vectors in `start..end` to `sink`, in order: storage-native bytes, or quantized
    /// codes for [`VectorSource::Quantized`]. Empty bytes: the point is not to be indexed.
    fn read_vectors(
        &self,
        start: PointOffsetType,
        end: PointOffsetType,
        sink: &mut dyn FnMut(PointOffsetType, &[u8]) -> SourceResult<()>,
    ) -> SourceResult<()>;

    /// The payload blocks from sequence number `next`, or that none is ready yet, or that
    /// there are no more. May wait a few seconds for core to prepare the next one.
    fn read_payload_blocks(&self, next: u64) -> SourceResult<BlocksBatch>;

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
        // Upload while still heartbeating: core only sees the upload once the whole graph
        // has arrived, so a large graph in transit would otherwise outlast the lease
        let result = result.map(|graph| {
            let timer = Instant::now();
            let uploaded = source.upload(graph);
            report.upload_time = timer.elapsed();
            uploaded
        });
        drop(done_guard);
        result
    });

    report.outcome = match result {
        Ok(uploaded) => match uploaded {
            Ok(()) => RunOutcome::Uploaded,
            Err(SourceError::PermissionDenied) => {
                RunOutcome::Dropped("token revoked before upload".to_string())
            }
            Err(SourceError::Other(err)) => {
                // Tell core at once instead of letting the lease run out; if core already
                // rejected the graph, the token is gone and this fails harmlessly
                let reason = format!("upload: {err}");
                let _ = source.fail(&reason);
                RunOutcome::Failed(reason)
            }
        },
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
    match (task.params.vector_source, &task.params.quantization) {
        (VectorSource::Storage, None) | (VectorSource::Quantized, Some(_)) => {}
        (VectorSource::Storage, Some(_)) => {
            return Err("quantization parameters for the storage vector source".to_string());
        }
        (VectorSource::Quantized, None) => {
            return Err("quantized vector source without quantization parameters".to_string());
        }
    }
    Ok(())
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
    let (storage, quantized, deleted) = match params.vector_source {
        VectorSource::Storage => {
            let (storage, deleted) = read_storage(
                source,
                params,
                &layout,
                storage_dir.path(),
                options,
                stopped,
                &mut bytes_read,
            )?;
            (storage, None, deleted)
        }
        VectorSource::Quantized => {
            let (quantized, deleted) = read_quantized(
                source,
                params,
                &storage_dir.path().join("quantized"),
                options,
                stopped,
                &mut bytes_read,
            )?;
            // The quantized vectors are all the build reads, the storage only carries the
            // point count and the vector parameters. Checked, not assumed: a build that made
            // its queries from original vectors would read them from this empty storage.
            if params.point_count > 0 && !quantized.builds_from_codes(0) {
                return Err(SourceError::Other(
                    "quantized vector source for a quantization that builds from the original \
                     vectors"
                        .to_string(),
                ));
            }
            let storage = layout.placeholder_storage(params.point_count);
            (storage, Some(quantized), deleted)
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
    let mut blocks = BlockReader {
        source,
        params,
        stopped,
        next: 0,
        last_block: Instant::now(),
        error: None,
    };
    let graph = build_requested_graph(
        params,
        &storage,
        &deleted,
        quantized.as_ref(),
        &id_tracker,
        options.num_threads,
        gpu_device,
        &mut rng,
        stopped,
        progress,
        &mut || blocks.next_batch(),
    );
    // A source error, e.g. a revoked token, rather than the build error it caused
    if let Some(err) = blocks.error.take() {
        return Err(err);
    }
    let graph = graph.map_err(other)?;
    report.build_time = timer.elapsed();
    Ok(WireGraph::from_graph_layers_builder(&graph))
}

/// How long to wait before asking again for a payload block that is not ready. Core holds
/// each request a few seconds already, see `BLOCKS_LONG_POLL`.
const BLOCKS_RETRY: Duration = Duration::from_millis(100);

/// Reads the payload blocks core streams, by sequence number, after the main graph.
struct BlockReader<'a> {
    source: &'a dyn TaskSource,
    params: &'a RemoteGraphParams,
    stopped: &'a AtomicBool,
    /// Sequence number of the next block.
    next: u64,
    /// When the last block arrived, or reading started: the wait limit counts from it.
    last_block: Instant,
    /// The source error that ended reading, if any.
    error: Option<SourceError>,
}

impl BlockReader<'_> {
    /// The next batch of blocks, or `None` once core finished them. Fails if none comes
    /// within `max_block_wait`.
    fn next_batch(&mut self) -> OperationResult<Option<Vec<StreamedBlock>>> {
        if !self.params.has_payload_blocks {
            return Ok(None);
        }
        loop {
            check_process_stopped(self.stopped)?;
            let batch = match self.source.read_payload_blocks(self.next) {
                Ok(batch) => batch,
                Err(err) => {
                    let message = err.to_string();
                    self.error = Some(err);
                    return Err(OperationError::service_error(message));
                }
            };
            match batch {
                BlocksBatch::Blocks { first, blocks } => {
                    self.check(first, &blocks)?;
                    self.next += blocks.len() as u64;
                    self.last_block = Instant::now();
                    return Ok(Some(blocks));
                }
                BlocksBatch::NotReady => {
                    let waited = self.last_block.elapsed();
                    if waited > self.params.max_block_wait {
                        let reason = format!(
                            "no payload block from core in {:?} (limit {:?})",
                            waited, self.params.max_block_wait,
                        );
                        self.error = Some(SourceError::Other(reason.clone()));
                        return Err(OperationError::service_error(reason));
                    }
                    std::thread::sleep(BLOCKS_RETRY);
                }
                BlocksBatch::Finished => return Ok(None),
            }
        }
    }

    /// Reject batches out of sequence, and blocks pointing outside the segment.
    fn check(&self, first: u64, blocks: &[StreamedBlock]) -> OperationResult<()> {
        if first != self.next || blocks.is_empty() {
            return Err(OperationError::service_error(format!(
                "core sent {} payload blocks from {first}, expected some from {}",
                blocks.len(),
                self.next,
            )));
        }
        let point_count = self.params.point_count;
        for block in blocks {
            if let Some(&point) = block.points.iter().find(|&&p| p as usize >= point_count) {
                return Err(OperationError::validation_error(format!(
                    "block of field {} has point {point} >= {point_count}",
                    block.field,
                )));
            }
        }
        Ok(())
    }
}

/// Stream the entries of all points, in order, to `sink`, and return which points are not
/// to be indexed: those with an empty entry.
fn read_entries(
    source: &dyn TaskSource,
    params: &RemoteGraphParams,
    options: &RunOptions,
    stopped: &AtomicBool,
    bytes_read: &mut u64,
    mut sink: impl FnMut(PointOffsetType, &[u8]) -> SourceResult<()>,
) -> SourceResult<BitVec> {
    let mut deleted = BitVec::repeat(false, params.point_count);
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
            if bytes.is_empty() {
                deleted.set(offset as usize, true);
            }
            sink(offset, bytes)
        })?;
        if expected != end {
            return Err(SourceError::Other(format!(
                "core sent vectors up to {expected}, expected {end}"
            )));
        }
        start = end;
    }
    Ok(deleted)
}

/// Read the vectors core serves into a new storage of `layout` in `dir`, with the points not
/// to be indexed deleted. Also returns which points those are.
fn read_storage(
    source: &dyn TaskSource,
    params: &RemoteGraphParams,
    layout: &RemoteVectorLayout,
    dir: &Path,
    options: &RunOptions,
    stopped: &AtomicBool,
    bytes_read: &mut u64,
) -> SourceResult<(VectorStorageEnum, BitVec)> {
    let other =
        |err: crate::common::operation_error::OperationError| SourceError::Other(err.to_string());
    let mut storage = layout.create_storage(dir).map_err(other)?;
    // Points not to be indexed keep their offset with a zero vector, and are deleted below.
    // Inserted as a vector rather than bytes: TurboQuant storages encode it.
    let zeros = vec![0.0; params.dim];
    let placeholder = match params.multivector_config {
        Some(_) => VectorRef::from(TypedMultiDenseVectorRef::new(&zeros, params.dim)),
        None => VectorRef::from(&zeros[..]),
    };
    let deleted = read_entries(
        source,
        params,
        options,
        stopped,
        bytes_read,
        |offset, bytes| {
            let inserted = if bytes.is_empty() {
                storage.insert_vector(offset, placeholder)
            } else {
                storage.insert_vector_bytes(offset, bytes)
            };
            inserted.map_err(|err| SourceError::Other(err.to_string()))
        },
    )?;
    for point in deleted.iter_ones() {
        storage
            .delete_vector(point as PointOffsetType)
            .map_err(other)?;
    }
    Ok((storage, deleted))
}

/// Read the quantized codes core serves into quantized vectors in `dir`. Also returns which
/// points are not to be indexed.
fn read_quantized(
    source: &dyn TaskSource,
    params: &RemoteGraphParams,
    dir: &Path,
    options: &RunOptions,
    stopped: &AtomicBool,
    bytes_read: &mut u64,
) -> SourceResult<(QuantizedVectors, BitVec)> {
    let other =
        |err: crate::common::operation_error::OperationError| SourceError::Other(err.to_string());
    let Some(quantization) = &params.quantization else {
        return Err(SourceError::Other(
            "quantized vector source without quantization parameters".to_string(),
        ));
    };
    if params.multivector_config.is_some() {
        return Err(SourceError::Other(
            "quantized vector source for multivectors".to_string(),
        ));
    }
    let mut builder = RemoteQuantizedBuilder::new(
        quantization,
        params.distance,
        params.dim,
        params.point_count,
        dir,
    )
    .map_err(other)?;
    // Points not to be indexed have no code: any code of the right size keeps their offset
    let placeholder = vec![0u8; builder.code_size()];
    let deleted = read_entries(source, params, options, stopped, bytes_read, |_, code| {
        let code = if code.is_empty() {
            &placeholder[..]
        } else {
            code
        };
        builder.push(code).map_err(other)
    })?;
    let quantized = builder
        .finish(params.distance, params.datatype)
        .map_err(other)?;
    Ok((quantized, deleted))
}

/// Process-wide default directory for temporary storages.
pub fn default_work_dir() -> std::path::PathBuf {
    std::env::temp_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw_index::graph_algorithm::MainGraphAlgorithm;
    use crate::index::hnsw_index::remote::wire::{
        HnswBuildParams, RemoteIndexParams, VECTOR_REPRESENTATION_STORAGE_NATIVE, VectorSource,
    };
    use crate::types::{Distance, VectorStorageDatatype};

    /// A core that never gets the next payload block ready.
    struct SlowCore;

    impl TaskSource for SlowCore {
        fn get_task(&self) -> SourceResult<FetchedTask> {
            unimplemented!()
        }

        fn read_vectors(
            &self,
            _start: PointOffsetType,
            _end: PointOffsetType,
            _sink: &mut dyn FnMut(PointOffsetType, &[u8]) -> SourceResult<()>,
        ) -> SourceResult<()> {
            unimplemented!()
        }

        fn read_payload_blocks(&self, _next: u64) -> SourceResult<BlocksBatch> {
            Ok(BlocksBatch::NotReady)
        }

        fn heartbeat(&self, _progress: TaskProgress) -> SourceResult<HeartbeatAction> {
            unimplemented!()
        }

        fn upload(&self, _graph: WireGraph) -> SourceResult<()> {
            unimplemented!()
        }

        fn fail(&self, _reason: &str) -> SourceResult<()> {
            unimplemented!()
        }
    }

    fn params(max_block_wait: Duration) -> RemoteGraphParams {
        RemoteGraphParams {
            vector_name: String::new(),
            dim: 2,
            datatype: VectorStorageDatatype::Float32,
            distance: Distance::Dot,
            multivector_config: None,
            index: RemoteIndexParams::Hnsw(HnswBuildParams {
                m: 4,
                m0: 8,
                ef_construct: 8,
                entry_points_num: 1,
                payload_m: 4,
                payload_m0: 8,
                main_graph: MainGraphAlgorithm::Hnsw,
            }),
            point_count: 10,
            vector_representation: VECTOR_REPRESENTATION_STORAGE_NATIVE.to_string(),
            vector_source: VectorSource::Storage,
            quantization: None,
            has_payload_blocks: true,
            max_block_wait,
        }
    }

    #[test]
    fn waiting_for_payload_blocks_is_bounded() {
        let params = params(Duration::from_millis(300));
        let stopped = AtomicBool::new(false);
        let mut reader = BlockReader {
            source: &SlowCore,
            params: &params,
            stopped: &stopped,
            next: 0,
            last_block: Instant::now(),
            error: None,
        };
        let started = Instant::now();
        assert!(reader.next_batch().is_err());
        assert!(started.elapsed() >= params.max_block_wait);
        assert!(
            matches!(reader.error, Some(SourceError::Other(ref reason)) if reason.contains("no payload block"))
        );
    }

    #[test]
    fn out_of_sequence_blocks_are_rejected() {
        let params = params(Duration::from_secs(1));
        let stopped = AtomicBool::new(false);
        let reader = BlockReader {
            source: &SlowCore,
            params: &params,
            stopped: &stopped,
            next: 3,
            last_block: Instant::now(),
            error: None,
        };
        let block = |points: Vec<PointOffsetType>| StreamedBlock {
            field: "f".to_string(),
            skip_if_connected: false,
            cardinality: points.len(),
            points,
        };
        assert!(reader.check(3, &[block(vec![1, 2])]).is_ok());
        assert!(reader.check(2, &[block(vec![1])]).is_err());
        assert!(reader.check(3, &[]).is_err());
        assert!(
            reader.check(3, &[block(vec![10])]).is_err(),
            "point out of range"
        );
    }
}
