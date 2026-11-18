use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use atomic_refcell::AtomicRefCell;
use bitvec::vec::BitVec;
use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;

use super::graph_builder::{GraphBuilder, GraphBuilderMethod};
use super::local_builder::LocalBuilder;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::index::hnsw_index::graph_algorithm::MainGraphAlgorithm;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::remote::blocks::BlockStream;
use crate::index::hnsw_index::remote::layout::RemoteVectorLayout;
use crate::index::hnsw_index::remote::metrics::REMOTE_INDEXING_METRICS;
use crate::index::hnsw_index::remote::registry::{
    NewTask, REMOTE_INDEXING_TASKS, SharedQuantizedVectors, TaskHandle, TaskOutcome, WaitInterrupt,
};
use crate::index::hnsw_index::remote::wire::{
    BlockPlan, HnswBuildParams, RemoteGraphParams, RemoteIndexParams, RemoteQuantization,
    SUPPORTED_GRAPH_FORMAT_VERSIONS, VECTOR_REPRESENTATION_STORAGE_NATIVE, VectorSource,
};
use crate::index::hnsw_index::remote::{
    OnFailure, RemoteIndexer, RemoteIndexingConfig, SubmitError, TaskScope, TaskSubmission,
};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead as _};

/// How often the waiting optimizer thread checks `stopped` and the lease.
const WAIT_POLL: Duration = Duration::from_millis(100);

/// Wait before retry `n` (from 1): 1s, 2s, 4s, ... up to [`MAX_RETRY_BACKOFF`], so retries
/// can outlast a short indexer outage.
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// Sleep for `duration`, waking up to check `stopped`.
fn backoff(duration: Duration, stopped: &std::sync::atomic::AtomicBool) -> OperationResult<()> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        check_process_stopped(stopped)?;
        std::thread::sleep(WAIT_POLL.min(deadline.saturating_duration_since(Instant::now())));
    }
    check_process_stopped(stopped)?;
    Ok(())
}

/// What a remote build needs besides the build context. Created by `HNSWIndex::build` when
/// the build is eligible (§4.2).
pub(in crate::index::hnsw_index::hnsw) struct RemoteBuildArgs {
    pub indexer: Arc<dyn RemoteIndexer>,
    /// Served to the indexer: the registry needs `'static` access, the builder only borrows.
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub layout: RemoteVectorLayout,
    pub scope: TaskScope,
    /// Size of the vector data, for the size threshold and billing.
    pub vector_bytes: u64,
    /// What the indexer reads: the storage's vectors, or the quantized codes.
    pub source: VectorSource,
    /// For [`VectorSource::Quantized`]: how the codes were encoded.
    pub quantization: Option<RemoteQuantization>,
    /// For [`VectorSource::Quantized`]: served to the indexer, like `vector_storage`.
    pub quantized_vectors: Option<SharedQuantizedVectors>,
    /// Shows the indexer's progress in the optimizations view.
    pub progress: ProgressTracker,
}

/// Whether a build of `vector_bytes` goes remote with `config` (D11).
pub(in crate::index::hnsw_index::hnsw) fn worth_remote(
    config: &RemoteIndexingConfig,
    vector_bytes: u64,
) -> bool {
    vector_bytes >= config.min_index_size_bytes as u64
}

enum RemoteState {
    /// Before `build_main`.
    Pending,
    /// The remote graph, blocks included, is in `cpu`'s graph.
    Built,
    /// Gave up on the remote build: `cpu` builds everything, blocks included.
    FellBack,
}

/// What every attempt of a remote build shares, prepared by [`RemoteBuilder::start`].
struct Prepared {
    params: RemoteGraphParams,
    deleted: Arc<BitVec>,
    /// Points linked by the indexer, from its heartbeats.
    counter: Arc<AtomicU64>,
    /// The first attempt, submitted before the payload blocks are prepared. Taken by
    /// `build_remotely`.
    first: Option<Result<TaskHandle, SubmitError>>,
}

/// Has an indexing service build the graph (D1, D2). The service decides how: core sends no
/// levels, only the parameters and the vectors. The payload blocks are streamed while the
/// indexer builds the main graph. What fails remotely is built by `cpu`, which also holds the
/// graph.
pub(in crate::index::hnsw_index::hnsw) struct RemoteBuilder<'a> {
    /// The local method: builds everything if the remote build fails. Never `Remote`.
    pub cpu: GraphBuilder<'a>,
    args: RemoteBuildArgs,
    has_payload_blocks: bool,
    /// What the local build would build the main graph with: sent to the indexer.
    main_graph: MainGraphAlgorithm,
    /// Shared by all attempts, and by the local fallback.
    blocks: Arc<BlockStream>,
    prepared: Option<Prepared>,
    state: RemoteState,
}

impl<'a> RemoteBuilder<'a> {
    pub fn new(
        cpu: GraphBuilder<'a>,
        args: RemoteBuildArgs,
        has_payload_blocks: bool,
        main_graph: MainGraphAlgorithm,
    ) -> Self {
        Self {
            cpu,
            args,
            has_payload_blocks,
            main_graph,
            blocks: Arc::new(BlockStream::default()),
            prepared: None,
            state: RemoteState::Pending,
        }
    }

    /// Submit the first attempt, so that the indexer starts on the main graph, and return the
    /// stream for the payload blocks.
    pub fn start(&mut self) -> Arc<BlockStream> {
        self.prepare();
        self.blocks.clone()
    }

    /// After `build_main`: whether the indexer linked the payload blocks.
    pub fn blocks_linked(&self) -> bool {
        match self.state {
            RemoteState::Built => true,
            RemoteState::Pending | RemoteState::FellBack => false,
        }
    }

    /// After `build_main`, if the remote build was given up: the blocks prepared for it.
    pub fn fallback_blocks(&self) -> Option<BlockPlan> {
        match self.state {
            RemoteState::FellBack => self.blocks.finished_plan(),
            RemoteState::Pending | RemoteState::Built => None,
        }
    }

    fn prepare(&mut self) {
        if self.prepared.is_some() {
            return;
        }
        let local = self.cpu.local();
        let params = Self::params(&self.args, local, self.has_payload_blocks, self.main_graph);
        let mut deleted = BitVec::repeat(true, params.point_count);
        for &point in &local.points {
            deleted.set(point as usize, false);
        }
        let points_to_index = local.points.len();
        self.args.progress.start();
        let counter = self
            .args
            .progress
            .track_progress(Some(points_to_index as u64));
        let mut prepared = Prepared {
            params,
            deleted: Arc::new(deleted),
            counter,
            first: None,
        };
        prepared.first = Some(submit_attempt(&self.args, &self.blocks, &prepared, 0));
        self.prepared = Some(prepared);
    }

    fn params(
        args: &RemoteBuildArgs,
        local: &LocalBuilder<'_>,
        has_payload_blocks: bool,
        main_graph: MainGraphAlgorithm,
    ) -> RemoteGraphParams {
        let RemoteVectorLayout {
            dim,
            datatype,
            distance,
            multivector_config,
        } = args.layout;
        RemoteGraphParams {
            vector_name: args.scope.vector_name.clone(),
            dim,
            datatype,
            distance,
            multivector_config,
            index: RemoteIndexParams::Hnsw(HnswBuildParams {
                m: local.ctx.main_params.hnsw_m.m,
                m0: local.ctx.main_params.hnsw_m.m0,
                ef_construct: local.ctx.main_params.ef_construct,
                entry_points_num: local.ctx.main_params.entry_points_num,
                payload_m: local.ctx.block_params.hnsw_m.m,
                payload_m0: local.ctx.block_params.hnsw_m.m0,
                main_graph,
            }),
            point_count: local.ctx.vector_storage.total_vector_count(),
            vector_representation: VECTOR_REPRESENTATION_STORAGE_NATIVE.to_string(),
            vector_source: args.source,
            quantization: args.quantization.clone(),
            has_payload_blocks,
            max_block_wait: args.indexer.config().max_block_wait,
        }
    }

    /// Submit, wait, and retry per `on_failure`. `Ok(Some(graph))` if a remote graph was
    /// accepted, `Ok(None)` to fall back to the local build.
    fn build_remotely(&mut self) -> OperationResult<Option<GraphLayersBuilder>> {
        // Blocks not streamed by the caller: there are none
        self.blocks.finish();
        self.prepare();
        let stopped = self.cpu.local().ctx.stopped;
        let Some(prepared) = self.prepared.as_mut() else {
            unreachable!("prepared above");
        };

        let config = self.args.indexer.config().clone();
        let attempts = match config.on_failure {
            OnFailure::Local => 1,
            OnFailure::Retry => 1 + config.max_retries,
        };

        for attempt in 0..attempts {
            check_process_stopped(stopped)?;
            let submitted = match prepared.first.take() {
                Some(first) => first,
                None => {
                    REMOTE_INDEXING_METRICS
                        .retries
                        .fetch_add(1, Ordering::Relaxed);
                    let delay = RETRY_BACKOFF
                        .saturating_mul(1 << attempt.saturating_sub(1).min(16))
                        .min(MAX_RETRY_BACKOFF);
                    backoff(delay, stopped)?;
                    submit_attempt(&self.args, &self.blocks, prepared, attempt)
                }
            };
            let handle = match submitted {
                Ok(handle) => handle,
                Err(SubmitError::Transient(_)) => continue,
                // Retrying cannot help: build locally right away
                Err(SubmitError::Permanent(_)) => break,
            };

            let outcome = handle.wait(
                config.queue_timeout,
                config.heartbeat_timeout,
                WAIT_POLL,
                || stopped.load(Ordering::Relaxed),
            );
            match outcome {
                Ok(TaskOutcome::Graph(graph)) => {
                    let task_id = handle.task_id();
                    drop(handle);
                    let timer = Instant::now();
                    let local = self.cpu.local();
                    let builder = graph.into_graph_layers_builder(
                        local.ctx.main_params.hnsw_m,
                        local.ctx.main_params.ef_construct,
                        local.ctx.main_params.entry_points_num,
                    );
                    REMOTE_INDEXING_METRICS
                        .accept_time_ms
                        .fetch_add(timer.elapsed().as_millis() as u64, Ordering::Relaxed);
                    REMOTE_INDEXING_METRICS
                        .tasks_succeeded
                        .fetch_add(1, Ordering::Relaxed);
                    log::debug!("Remote indexing task {task_id} done");
                    return Ok(Some(builder));
                }
                Ok(TaskOutcome::Failed(reason)) => {
                    REMOTE_INDEXING_METRICS
                        .tasks_failed
                        .fetch_add(1, Ordering::Relaxed);
                    log::warn!("Remote indexing task {} failed: {reason}", handle.task_id(),);
                }
                Ok(TaskOutcome::Rejected(reason)) => {
                    // Counted by the registry
                    log::warn!(
                        "Remote indexing task {} graph rejected: {reason}",
                        handle.task_id(),
                    );
                }
                Err(WaitInterrupt::Stopped) => {
                    REMOTE_INDEXING_METRICS
                        .cancelled
                        .fetch_add(1, Ordering::Relaxed);
                    log::debug!(
                        "Remote indexing task {} cancelled: optimization stopped",
                        handle.task_id(),
                    );
                    // Dropping the handle revokes the token: the next heartbeat gets CANCEL
                    drop(handle);
                    return Err(OperationError::cancelled(
                        "optimization cancelled while waiting for remote indexing",
                    ));
                }
                Err(WaitInterrupt::QueueTimeout) => {
                    REMOTE_INDEXING_METRICS
                        .queue_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    log::warn!(
                        "Remote indexing task {}: not picked up in {:?}",
                        handle.task_id(),
                        config.queue_timeout,
                    );
                }
                Err(WaitInterrupt::LeaseExpired) => {
                    REMOTE_INDEXING_METRICS
                        .heartbeats_missed
                        .fetch_add(1, Ordering::Relaxed);
                    log::warn!(
                        "Remote indexing task {}: no heartbeat in {:?}, progress {:?}",
                        handle.task_id(),
                        config.heartbeat_timeout,
                        handle.progress(),
                    );
                }
            }
            // `handle` is dropped here, revoking the token before the next attempt
        }
        Ok(None)
    }
}

/// Register attempt `attempt` of a task and submit it. On failure the handle is dropped,
/// which revokes the token.
fn submit_attempt(
    args: &RemoteBuildArgs,
    blocks: &Arc<BlockStream>,
    prepared: &Prepared,
    attempt: u32,
) -> Result<TaskHandle, SubmitError> {
    let handle = REMOTE_INDEXING_TASKS.register(NewTask {
        scope: args.scope.clone(),
        params: prepared.params.clone(),
        blocks: blocks.clone(),
        vector_storage: args.vector_storage.clone(),
        deleted: prepared.deleted.clone(),
        quantized_vectors: args.quantized_vectors.clone(),
    });
    handle.set_progress_counter(prepared.counter.clone());
    let submission = TaskSubmission {
        task_id: handle.task_id(),
        token: handle.token(),
        scope: args.scope.clone(),
        vector_bytes: args.vector_bytes,
        point_count: prepared.params.point_count as u64,
        index: prepared.params.index,
        graph_format_versions: SUPPORTED_GRAPH_FORMAT_VERSIONS.to_vec(),
        attempt,
    };
    log::debug!(
        "Remote indexing task {} for {:?} (attempt {attempt}): {} points, {} bytes",
        submission.task_id,
        submission.scope,
        submission.point_count,
        submission.vector_bytes,
    );
    match args.indexer.submit(&submission) {
        Ok(()) => Ok(handle),
        Err(SubmitError::Transient(reason)) => {
            REMOTE_INDEXING_METRICS
                .submit_failures
                .fetch_add(1, Ordering::Relaxed);
            log::warn!(
                "Failed to submit remote indexing task {}: {reason}",
                submission.task_id,
            );
            Err(SubmitError::Transient(reason))
        }
        Err(SubmitError::Permanent(reason)) => {
            REMOTE_INDEXING_METRICS
                .submit_failures
                .fetch_add(1, Ordering::Relaxed);
            REMOTE_INDEXING_METRICS
                .submit_refused_permanently
                .fetch_add(1, Ordering::Relaxed);
            log::info!(
                "Remote indexing task {} refused for good, building locally: {reason}",
                submission.task_id,
            );
            Err(SubmitError::Permanent(reason))
        }
    }
}

impl GraphBuilderMethod for RemoteBuilder<'_> {
    fn build_main(&mut self) -> OperationResult<()> {
        match self.build_remotely()? {
            Some(graph) => {
                let local = self.cpu.local();
                local.graph = graph;
                // The local phases only time CPU work, so they end unstarted
                drop(local.ctx.progress.take());
                self.state = RemoteState::Built;
                Ok(())
            }
            None => {
                REMOTE_INDEXING_METRICS
                    .fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                log::warn!(
                    "Remote indexing of {:?} gave up, building locally",
                    self.args.scope,
                );
                self.state = RemoteState::FellBack;
                self.cpu.build_main()
            }
        }
    }

    fn build_block(
        &mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        // Called only after a fallback: remote blocks are linked by the indexer
        debug_assert!(!self.blocks_linked());
        self.cpu.build_block(points, counter)
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        self.cpu.finalize()
    }

    fn seeds_payload_blocks(&self) -> bool {
        // Remote builds never heal (D12), and neither does the fallback, also when an old graph
        // was passed over for covering too few points
        self.cpu.seeds_payload_blocks()
    }
}
