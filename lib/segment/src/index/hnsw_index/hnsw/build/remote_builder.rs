use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use atomic_refcell::AtomicRefCell;
use bitvec::vec::BitVec;
use common::budget::ResourcePermit;
use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;

use super::graph_builder::{GraphBuilder, GraphBuilderMethod};
use super::local_builder::LocalBuilder;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::remote::layout::RemoteVectorLayout;
use crate::index::hnsw_index::remote::metrics::REMOTE_INDEXING_METRICS;
use crate::index::hnsw_index::remote::registry::{
    NewTask, REMOTE_INDEXING_TASKS, TaskOutcome, WaitInterrupt,
};
use crate::index::hnsw_index::remote::wire::{
    BlockPlan, RemoteGraphParams, SUPPORTED_GRAPH_FORMAT_VERSIONS,
    VECTOR_REPRESENTATION_STORAGE_NATIVE,
};
use crate::index::hnsw_index::remote::{
    OnFailure, RemoteIndexer, RemoteIndexingConfig, TaskScope, TaskSubmission,
};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead as _};

/// How often the waiting optimizer thread checks `stopped` and the lease.
const WAIT_POLL: Duration = Duration::from_millis(100);

/// What a remote build needs besides the build context. Created by `HNSWIndex::build` when
/// the build is eligible (§4.2).
pub(in crate::index::hnsw_index::hnsw) struct RemoteBuildArgs<'a> {
    pub indexer: Arc<dyn RemoteIndexer>,
    /// Served to the indexer: the registry needs `'static` access, the builder only borrows.
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub layout: RemoteVectorLayout,
    pub scope: TaskScope,
    pub vector_bytes: u64,
    /// Its CPUs are lent back to the budget while the indexer works (§4.6).
    pub permit: &'a ResourcePermit,
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
    /// Waiting for the block plan, then `build_main`.
    Pending,
    /// The remote graph, blocks included, is in `cpu`'s graph.
    Built,
    /// Gave up on the remote build: `cpu` builds everything, blocks included.
    FellBack,
}

/// Has an indexing service build the graph (D1, D2). The service decides how: core sends no
/// levels, only the parameters, the vectors, the deleted points and the block plan (D4).
/// What fails remotely is built by `cpu`, which also holds the graph.
pub(in crate::index::hnsw_index::hnsw) struct RemoteBuilder<'a> {
    /// The local method: builds everything if the remote build fails. Never `Remote`.
    pub cpu: GraphBuilder<'a>,
    args: RemoteBuildArgs<'a>,
    plan: Option<Arc<BlockPlan>>,
    state: RemoteState,
}

impl<'a> RemoteBuilder<'a> {
    pub fn new(cpu: GraphBuilder<'a>, args: RemoteBuildArgs<'a>) -> Self {
        Self {
            cpu,
            args,
            plan: None,
            state: RemoteState::Pending,
        }
    }

    pub fn takes_block_plan(&self) -> bool {
        match self.state {
            RemoteState::Pending | RemoteState::Built => true,
            RemoteState::FellBack => false,
        }
    }

    pub fn set_block_plan(&mut self, plan: BlockPlan) {
        self.plan = Some(Arc::new(plan));
    }

    fn params(args: &RemoteBuildArgs<'_>, local: &LocalBuilder<'_>) -> RemoteGraphParams {
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
            m: local.ctx.main_params.hnsw_m.m,
            m0: local.ctx.main_params.hnsw_m.m0,
            ef_construct: local.ctx.main_params.ef_construct,
            entry_points_num: local.ctx.main_params.entry_points_num,
            payload_m: local.ctx.block_params.hnsw_m.m,
            payload_m0: local.ctx.block_params.hnsw_m.m0,
            point_count: local.ctx.vector_storage.total_vector_count(),
            indexed_point_count: local.points.len(),
            vector_representation: VECTOR_REPRESENTATION_STORAGE_NATIVE.to_string(),
        }
    }

    /// Submit, wait, and retry per `on_failure`. `Ok(Some(graph))` if a remote graph was
    /// accepted, `Ok(None)` to fall back to the local build.
    fn build_remotely(&mut self) -> OperationResult<Option<GraphLayersBuilder>> {
        let local = self.cpu.local();
        let params = Self::params(&self.args, local);
        let stopped = local.ctx.stopped;
        let mut deleted = BitVec::repeat(true, params.point_count);
        for &point in &local.points {
            deleted.set(point as usize, false);
        }
        let deleted = Arc::new(deleted);
        let plan = self.plan.take().unwrap_or_default();

        let config = self.args.indexer.config().clone();
        let attempts = match config.on_failure {
            OnFailure::Local => 1,
            OnFailure::Retry => 1 + config.max_retries,
        };

        self.args.progress.start();
        let counter: Arc<AtomicU64> = self
            .args
            .progress
            .track_progress(Some(params.indexed_point_count as u64));

        for attempt in 0..attempts {
            check_process_stopped(stopped)?;
            if attempt > 0 {
                REMOTE_INDEXING_METRICS.retries.fetch_add(1, Ordering::Relaxed);
            }

            let handle = REMOTE_INDEXING_TASKS.register(NewTask {
                scope: self.args.scope.clone(),
                params: params.clone(),
                block_plan: plan.clone(),
                vector_storage: self.args.vector_storage.clone(),
                deleted: deleted.clone(),
            });
            handle.set_progress_counter(counter.clone());
            let submission = TaskSubmission {
                task_id: handle.task_id(),
                token: handle.token(),
                scope: self.args.scope.clone(),
                vector_bytes: self.args.vector_bytes,
                point_count: params.point_count as u64,
                graph_format_versions: SUPPORTED_GRAPH_FORMAT_VERSIONS.to_vec(),
                attempt,
            };
            log::debug!(
                "Remote indexing task {} for {:?} (attempt {attempt}): {} points, {} bytes, {} blocks",
                submission.task_id,
                submission.scope,
                submission.point_count,
                submission.vector_bytes,
                plan.num_blocks(),
            );

            if let Err(err) = self.args.indexer.submit(&submission) {
                REMOTE_INDEXING_METRICS
                    .submit_failures
                    .fetch_add(1, Ordering::Relaxed);
                log::warn!(
                    "Failed to submit remote indexing task {}: {err}",
                    submission.task_id,
                );
                continue;
            }

            // Lend the CPUs while the indexer works. Once lent, they stay lent across retries.
            self.args.permit.suspend_cpu();

            let outcome = handle.wait(config.heartbeat_timeout, WAIT_POLL, || {
                stopped.load(Ordering::Relaxed)
            });
            match outcome {
                Ok(TaskOutcome::Graph(graph)) => {
                    let task_id = handle.task_id();
                    drop(handle);
                    // Converting and saving the graph takes CPU again
                    if !self.args.permit.resume_cpu(stopped) {
                        return Err(OperationError::cancelled(
                            "optimization cancelled while waiting for budget",
                        ));
                    }
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
                    log::warn!(
                        "Remote indexing task {} failed: {reason}",
                        handle.task_id(),
                    );
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

impl GraphBuilderMethod for RemoteBuilder<'_> {
    fn build_main(&mut self) -> OperationResult<()> {
        let result = self.build_remotely();
        // Make sure the CPUs are back for whatever comes next, also on error
        let resumed = self.args.permit.resume_cpu(self.cpu.local().ctx.stopped);
        let graph = result?;
        if !resumed {
            return Err(OperationError::cancelled(
                "optimization cancelled while waiting for budget",
            ));
        }
        match graph {
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
        debug_assert!(!self.takes_block_plan());
        self.cpu.build_block(points, counter)
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        self.cpu.finalize()
    }

    fn seeds_payload_blocks(&self) -> bool {
        // Remote builds never heal (D12), and neither does the fallback
        self.cpu.seeds_payload_blocks()
    }
}
