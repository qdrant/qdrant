use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use common::ambient;
use common::reason::reason;
use common::types::PointOffsetType;

use super::context::{GraphBuildContext, GraphParams};
use super::graph_builder::{Fallback, GraphBuilderMethod, StepError};
use super::local_builder::LocalBuilder;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::gpu::gpu_graph_builder::{
    GPU_MAX_VISITED_FLAGS_FACTOR, build_hnsw_on_gpu,
};
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::gpu::gpu_vector_storage::GpuVectorStorage;
use crate::index::hnsw_index::gpu::{get_gpu_force_half_precision, get_gpu_groups_count};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::old_index::{OldIndex, payload_links_reusable};
use crate::index::hnsw_index::hnsw::{
    FINISH_MAIN_GRAPH_LOG_MESSAGE, HNSW_USE_HEURISTIC, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::vector_storage::VectorStorageRead as _;

/// Inserts points in batches on a GPU. A step the GPU fails to build returns the state as a
/// [`Fallback`], and is built on the CPU by the method `select` picks without a GPU: the old
/// graph is healed if there is one worth healing.
pub(super) struct GpuBuilder<'a> {
    pub local: LocalBuilder<'a>,
    pub gpu: GpuState,
    /// The old graph worth healing, if any: it picks the method that builds what the GPU fails
    /// to build.
    pub old_index: Option<&'a OldIndex<'a>>,
}

/// GPU vectors and block insert context, kept across blocks.
pub(super) struct GpuState {
    /// Shared with the insert contexts, so the vector buffers outlive every one of them.
    vectors: Arc<GpuVectorStorage>,
    /// Created with the first block and reused for the rest: all blocks have the same
    /// parameters.
    blocks: Option<GpuBlocks>,
}

struct GpuBlocks {
    /// The GPU stores vectors and links by segment id, so blocks are built in a segment-sized
    /// builder. This one only carries the block graph parameters: `payload_m`, `ef_construct`
    /// and level 0 for every point.
    reference: GraphLayersBuilder,
    insert_context: GpuInsertContext,
}

impl GpuState {
    /// Uploads the vectors to `device`. `None` if that fails, so the graphs are built on the CPU.
    pub fn new(
        ctx: &GraphBuildContext<'_>,
        device: &LockedGpuDevice,
    ) -> OperationResult<Option<Self>> {
        let vectors = GpuVectorStorage::new(
            device.device(),
            ctx.vector_storage,
            ctx.quantized_vectors,
            get_gpu_force_half_precision(),
            ctx.stopped,
        );

        // GPU construction does not return an error. If it fails, it will fall back to CPU.
        // To cover stopping case, we need to check stopping flag here.
        check_process_stopped(ctx.stopped)?;

        match vectors {
            Ok(vectors) => {
                log::debug!("building HNSW on GPU {}", device.device().name());
                Ok(Some(Self {
                    vectors: Arc::new(vectors),
                    blocks: None,
                }))
            }
            Err(err) => {
                log::error!("Failed to create GPU vectors, use CPU instead. Error: {err}.");
                Ok(None)
            }
        }
    }

    /// The main graph of `points`, with the parameters and levels of `graph`. `None` if the
    /// GPU failed.
    pub fn build_main(
        &self,
        ctx: &GraphBuildContext<'_>,
        graph: &GraphLayersBuilder,
        points: &[PointOffsetType],
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        let timer = std::time::Instant::now();

        let mut insert_context = GpuInsertContext::new(
            self.vectors.clone(),
            get_gpu_groups_count(),
            graph.hnsw_m(),
            graph.ef_construct(),
            false,
            1..=GPU_MAX_VISITED_FLAGS_FACTOR,
        )?;

        let gpu_graph = build_graph_on_gpu(
            &mut insert_context,
            graph,
            points.to_vec(),
            ctx.main_params.entry_points_num,
            |point_id| scorer(ctx, point_id),
            ctx.stopped,
        )?;
        if gpu_graph.is_some() {
            log::debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
        }
        Ok(gpu_graph)
    }

    /// Link `points` only to each other, in a new segment-sized builder. `None` if the GPU
    /// failed.
    pub fn build_block(
        &mut self,
        ctx: &GraphBuildContext<'_>,
        points: &[PointOffsetType],
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        let GraphParams {
            hnsw_m,
            ef_construct,
            entry_points_num,
        } = ctx.block_params;

        let blocks = match &mut self.blocks {
            Some(blocks) => blocks,
            None => self.blocks.insert(GpuBlocks {
                reference: GraphLayersBuilder::new_with_params(
                    ctx.vector_storage.total_vector_count(),
                    hnsw_m,
                    ef_construct,
                    entry_points_num,
                    HNSW_USE_HEURISTIC,
                    false,
                ),
                insert_context: GpuInsertContext::new(
                    self.vectors.clone(),
                    get_gpu_groups_count(),
                    hnsw_m,
                    ef_construct,
                    false,
                    1..=GPU_MAX_VISITED_FLAGS_FACTOR,
                )?,
            }),
        };

        // Searches start from block points and follow links between block points, so they cannot
        // reach other points and need no block filter.
        build_graph_on_gpu(
            &mut blocks.insert_context,
            &blocks.reference,
            points.to_vec(),
            entry_points_num,
            |point_id| scorer(ctx, point_id),
            ctx.stopped,
        )
    }
}

impl<'a> GpuBuilder<'a> {
    /// The GPU failed and nothing was linked: hand the state to the method `select` picks
    /// without a GPU.
    fn fall_back(self) -> StepError<'a> {
        let GpuBuilder {
            local,
            gpu,
            old_index,
        } = self;
        StepError::Fallback(Box::new(Fallback {
            local,
            gpu,
            old_index,
        }))
    }
}

impl<'a> GraphBuilderMethod<'a> for GpuBuilder<'a> {
    fn build_main(mut self) -> Result<Self, StepError<'a>> {
        let GpuBuilder {
            local,
            gpu,
            old_index: _,
        } = &mut self;
        if !local.ctx.has_main_graph() {
            // No main graph
            return Ok(self);
        }
        match gpu.build_main(&local.ctx, &local.graph, &local.points)? {
            Some(graph) => {
                local.graph = graph;
                // The phases only time CPU work, so they end unstarted
                drop(local.ctx.progress.take());
                Ok(self)
            }
            // The phases are untouched, so the CPU method runs them in order, as without a GPU
            None => Err(self.fall_back()),
        }
    }

    /// Block points linked on the GPU are not counted.
    fn build_block(
        mut self,
        points: &[PointOffsetType],
        _counter: &AtomicU64,
    ) -> Result<Self, StepError<'a>> {
        let GpuBuilder {
            local,
            gpu,
            old_index: _,
        } = &mut self;
        match gpu.build_block(&local.ctx, points)? {
            Some(block_graph) => {
                local.graph.merge_from_other(block_graph);
                Ok(self)
            }
            None => Err(self.fall_back()),
        }
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        let GpuBuilder {
            local,
            gpu,
            old_index: _,
        } = self;
        // Release the GPU memory before the graph is saved
        drop(gpu);
        Ok(local.finalize())
    }

    fn seeds_payload_blocks(&self) -> bool {
        // The GPU does not, but the blocks it fails to build are built by the method `select`
        // picks without a GPU: healing, which seeds them if it can
        self.old_index.is_some_and(|old_index| {
            payload_links_reusable(self.local.ctx.block_params.hnsw_m, &old_index.index.config)
        })
    }
}

/// Scorer against `point_id` over the whole segment, for the CPU parts of a GPU build.
fn scorer<'a>(
    ctx: &GraphBuildContext<'a>,
    point_id: PointOffsetType,
) -> OperationResult<FilteredScorer<'a>> {
    FilteredScorer::new_internal(
        point_id,
        ctx.vector_storage,
        ctx.quantized_vectors,
        None,
        ctx.id_tracker.deleted_point_bitslice(),
    )
}

fn build_graph_on_gpu<'a>(
    insert_context: &mut GpuInsertContext,
    graph_layers_builder: &GraphLayersBuilder,
    points_to_index: Vec<PointOffsetType>,
    entry_points_num: usize,
    points_scorer_builder: impl Fn(PointOffsetType) -> OperationResult<FilteredScorer<'a>> + Send + Sync,
    stopped: &AtomicBool,
) -> OperationResult<Option<GraphLayersBuilder>> {
    let gpu_constructed_graph = ambient::unmeasured(
        reason("internal operation, the scorers are used within this call"),
        || {
            build_hnsw_on_gpu(
                insert_context,
                graph_layers_builder,
                get_gpu_groups_count(),
                entry_points_num,
                SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
                points_to_index,
                points_scorer_builder,
                stopped,
            )
        },
    );

    // GPU construction does not return an error. If it fails, it will fall back to CPU.
    // To cover stopping case, we need to check stopping flag here.
    check_process_stopped(stopped)?;

    match gpu_constructed_graph {
        Ok(gpu_constructed_graph) => Ok(Some(gpu_constructed_graph)),
        Err(gpu_error) => {
            log::warn!("Failed to build HNSW on GPU: {gpu_error}. Falling back to CPU.");
            Ok(None)
        }
    }
}
