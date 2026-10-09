use std::sync::atomic::{AtomicBool, AtomicU64};

use common::counter::hw;
use common::reason::reason;
use common::types::PointOffsetType;

use super::graph_builder::{GraphBuildContext, GraphBuilder};
use super::hnsw_builder::HnswGraphBuilder;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::gpu::gpu_graph_builder::{
    GPU_MAX_VISITED_FLAGS_FACTOR, build_hnsw_on_gpu,
};
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::gpu::gpu_vector_storage::GpuVectorStorage;
use crate::index::hnsw_index::gpu::{get_gpu_force_half_precision, get_gpu_groups_count};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::hnsw_index::hnsw::{
    FINISH_MAIN_GRAPH_LOG_MESSAGE, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::visited_pool::VisitedListHandle;
use crate::vector_storage::VectorStorageRead;

/// Inserts points in batches on a GPU, from vectors uploaded once for the whole segment.
///
/// Whatever fails on the GPU is built by [`HnswGraphBuilder`].
pub(super) struct GpuGraphBuilder {
    vectors: GpuVectorStorage,
    /// Number of entry points the main graph keeps.
    entry_points_num: usize,
    /// Insert context for the payload block graphs, created with the first block.
    block_insert_context: Option<GpuInsertContext>,
}

impl GpuGraphBuilder {
    /// Uploads the vectors to `gpu_device`. Returns `None` if that fails, so that the graphs
    /// are built on the CPU.
    pub(super) fn new(
        gpu_device: &LockedGpuDevice,
        context: GraphBuildContext<'_>,
        entry_points_num: usize,
    ) -> OperationResult<Option<Self>> {
        let GraphBuildContext {
            vector_storage,
            quantized_vectors,
            stopped,
            ..
        } = context;

        log::debug!("building HNSW on GPU {}", gpu_device.device().name());

        let vectors = GpuVectorStorage::new(
            gpu_device.device(),
            vector_storage,
            quantized_vectors.as_ref(),
            get_gpu_force_half_precision(),
            stopped,
        );

        // GPU construction does not return an error. If it fails, it will fall back to CPU.
        // To cover stopping case, we need to check stopping flag here.
        check_process_stopped(stopped)?;

        match vectors {
            Ok(vectors) => Ok(Some(Self {
                vectors,
                entry_points_num,
                block_insert_context: None,
            })),
            Err(err) => {
                log::error!("Failed to create GPU vectors, use CPU instead. Error: {err}.");
                Ok(None)
            }
        }
    }

    /// Link every non-deleted point into a copy of `graph_layers_builder`, whose levels must
    /// already be set. Returns `None` if the GPU failed.
    fn build_main_graph_on_gpu(
        &self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &GraphLayersBuilder,
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        let GraphBuildContext {
            id_tracker,
            vector_storage,
            quantized_vectors,
            stopped,
            ..
        } = context;

        let timer = std::time::Instant::now();

        let mut insert_context = GpuInsertContext::new(
            &self.vectors,
            get_gpu_groups_count(),
            graph_layers_builder.hnsw_m(),
            graph_layers_builder.ef_construct(),
            false,
            1..=GPU_MAX_VISITED_FLAGS_FACTOR,
        )?;

        let points_scorer_builder = |vector_id| {
            FilteredScorer::new_internal(
                vector_id,
                vector_storage,
                quantized_vectors.as_ref(),
                None,
                id_tracker.deleted_point_bitslice(),
            )
        };

        let gpu_graph = build_graph_on_gpu(
            &mut insert_context,
            graph_layers_builder,
            id_tracker
                .point_mappings()
                .iter_internal_excluding(vector_storage.deleted_vector_bitslice()),
            self.entry_points_num,
            points_scorer_builder,
            stopped,
        )?;
        if gpu_graph.is_some() {
            log::debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
        }
        Ok(gpu_graph)
    }

    /// Link `points_to_index` only to each other, in a copy of `graph_layers_builder`.
    /// Returns `None` if the GPU failed.
    fn build_block_graph_on_gpu(
        &mut self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &GraphLayersBuilder,
        block_filter_list: &VisitedListHandle,
        points_to_index: &[PointOffsetType],
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        let GraphBuildContext {
            id_tracker,
            vector_storage,
            quantized_vectors,
            stopped,
            ..
        } = context;

        let insert_context = match &mut self.block_insert_context {
            Some(insert_context) => insert_context,
            None => self.block_insert_context.insert(GpuInsertContext::new(
                &self.vectors,
                get_gpu_groups_count(),
                graph_layers_builder.hnsw_m(),
                graph_layers_builder.ef_construct(),
                false,
                1..=GPU_MAX_VISITED_FLAGS_FACTOR,
            )?),
        };

        build_graph_on_gpu(
            insert_context,
            graph_layers_builder,
            points_to_index.iter().copied(),
            1,
            |block_point_id| -> OperationResult<_> {
                let block_condition_checker = OptimizedFilter::from_checker(
                    ConditionCheckerEnum::Build(BuildConditionChecker {
                        filter_list: block_filter_list,
                        current_point: block_point_id,
                    }),
                );
                FilteredScorer::new_internal(
                    block_point_id,
                    vector_storage,
                    quantized_vectors.as_ref(),
                    Some(block_condition_checker),
                    id_tracker.deleted_point_bitslice(),
                )
            },
            stopped,
        )
    }
}

impl GraphBuilder for GpuGraphBuilder {
    fn build_main_graph(
        &self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: GraphLayersBuilder,
        old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<GraphLayersBuilder> {
        match self.build_main_graph_on_gpu(context, &graph_layers_builder)? {
            Some(main_graph) => Ok(main_graph),
            None => HnswGraphBuilder.build_main_graph(context, graph_layers_builder, old_index),
        }
    }

    fn build_block_graph(
        &mut self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &mut GraphLayersBuilder,
        block_graph: &mut GraphLayersBuilder,
        block_filter_list: &VisitedListHandle,
        points_to_index: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        match self.build_block_graph_on_gpu(
            context,
            block_graph,
            block_filter_list,
            points_to_index,
        )? {
            Some(gpu_graph) => {
                graph_layers_builder.merge_from_other(gpu_graph);
                Ok(())
            }
            None => HnswGraphBuilder.build_block_graph(
                context,
                graph_layers_builder,
                block_graph,
                block_filter_list,
                points_to_index,
                counter,
            ),
        }
    }
}

fn build_graph_on_gpu<'a>(
    insert_context: &mut GpuInsertContext,
    graph_layers_builder: &GraphLayersBuilder,
    points_to_index: impl Iterator<Item = PointOffsetType>,
    entry_points_num: usize,
    points_scorer_builder: impl Fn(PointOffsetType) -> OperationResult<FilteredScorer<'a>> + Send + Sync,
    stopped: &AtomicBool,
) -> OperationResult<Option<GraphLayersBuilder>> {
    let gpu_constructed_graph = hw::unmeasured(
        reason("internal operation, the scorers are used within this call"),
        || {
            build_hnsw_on_gpu(
                insert_context,
                graph_layers_builder,
                get_gpu_groups_count(),
                entry_points_num,
                SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
                points_to_index.collect::<Vec<_>>(),
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
