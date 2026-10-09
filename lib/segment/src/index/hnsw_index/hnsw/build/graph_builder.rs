use std::sync::atomic::{AtomicBool, AtomicU64};

use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use rayon::ThreadPool;

#[cfg(feature = "gpu")]
use super::gpu_builder::GpuGraphBuilder;
use super::hnsw_builder::HnswGraphBuilder;
use super::pipnn_builder::PipnnGraphBuilder;
use super::remote_builder::RemoteGraphBuilder;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerEnum;
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::visited_pool::VisitedListHandle;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// The segment a graph is built for, and what the build may use.
#[derive(Clone, Copy)]
pub(super) struct GraphBuildContext<'a> {
    pub id_tracker: &'a IdTrackerEnum,
    pub vector_storage: &'a VectorStorageEnum,
    pub quantized_vectors: &'a Option<QuantizedVectors>,
    pub pool: &'a ThreadPool,
    /// Progress of building this vector index. Each builder adds the phases of its work.
    pub progress: &'a ProgressTracker,
    /// GPU locked for this build, if the node indexes on GPU.
    #[cfg_attr(
        not(feature = "gpu"),
        expect(dead_code, reason = "only the GPU builder uses it")
    )]
    pub gpu_device: Option<&'a LockedGpuDevice<'a>>,
    pub stopped: &'a AtomicBool,
}

/// Builds the graphs of a vector index: the main graph and the payload block graphs.
///
/// A builder that fails falls back to [`HnswGraphBuilder`].
pub(super) trait GraphBuilder {
    /// Link every non-deleted point into the main graph.
    ///
    /// `graph_layers_builder` must have the levels of the points set. Returns the builder with
    /// the main graph, which may be another one.
    ///
    /// `old_index` holds the graph of an older segment with most of these points. Only
    /// [`HnswGraphBuilder::try_heal`] can heal it, and other builders call it to decide whether
    /// to build from scratch.
    fn build_main_graph(
        &self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: GraphLayersBuilder,
        old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<GraphLayersBuilder>;

    /// Link `points_to_index`, the points of one payload block, only to each other, and add
    /// these links to `graph_layers_builder`.
    ///
    /// `block_graph` has the parameters of block graphs and no links, and is left so.
    /// `block_filter_list` marks the points of the block.
    fn build_block_graph(
        &mut self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &mut GraphLayersBuilder,
        block_graph: &mut GraphLayersBuilder,
        block_filter_list: &VisitedListHandle,
        points_to_index: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()>;
}

pub(super) enum GraphBuilderEnum {
    Hnsw(HnswGraphBuilder),
    #[cfg(feature = "gpu")]
    Gpu(Box<GpuGraphBuilder>),
    #[expect(dead_code, reason = "collections cannot request PiPNN yet")]
    Pipnn(PipnnGraphBuilder),
    #[expect(
        dead_code,
        reason = "nodes cannot be configured with a remote builder yet"
    )]
    Remote(RemoteGraphBuilder),
}

impl GraphBuilderEnum {
    /// Choose how to build the graphs of a vector index.
    ///
    /// `builds_graph` tells whether there is any graph to build at all, and the main graph keeps
    /// `entry_points_num` entry points.
    #[cfg_attr(
        not(feature = "gpu"),
        expect(
            unused_variables,
            clippy::unnecessary_wraps,
            reason = "without the GPU, there is nothing to choose from yet"
        )
    )]
    pub(super) fn select(
        context: GraphBuildContext<'_>,
        builds_graph: bool,
        entry_points_num: usize,
    ) -> OperationResult<Self> {
        // Small segments are always built on the CPU, nothing else would pay off.
        if !builds_graph
            || context.vector_storage.total_vector_count() < SINGLE_THREADED_HNSW_BUILD_THRESHOLD
        {
            return Ok(Self::Hnsw(HnswGraphBuilder));
        }

        // TODO: `Remote` when the node is configured with a remote builder.
        // TODO: `Pipnn` when the collection requests the PiPNN build method.

        #[cfg(feature = "gpu")]
        if let Some(gpu_device) = context.gpu_device
            && let Some(gpu_builder) = GpuGraphBuilder::new(gpu_device, context, entry_points_num)?
        {
            return Ok(Self::Gpu(Box::new(gpu_builder)));
        }

        Ok(Self::Hnsw(HnswGraphBuilder))
    }
}

impl GraphBuilder for GraphBuilderEnum {
    fn build_main_graph(
        &self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: GraphLayersBuilder,
        old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<GraphLayersBuilder> {
        match self {
            GraphBuilderEnum::Hnsw(builder) => {
                builder.build_main_graph(context, graph_layers_builder, old_index)
            }
            #[cfg(feature = "gpu")]
            GraphBuilderEnum::Gpu(builder) => {
                builder.build_main_graph(context, graph_layers_builder, old_index)
            }
            GraphBuilderEnum::Pipnn(builder) => {
                builder.build_main_graph(context, graph_layers_builder, old_index)
            }
            GraphBuilderEnum::Remote(builder) => {
                builder.build_main_graph(context, graph_layers_builder, old_index)
            }
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
        match self {
            GraphBuilderEnum::Hnsw(builder) => builder.build_block_graph(
                context,
                graph_layers_builder,
                block_graph,
                block_filter_list,
                points_to_index,
                counter,
            ),
            #[cfg(feature = "gpu")]
            GraphBuilderEnum::Gpu(builder) => builder.build_block_graph(
                context,
                graph_layers_builder,
                block_graph,
                block_filter_list,
                points_to_index,
                counter,
            ),
            GraphBuilderEnum::Pipnn(builder) => builder.build_block_graph(
                context,
                graph_layers_builder,
                block_graph,
                block_filter_list,
                points_to_index,
                counter,
            ),
            GraphBuilderEnum::Remote(builder) => builder.build_block_graph(
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
