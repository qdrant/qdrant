use std::sync::atomic::{AtomicBool, AtomicU64};

use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use rayon::ThreadPool;

use super::hnsw_builder::HnswGraphBuilder;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerEnum;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// The segment a graph is built for, and what the build may use.
#[derive(Clone, Copy)]
pub(super) struct GraphBuildContext<'a> {
    pub id_tracker: &'a IdTrackerEnum,
    pub vector_storage: &'a VectorStorageEnum,
    pub quantized_vectors: &'a Option<QuantizedVectors>,
    pub pool: &'a ThreadPool,
    /// Progress of building this vector index. Builders add the phases of their work to it.
    pub progress: &'a ProgressTracker,
    pub stopped: &'a AtomicBool,
}

/// Builds the graphs of a vector index: the main graph and the payload block graphs.
///
/// Errors, cancellation included, are returned to the caller. A builder that cannot build a
/// graph on its own, for example when its device fails, hands the work over to
/// [`HnswGraphBuilder`] instead.
pub(super) trait GraphBuilder {
    /// Link every non-deleted point into the main graph.
    ///
    /// `graph_layers_builder` must have the levels of the points set. Returns the builder with
    /// the main graph, which may be another one.
    ///
    /// `old_index` is the graph of an older segment that `OldIndexCandidate::evaluate` found
    /// worth healing: built with the same parameters, and with few of its points gone since.
    /// Only [`HnswGraphBuilder::try_heal`] can heal it.
    fn build_main_graph(
        &self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: GraphLayersBuilder,
        old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<GraphLayersBuilder>;

    /// Link `points_to_index`, the points of one payload block, only to each other, and add
    /// these links to `graph_layers_builder`.
    ///
    /// Block graphs are built with `payload_m` and `ef_construct`, which can differ from the
    /// parameters of `graph_layers_builder`. `counter` counts the points linked, for progress.
    fn build_block_graph(
        &mut self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &mut GraphLayersBuilder,
        points_to_index: &[PointOffsetType],
        payload_m: HnswM,
        ef_construct: usize,
        counter: &AtomicU64,
    ) -> OperationResult<()>;
}

pub(super) enum GraphBuilderEnum {
    /// Insert points one by one on the CPU.
    Hnsw(HnswGraphBuilder),
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
        }
    }

    fn build_block_graph(
        &mut self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &mut GraphLayersBuilder,
        points_to_index: &[PointOffsetType],
        payload_m: HnswM,
        ef_construct: usize,
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        match self {
            GraphBuilderEnum::Hnsw(builder) => builder.build_block_graph(
                context,
                graph_layers_builder,
                points_to_index,
                payload_m,
                ef_construct,
                counter,
            ),
        }
    }
}
