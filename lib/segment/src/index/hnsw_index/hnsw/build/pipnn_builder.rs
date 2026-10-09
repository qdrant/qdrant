use std::sync::atomic::AtomicU64;

use common::types::PointOffsetType;

use super::graph_builder::{GraphBuildContext, GraphBuilder};
use super::hnsw_builder::HnswGraphBuilder;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::visited_pool::VisitedListHandle;

/// Builds each level of the main graph from overlapping clusters with matrix products, without
/// searching the graph (PiPNN, arXiv:2602.21247).
pub(super) struct PipnnGraphBuilder;

impl GraphBuilder for PipnnGraphBuilder {
    fn build_main_graph(
        &self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: GraphLayersBuilder,
        old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<GraphLayersBuilder> {
        // PiPNN can only build from scratch, healing is cheaper when it pays off at all
        if HnswGraphBuilder::try_heal(context, &graph_layers_builder, old_index)? {
            return Ok(graph_layers_builder);
        }
        todo!("build the main graph with PiPNN")
    }

    /// Payload blocks are small and scattered over the segment, HNSW builds them fast.
    // TODO: build large tenant blocks with PiPNN.
    fn build_block_graph(
        &mut self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &mut GraphLayersBuilder,
        block_graph: &mut GraphLayersBuilder,
        block_filter_list: &VisitedListHandle,
        points_to_index: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        HnswGraphBuilder.build_block_graph(
            context,
            graph_layers_builder,
            block_graph,
            block_filter_list,
            points_to_index,
            counter,
        )
    }
}
