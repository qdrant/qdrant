use std::sync::atomic::AtomicU64;

use common::types::PointOffsetType;

use super::graph_builder::{GraphBuildContext, GraphBuilder};
use super::hnsw_builder::HnswGraphBuilder;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::visited_pool::VisitedListHandle;

/// Builds the main graph on another machine.
pub(super) struct RemoteGraphBuilder;

impl GraphBuilder for RemoteGraphBuilder {
    fn build_main_graph(
        &self,
        _context: GraphBuildContext<'_>,
        _graph_layers_builder: GraphLayersBuilder,
        _old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<GraphLayersBuilder> {
        todo!("build the main graph on a remote builder")
    }

    /// Payload blocks are built on the node, which keeps the payload.
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
