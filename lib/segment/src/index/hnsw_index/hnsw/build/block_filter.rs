use common::types::PointOffsetType;
use log::{debug, trace};
use rand::Rng;

use super::context::GraphBuildContext;
use super::payload_blocks::PayloadBlock;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::vector_storage::VectorStorageRead as _;

/// Which payload blocks the graph needs: those it does not connect well already.
pub(super) struct BlockFilter {
    /// Larger blocks stay connected through the main graph.
    max_block_size: usize,
    /// Probability to keep an edge when sampling connectivity.
    percolation: f32,
    /// Connectivity of the whole graph, which a block must not reach to be built. `None` if the
    /// graph is too small to estimate it, so every block is built.
    required_connectivity: Option<f32>,
}

impl BlockFilter {
    /// Sample the connectivity of `graph` over `points`, the non-deleted points.
    pub fn new<R: Rng + ?Sized>(
        ctx: GraphBuildContext,
        graph: &GraphLayersBuilder,
        points: &[PointOffsetType],
        rng: &mut R,
    ) -> Self {
        // Calculate true average number of links per vertex in the HNSW graph
        // to better estimate percolation threshold
        let average_links_per_0_level = graph.get_average_connectivity_on_level(0);
        let average_links_per_0_level_int = (average_links_per_0_level as usize).max(1);

        // According to percolation theory, random graph becomes disconnected
        // if 1/K points are left, where K is average number of links per point
        // So we need to sample connectivity relative to this bifurcation point, but
        // not exactly at 1/K, as at this point graph is very sensitive to noise.
        //
        // Instead, we choose sampling point at 2/K, which expects graph to still be
        // mostly connected, but still have some measurable disconnected components.

        let percolation = 1. - 2. / (average_links_per_0_level_int as f32);

        let required_connectivity = if average_links_per_0_level_int >= 4 {
            let global_graph_connectivity = [
                graph.subgraph_connectivity(rng, points, percolation),
                graph.subgraph_connectivity(rng, points, percolation),
                graph.subgraph_connectivity(rng, points, percolation),
            ];

            debug!("graph connectivity: {global_graph_connectivity:?} @ {percolation}");

            global_graph_connectivity
                .iter()
                .copied()
                .max_by(|a, b| a.partial_cmp(b).unwrap())
        } else {
            // Main graph is too small to estimate connectivity,
            // we can't shortcut sub-graph building
            None
        };

        // It is expected, that graph will become disconnected less than
        // $1/m$ points left.
        // So blocks larger than $1/m$ are not needed.
        // We add multiplier for the extra safety.
        const PERCOLATION_MULTIPLIER: usize = 4;
        let max_block_size = if ctx.main_params.hnsw_m.m > 0 {
            ctx.vector_storage.total_vector_count() / average_links_per_0_level_int
                * PERCOLATION_MULTIPLIER
        } else {
            usize::MAX
        };

        Self {
            max_block_size,
            percolation,
            required_connectivity,
        }
    }

    /// Whether `block` is worth building, given `graph` built so far. Call right before
    /// building each block: earlier blocks change the answer.
    ///
    /// With `skip_if_connected`, a block whose points the graph already connects well is
    /// skipped.
    pub fn should_build<R: Rng + ?Sized>(
        &self,
        graph: &GraphLayersBuilder,
        block: &PayloadBlock,
        skip_if_connected: bool,
        rng: &mut R,
    ) -> bool {
        if block.cardinality > self.max_block_size {
            return false;
        }

        if skip_if_connected && let Some(required_connectivity) = self.required_connectivity {
            let graph_connectivity =
                graph.subgraph_connectivity(rng, &block.points, self.percolation);
            if graph_connectivity >= required_connectivity {
                trace!(
                    "skip building block, connectivity {graph_connectivity:.4} >= {required_connectivity:.4}"
                );
                return false;
            }
            trace!("graph connectivity: {graph_connectivity}");
        }

        true
    }
}
