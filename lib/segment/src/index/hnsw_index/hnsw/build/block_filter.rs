use common::types::PointOffsetType;
use log::{debug, trace};
use rand::Rng;

use super::context::GraphBuildContext;
use super::payload_blocks::PayloadBlock;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::vector_storage::VectorStorageRead as _;

/// Which payload blocks of a field to skip, besides those too large to build.
#[derive(Clone, Copy, Debug)]
pub(super) enum BlockSkip {
    /// Blocks whose points the graph built so far already connects well.
    IfConnected,
    /// None: the blocks of the first field, and of tenant fields.
    Never,
}

impl BlockSkip {
    /// The skip rule for the field at `index_pos` in build order: the first field and tenant
    /// fields always get their blocks.
    pub fn for_field(index_pos: usize, is_tenant: bool) -> Self {
        if index_pos > 0 && !is_tenant {
            BlockSkip::IfConnected
        } else {
            BlockSkip::Never
        }
    }

    /// From its form in a remote indexing task.
    #[cfg_attr(not(feature = "remote-indexing"), expect(dead_code))]
    pub fn from_skip_if_connected(skip_if_connected: bool) -> Self {
        if skip_if_connected {
            BlockSkip::IfConnected
        } else {
            BlockSkip::Never
        }
    }

    /// Its form in a remote indexing task.
    #[cfg_attr(not(feature = "remote-indexing"), expect(dead_code))]
    pub fn skip_if_connected(self) -> bool {
        match self {
            BlockSkip::IfConnected => true,
            BlockSkip::Never => false,
        }
    }
}

/// Blocks larger than this many times the point count at which the graph percolates (see
/// [`max_block_size`]) stay connected through the main graph. Extra safety margin.
const PERCOLATION_MULTIPLIER: usize = 4;

/// The largest payload block worth building, in a graph of `total_vector_count` points with
/// `average_links` links per point on level 0.
///
/// A random graph becomes disconnected when fewer than $1/K$ of its points are left, with $K$
/// its average number of links per point. Blocks larger than that stay connected through the
/// main graph. Without a main graph, every block is built.
pub(super) fn max_block_size(
    total_vector_count: usize,
    average_links: usize,
    has_main_graph: bool,
) -> usize {
    if has_main_graph {
        total_vector_count / average_links.max(1) * PERCOLATION_MULTIPLIER
    } else {
        usize::MAX
    }
}

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
        ctx: &GraphBuildContext<'_>,
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

        let max_block_size = max_block_size(
            ctx.vector_storage.total_vector_count(),
            average_links_per_0_level_int,
            ctx.has_main_graph(),
        );

        Self {
            max_block_size,
            percolation,
            required_connectivity,
        }
    }

    /// Whether a block of estimated `cardinality` is small enough to build. Checked before its
    /// points are read, so that blocks too large to build are not read at all.
    pub fn fits(&self, cardinality: usize) -> bool {
        cardinality <= self.max_block_size
    }

    /// Whether `block` is worth building, given `graph` built so far. Call right before
    /// building each block: earlier blocks change the answer.
    pub fn should_build<R: Rng + ?Sized>(
        &self,
        graph: &GraphLayersBuilder,
        block: &PayloadBlock,
        skip: BlockSkip,
        rng: &mut R,
    ) -> bool {
        if !self.fits(block.cardinality) {
            return false;
        }

        let required_connectivity = match skip {
            BlockSkip::IfConnected => self.required_connectivity,
            BlockSkip::Never => None,
        };
        if let Some(required_connectivity) = required_connectivity {
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
