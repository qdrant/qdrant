use std::sync::atomic::AtomicU64;

use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use rand::Rng;
use rayon::ThreadPool;

use super::block_filter::{BlockFilter, BlockSkip};
use super::context::{GraphBuildContext, GraphParams};
use super::payload_blocks::{FieldBlocks, PayloadBlock};
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::HNSW_USE_HEURISTIC;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::vector_storage::VectorStorageRead;

/// Progress phase of linking points into the main graph.
const MAIN_GRAPH_PHASE: &str = "main_graph";

/// State shared by every method that builds the graph on this machine. The methods wrap it
/// and implement the algorithms themselves.
pub(super) struct LocalBuilder<'a> {
    pub ctx: GraphBuildContext<'a>,
    pub pool: &'a ThreadPool,
    /// Borrowed for the whole build: levels, connectivity sampling for blocks.
    pub rng: &'a mut dyn Rng,
    /// The non-deleted points, in offset order.
    pub points: Vec<PointOffsetType>,
    /// The segment graph, with `main_params` and every point's level from the start,
    /// so it is there for blocks even when `m == 0`.
    pub graph: GraphLayersBuilder,
    /// Progress of linking the main graph, created with the builder so that it is listed before
    /// the payload blocks. `None` without a main graph, or once taken by
    /// [`Self::take_main_graph_progress`].
    pub main_graph_progress: Option<ProgressTracker>,
    /// Decides which blocks get built. Created from `graph` at the first block.
    block_filter: Option<BlockFilter>,
}

impl<'a> LocalBuilder<'a> {
    /// Collect the points, then assign levels: from `old` when healing, random otherwise.
    pub fn new(
        ctx: GraphBuildContext<'a>,
        pool: &'a ThreadPool,
        rng: &'a mut dyn Rng,
        old: Option<&OldIndex<'_>>,
    ) -> OperationResult<Self> {
        let GraphParams {
            hnsw_m,
            ef_construct,
            entry_points_num,
        } = ctx.main_params;
        let mut graph = GraphLayersBuilder::new(
            ctx.vector_storage.total_vector_count(),
            hnsw_m,
            ef_construct,
            entry_points_num,
            HNSW_USE_HEURISTIC,
        );

        let points = ctx
            .id_tracker
            .point_mappings()
            .iter_internal_excluding(ctx.vector_storage.deleted_vector_bitslice())
            .collect::<Vec<_>>();
        for &point_id in &points {
            check_process_stopped(ctx.stopped)?;
            let level = old
                .and_then(|old| old.point_level(point_id))
                .unwrap_or_else(|| graph.get_random_layer(rng));
            graph.set_levels(point_id, level);
        }

        let main_graph_progress = ctx
            .has_main_graph()
            .then(|| ctx.progress.subtask(MAIN_GRAPH_PHASE));

        Ok(Self {
            ctx,
            pool,
            rng,
            points,
            graph,
            main_graph_progress,
            block_filter: None,
        })
    }

    /// The progress of linking the main graph, for `build_main`. Created here if it was taken
    /// already, so the main graph is never skipped for want of it.
    pub fn take_main_graph_progress(&mut self) -> ProgressTracker {
        self.main_graph_progress
            .take()
            .unwrap_or_else(|| self.ctx.progress.subtask(MAIN_GRAPH_PHASE))
    }

    /// Whether a block of `cardinality` points is small enough to build. Checked before its
    /// points are read, so that blocks too large to build are not read at all.
    pub fn fits(&mut self, cardinality: usize) -> bool {
        let Self {
            ctx,
            pool: _,
            rng,
            points,
            graph,
            main_graph_progress: _,
            block_filter,
        } = self;
        block_filter
            .get_or_insert_with(|| BlockFilter::new(*ctx, graph, points, &mut **rng))
            .fits(cardinality)
    }

    /// Whether `block` is worth building, given the graph built so far. Call right before
    /// building each block: earlier blocks change the answer.
    pub fn should_build(&mut self, block: &PayloadBlock, skip: BlockSkip) -> bool {
        let Self {
            ctx,
            pool: _,
            rng,
            points,
            graph,
            main_graph_progress: _,
            block_filter,
        } = self;
        block_filter
            .get_or_insert_with(|| BlockFilter::new(*ctx, graph, points, &mut **rng))
            .should_build(graph, block, skip, &mut **rng)
    }

    /// The graph, and the number of indexed vectors.
    pub fn finalize(self) -> (GraphLayersBuilder, usize) {
        let indexed_vectors = self.points.len();
        (self.graph, indexed_vectors)
    }
}

/// A method that builds the graph on this machine, on top of a [`LocalBuilder`].
pub(super) trait LocalMethod<'a> {
    /// The state the method builds on.
    fn local(&mut self) -> &mut LocalBuilder<'a>;

    /// Link one payload block in its own id space and merge it into the graph.
    fn build_block(
        &mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()>;

    /// Build the blocks of `field` worth building, given the graph built so far, with
    /// [`Self::build_block`].
    fn build_blocks(&mut self, field: FieldBlocks<'_>) -> OperationResult<()> {
        let FieldBlocks {
            progress,
            skip,
            blocks,
            loader,
        } = field;
        let counter = progress.track_progress(None);
        for block in blocks {
            check_process_stopped(self.local().ctx.stopped)?;
            // Read the points of one block at a time, and only of blocks small enough to build
            if !self.local().fits(block.cardinality) {
                continue;
            }
            let Some(block) = loader.load(block)? else {
                continue;
            };
            if self.local().should_build(&block, skip) {
                self.build_block(&block.points, &counter)?;
            }
        }
        Ok(())
    }
}
