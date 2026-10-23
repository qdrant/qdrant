use std::sync::atomic::{AtomicU64, Ordering};

use common::ambient;
use common::bitvec::{BitSliceExt as _, BitVec};
use common::reason::reason;
use common::types::PointOffsetType;
use rayon::ThreadPool;
use rayon::prelude::*;

use super::context::{GraphBuildContext, GraphParams};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::{
    HNSW_BUILD_MAX_PAR_LEN, HNSW_USE_HEURISTIC, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::vector_storage::VectorStorageRead;

/// One payload block being built, linking its points only to each other.
///
/// Block points are numbered `0..points.len()`, so the block graph's links and visited lists
/// are sized to the block, not the segment, and a search can only reach block points.
pub(super) struct Block<'a> {
    pub ctx: &'a GraphBuildContext<'a>,
    /// Segment ids of the block points.
    pub points: &'a [PointOffsetType],
    /// One flag per block point: whether it has no vector, or is deleted.
    pub deleted: BitVec,
    /// Links between block points, with the block parameters.
    pub graph: GraphLayersBuilder,
}

impl<'a> Block<'a> {
    pub fn new(ctx: &'a GraphBuildContext<'a>, points: &'a [PointOffsetType]) -> Self {
        let vector_deleted = ctx.vector_storage.deleted_vector_bitslice();
        let point_deleted = ctx.id_tracker.deleted_point_bitslice();
        let deleted = points
            .iter()
            .map(|&point_id| {
                vector_deleted.get_bit(point_id as usize).unwrap_or(false)
                    || point_deleted.get_bit(point_id as usize).unwrap_or(true)
            })
            .collect();

        let GraphParams {
            hnsw_m,
            ef_construct,
            entry_points_num,
        } = ctx.block_params;
        let graph = GraphLayersBuilder::new_with_params(
            points.len(),
            hnsw_m,
            ef_construct,
            entry_points_num,
            HNSW_USE_HEURISTIC,
            false,
        );

        Self {
            ctx,
            points,
            deleted,
            graph,
        }
    }

    /// Scorer against segment point `point_id`, taking block point ids.
    pub fn scorer(&self, point_id: PointOffsetType) -> OperationResult<FilteredScorer<'_>> {
        FilteredScorer::new_block_scorer(
            point_id,
            self.points,
            self.ctx.vector_storage,
            self.ctx.quantized_vectors,
            &self.deleted,
        )
    }

    /// Insert block points `local_ids` by search. The other block points must be in the block
    /// graph already.
    pub fn insert(
        &self,
        local_ids: &[PointOffsetType],
        pool: &ThreadPool,
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        let stopped = self.ctx.stopped;
        let insert_point = |&local_id: &PointOffsetType| {
            check_process_stopped(stopped)?;
            let _scope = ambient::unmeasured_guard(reason("Internal operation"));
            let points_scorer = self.scorer(self.points[local_id as usize])?;
            self.graph.link_new_point(local_id, points_scorer);
            counter.fetch_add(1, Ordering::Relaxed);
            Ok::<_, OperationError>(())
        };

        // First index points in single thread so ensure warm start for parallel indexing process.
        // Once initial structure is built, index remaining points in parallel, so that each thread
        // inserts points in different parts of the graph and is less likely to compete for locks.
        let present_points = self.points.len() - local_ids.len();
        let first_points = SINGLE_THREADED_HNSW_BUILD_THRESHOLD
            .saturating_sub(present_points)
            .min(local_ids.len());
        let (first, rest) = local_ids.split_at(first_points);
        first.iter().try_for_each(insert_point)?;
        if !rest.is_empty() {
            pool.install(|| {
                rest.par_iter()
                    .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                    .try_for_each(insert_point)
            })?;
        }
        Ok(())
    }

    /// Add the block links to `graph`, the segment graph.
    pub fn merge_into(self, graph: &mut GraphLayersBuilder) {
        graph.merge_block(self.graph, self.points);
    }
}
