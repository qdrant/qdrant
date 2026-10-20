use std::ops::Deref as _;
use std::sync::atomic::{AtomicU64, Ordering};

use common::ambient;
use common::progress_tracker::ProgressTracker;
use common::reason::reason;
use common::types::PointOffsetType;
use log::debug;
use rayon::prelude::*;

use super::block::Block;
use super::context::GraphBuildContext;
use super::graph_builder::GraphBuilderMethod;
use super::local_builder::{LocalBuilder, LocalMethod};
use super::payload_blocks::FieldBlocks;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::{
    FINISH_MAIN_GRAPH_LOG_MESSAGE, HNSW_BUILD_MAX_PAR_LEN, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;

/// Inserts points one at a time, searching the graph built so far.
/// Healing and GPU wrap it and use it for everything but their own main graph.
pub(super) struct HnswBuilder<'a>(pub LocalBuilder<'a>);

impl HnswBuilder<'_> {
    /// Insert `points` into the main graph: the first few on one thread, to avoid disconnected
    /// components, the rest in parallel. Also used by healing (for the points it lacks) and as
    /// the GPU fallback.
    pub fn insert(
        &self,
        points: &[PointOffsetType],
        progress: ProgressTracker,
    ) -> OperationResult<()> {
        let LocalBuilder {
            ctx, pool, graph, ..
        } = &self.0;
        let GraphBuildContext {
            id_tracker,
            vector_storage,
            quantized_vectors,
            stopped,
            ..
        } = *ctx;

        let timer = std::time::Instant::now();

        progress.start();
        let counter = progress.track_progress(Some(points.len() as u64));
        let counter = counter.deref();

        let insert_point = |&vector_id: &PointOffsetType| {
            check_process_stopped(stopped)?;
            let _scope = ambient::unmeasured_guard(reason(
                "No need to accumulate hardware, since this is an internal operation",
            ));

            let points_scorer = FilteredScorer::new_internal(
                vector_id,
                vector_storage,
                quantized_vectors,
                None,
                id_tracker.deleted_point_bitslice(),
            )?;

            graph.link_new_point(vector_id, points_scorer);

            counter.fetch_add(1, Ordering::Relaxed);

            Ok::<_, OperationError>(())
        };

        let (first_few, rest) =
            points.split_at(points.len().min(SINGLE_THREADED_HNSW_BUILD_THRESHOLD));
        first_few.iter().try_for_each(insert_point)?;
        if !rest.is_empty() {
            pool.install(|| {
                rest.par_iter()
                    .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                    .try_for_each(insert_point)
            })?;
        }

        drop(progress);
        debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
        Ok(())
    }
}

impl<'a> LocalMethod<'a> for HnswBuilder<'a> {
    fn local(&mut self) -> &mut LocalBuilder<'a> {
        &mut self.0
    }

    fn build_block(
        &mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        let block = Block::new(self.0.ctx, points);
        let local_ids = (0..points.len() as PointOffsetType).collect::<Vec<_>>();
        block.insert(&local_ids, self.0.pool, counter)?;
        block.merge_into(&mut self.0.graph);
        Ok(())
    }
}

impl GraphBuilderMethod for HnswBuilder<'_> {
    fn build_main(&mut self) -> OperationResult<()> {
        if !self.0.ctx.has_main_graph() {
            return Ok(());
        }
        let progress = self.0.take_main_graph_progress();
        self.insert(&self.0.points, progress)
    }

    fn build_field_blocks(&mut self, field: FieldBlocks<'_>) -> OperationResult<()> {
        self.build_blocks(field)
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        Ok(self.0.finalize())
    }

    fn seeds_payload_blocks(&self) -> bool {
        false
    }
}
