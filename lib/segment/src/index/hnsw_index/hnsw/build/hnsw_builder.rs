use std::ops::Deref as _;
use std::sync::atomic::{AtomicU64, Ordering};

use common::counter::hw;
use common::reason::reason;
use common::types::PointOffsetType;
use log::debug;
use rayon::prelude::*;

use super::graph_builder::{GraphBuildContext, GraphBuilder};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_layers_healer::GraphLayersHealer;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::hnsw_index::hnsw::{
    FINISH_MAIN_GRAPH_LOG_MESSAGE, HNSW_BUILD_MAX_PAR_LEN, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::visited_pool::VisitedListHandle;
use crate::vector_storage::VectorStorageRead;

/// Inserts points one by one on the CPU, searching the graph built so far for their neighbors.
pub(super) struct HnswGraphBuilder;

impl HnswGraphBuilder {
    /// Heal the graph of `old_index` into `graph_layers_builder`, then insert the points it lacks.
    ///
    /// Returns `false`, leaving `graph_layers_builder` untouched, when there is no old graph
    /// worth healing. Builders that cannot heal call this before building from scratch, so
    /// whether to heal is decided in one place.
    pub(super) fn try_heal(
        context: GraphBuildContext<'_>,
        graph_layers_builder: &GraphLayersBuilder,
        old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<bool> {
        let Some(old_index) = old_index else {
            return Ok(false);
        };
        let GraphBuildContext {
            id_tracker,
            vector_storage,
            pool,
            progress,
            stopped,
            ..
        } = context;

        let progress_migrate = progress.running_subtask("migrate");
        let timer = std::time::Instant::now();

        let mut healer = GraphLayersHealer::new(
            old_index.graph(),
            &old_index.old_to_new,
            graph_layers_builder.ef_construct(),
        );
        let old_vector_storage = old_index.index.vector_storage.borrow();
        let old_quantized_vectors = old_index.index.quantized_vectors.borrow();

        let counter = progress_migrate.track_progress(Some(healer.to_heal_count() as u64));
        healer.heal(
            pool,
            &old_vector_storage,
            old_quantized_vectors.as_ref(),
            stopped,
            counter.deref(),
        )?;
        check_process_stopped(stopped)?;
        healer.save_into_builder(graph_layers_builder);

        let new_points = id_tracker
            .point_mappings()
            .iter_internal_excluding(vector_storage.deleted_vector_bitslice())
            .filter(|&vector_id| old_index.new_to_old[vector_id as usize].is_none())
            .collect();

        debug!("Migrated in {:?}", timer.elapsed());
        drop(progress_migrate);

        Self::insert_points(context, graph_layers_builder, new_points)?;
        Ok(true)
    }

    /// Link `ids` into `graph_layers_builder`, whose levels must already be set.
    fn insert_points(
        context: GraphBuildContext<'_>,
        graph_layers_builder: &GraphLayersBuilder,
        ids: Vec<PointOffsetType>,
    ) -> OperationResult<()> {
        let GraphBuildContext {
            id_tracker,
            vector_storage,
            quantized_vectors,
            pool,
            progress,
            stopped,
            ..
        } = context;

        let timer = std::time::Instant::now();

        let progress_main_graph = progress.running_subtask("main_graph");
        let counter = progress_main_graph.track_progress(Some(ids.len() as u64));
        let counter = counter.deref();

        let insert_point = |vector_id| {
            check_process_stopped(stopped)?;
            let _hw = hw::unmeasured_guard(reason(
                "No need to accumulate hardware, since this is an internal operation",
            ));

            let points_scorer = FilteredScorer::new_internal(
                vector_id,
                vector_storage,
                quantized_vectors.as_ref(),
                None,
                id_tracker.deleted_point_bitslice(),
            )?;

            graph_layers_builder.link_new_point(vector_id, points_scorer);

            counter.fetch_add(1, Ordering::Relaxed);

            Ok::<_, OperationError>(())
        };

        // First points are linked in a single thread, to avoid disconnected components
        let first_few = ids.len().min(SINGLE_THREADED_HNSW_BUILD_THRESHOLD);
        for vector_id in ids[..first_few].iter().copied() {
            insert_point(vector_id)?;
        }

        if ids.len() > first_few {
            pool.install(|| {
                ids[first_few..]
                    .par_iter()
                    .copied()
                    .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                    .try_for_each(insert_point)
            })?;
        }

        drop(progress_main_graph);
        debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
        Ok(())
    }
}

impl GraphBuilder for HnswGraphBuilder {
    fn build_main_graph(
        &self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: GraphLayersBuilder,
        old_index: Option<OldIndex<'_>>,
    ) -> OperationResult<GraphLayersBuilder> {
        if !Self::try_heal(context, &graph_layers_builder, old_index)? {
            let ids = context
                .id_tracker
                .point_mappings()
                .iter_internal_excluding(context.vector_storage.deleted_vector_bitslice())
                .collect();
            Self::insert_points(context, &graph_layers_builder, ids)?;
        }
        Ok(graph_layers_builder)
    }

    /// The block is built in `block_graph` and then moved into `graph_layers_builder`.
    fn build_block_graph(
        &mut self,
        context: GraphBuildContext<'_>,
        graph_layers_builder: &mut GraphLayersBuilder,
        block_graph: &mut GraphLayersBuilder,
        block_filter_list: &VisitedListHandle,
        points_to_index: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        let GraphBuildContext {
            id_tracker,
            vector_storage,
            quantized_vectors,
            pool,
            stopped,
            ..
        } = context;

        let insert_points = |block_point_id| {
            check_process_stopped(stopped)?;

            let _hw = hw::unmeasured_guard(reason(
                "This hardware counter can be discarded, since it is only used for internal operations",
            ));

            let block_condition_checker =
                OptimizedFilter::from_checker(ConditionCheckerEnum::Build(BuildConditionChecker {
                    filter_list: block_filter_list,
                    current_point: block_point_id,
                }));
            let points_scorer = FilteredScorer::new_internal(
                block_point_id,
                vector_storage,
                quantized_vectors.as_ref(),
                Some(block_condition_checker),
                id_tracker.deleted_point_bitslice(),
            )?;

            block_graph.link_new_point(block_point_id, points_scorer);

            counter.fetch_add(1, Ordering::Relaxed);

            Ok::<_, OperationError>(())
        };

        let first_points = points_to_index
            .len()
            .min(SINGLE_THREADED_HNSW_BUILD_THRESHOLD);

        // First index points in single thread so ensure warm start for parallel indexing process
        for point_id in points_to_index[..first_points].iter().copied() {
            insert_points(point_id)?;
        }
        // Once initial structure is built, index remaining points in parallel
        // So that each thread will insert points in different parts of the graph,
        // it is less likely that they will compete for the same locks
        if points_to_index.len() > first_points {
            pool.install(|| {
                points_to_index[first_points..]
                    .par_iter()
                    .copied()
                    .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                    .try_for_each(insert_points)
            })?;
        }

        graph_layers_builder.merge_block_from(block_graph, points_to_index);
        Ok(())
    }
}
