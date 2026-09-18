use std::ops::Deref as _;
use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::progress_tracker::ProgressTracker;
use log::debug;
use rayon::ThreadPool;
use rayon::prelude::*;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_layers_healer::GraphLayersHealer;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::hnsw_index::hnsw::{
    FINISH_MAIN_GRAPH_LOG_MESSAGE, HNSW_BUILD_MAX_PAR_LEN, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Link every non-deleted point into `graph_layers_builder`, whose levels must already be set.
///
/// With an `old_index`, its graph is healed into the builder first and only points absent
/// from it are inserted.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_main_graph_on_cpu(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    old_index: Option<OldIndex<'_>>,
    graph_layers_builder: &GraphLayersBuilder,
    ef_construct: usize,
    progress_migrate: ProgressTracker,
    progress_main_graph: ProgressTracker,
    pool: &ThreadPool,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    let total_vector_count = vector_storage.total_vector_count();
    let deleted_bitslice = vector_storage.deleted_vector_bitslice();

    let mut ids = Vec::with_capacity(total_vector_count);
    let mut first_few_ids = Vec::with_capacity(SINGLE_THREADED_HNSW_BUILD_THRESHOLD);

    let mut ids_iter = id_tracker
        .point_mappings()
        .iter_internal_excluding(deleted_bitslice);
    if let Some(old_index) = old_index {
        progress_migrate.start();

        let timer = std::time::Instant::now();

        let mut healer =
            GraphLayersHealer::new(old_index.graph(), &old_index.old_to_new, ef_construct);
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

        for vector_id in ids_iter {
            if old_index.new_to_old[vector_id as usize].is_none() {
                if first_few_ids.len() < SINGLE_THREADED_HNSW_BUILD_THRESHOLD {
                    first_few_ids.push(vector_id);
                } else {
                    ids.push(vector_id);
                }
            }
        }

        debug!("Migrated in {:?}", timer.elapsed());
    } else {
        first_few_ids.extend(ids_iter.by_ref().take(SINGLE_THREADED_HNSW_BUILD_THRESHOLD));
        ids.extend(ids_iter);
    }
    drop(progress_migrate);

    let timer = std::time::Instant::now();

    progress_main_graph.start();
    let counter =
        progress_main_graph.track_progress(Some(first_few_ids.len() as u64 + ids.len() as u64));
    let counter = counter.deref();

    let insert_point = |vector_id| {
        check_process_stopped(stopped)?;
        // No need to accumulate hardware, since this is an internal operation
        let internal_hardware_counter = HardwareCounterCell::disposable();

        let points_scorer = FilteredScorer::new_internal(
            vector_id,
            vector_storage,
            quantized_vectors.as_ref(),
            None,
            id_tracker.deleted_point_bitslice(),
            internal_hardware_counter,
        )?;

        graph_layers_builder.link_new_point(vector_id, points_scorer);

        counter.fetch_add(1, Ordering::Relaxed);

        Ok::<_, OperationError>(())
    };

    for vector_id in first_few_ids {
        insert_point(vector_id)?;
    }

    if !ids.is_empty() {
        pool.install(|| {
            ids.into_par_iter()
                .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                .try_for_each(insert_point)
        })?;
    }

    drop(progress_main_graph);
    debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
    Ok(())
}
