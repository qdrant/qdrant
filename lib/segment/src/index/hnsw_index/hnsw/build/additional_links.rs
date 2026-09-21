use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use common::bitvec::{BitSliceExt as _, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::progress_tracker::ProgressTracker;
use common::types::{DeferredBehavior, PointOffsetType};
use log::{debug, trace};
use rand::Rng;
use rayon::ThreadPool;
use rayon::prelude::*;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::PayloadIndexRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::PayloadBlockCondition;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::{
    HNSW_BUILD_MAX_PAR_LEN, HNSW_USE_HEURISTIC, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};
use crate::json_path::JsonPath;
use crate::types::Condition::Field;
use crate::types::{FieldCondition, Filter};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Fields that get additional HNSW links, each paired with its progress subtask under the
/// returned `additional_links` tracker. `None` when there is nothing to build.
pub(super) fn additional_links_fields(
    payload_index: &StructPayloadIndex,
    payload_m: HnswM,
    progress: &ProgressTracker,
) -> Option<(ProgressTracker, Vec<(ProgressTracker, JsonPath)>)> {
    if payload_m.m == 0 {
        return None;
    }
    let fields = payload_index.with_view(|v| v.indexed_fields());
    if fields.is_empty() {
        return None;
    }
    let progress_additional_links = progress.subtask("additional_links");
    let fields = fields
        .into_iter()
        .filter_map(|(field, payload_schema)| {
            let subtask_name = format!("{}:{field}", payload_schema.name());
            if payload_schema.enable_hnsw() {
                Some((progress_additional_links.subtask(subtask_name), field))
            } else {
                debug!("enable_hnsw=false. Skip building additional index for field {field}");
                None
            }
        })
        .collect::<Vec<_>>();
    Some((progress_additional_links, fields))
}

/// Build per-payload-block subgraphs for every field in `indexed_fields` and merge them
/// into `graph_layers_builder`.
///
/// Returns the number of vectors that got indexed through these subgraphs.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_additional_links<R: Rng + ?Sized>(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    payload_index: &StructPayloadIndex,
    gpu_insert_context: &mut Option<GpuInsertContext<'_>>,
    graph_layers_builder: &mut GraphLayersBuilder,
    config: &HnswGraphConfig,
    payload_m: HnswM,
    indexed_fields: Vec<(ProgressTracker, JsonPath)>,
    progress_additional_links: ProgressTracker,
    pool: &ThreadPool,
    rng: &mut R,
    stopped: &AtomicBool,
) -> OperationResult<usize> {
    let total_vector_count = vector_storage.total_vector_count();
    let deleted_bitslice = vector_storage.deleted_vector_bitslice();

    progress_additional_links.start();

    // Calculate true average number of links per vertex in the HNSW graph
    // to better estimate percolation threshold
    let average_links_per_0_level = graph_layers_builder.get_average_connectivity_on_level(0);
    let average_links_per_0_level_int = (average_links_per_0_level as usize).max(1);

    // Estimate connectivity of the main graph
    let all_points = id_tracker
        .point_mappings()
        .iter_internal_excluding(deleted_bitslice)
        .collect::<Vec<_>>();

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
            graph_layers_builder.subgraph_connectivity(rng, &all_points, percolation),
            graph_layers_builder.subgraph_connectivity(rng, &all_points, percolation),
            graph_layers_builder.subgraph_connectivity(rng, &all_points, percolation),
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

    let mut indexed_vectors_set = if config.m != 0 {
        // Every vector is already indexed in the main graph, so skip counting.
        BitVec::new()
    } else {
        BitVec::repeat(false, total_vector_count)
    };

    let visited_pool = VisitedPool::new();
    let mut block_filter_list = visited_pool.get(total_vector_count);

    for (index_pos, (field_progress, field)) in indexed_fields.into_iter().enumerate() {
        field_progress.start();

        debug!("building additional index for field {field}");

        let is_tenant = payload_index.is_tenant(&field);

        // It is expected, that graph will become disconnected less than
        // $1/m$ points left.
        // So blocks larger than $1/m$ are not needed.
        // We add multiplier for the extra safety.
        const PERCOLATION_MULTIPLIER: usize = 4;
        let max_block_size = if config.m > 0 {
            total_vector_count / average_links_per_0_level_int * PERCOLATION_MULTIPLIER
        } else {
            usize::MAX
        };

        let counter = field_progress.track_progress(None);

        let mut process_block = |payload_block: PayloadBlockCondition| {
            check_process_stopped(stopped)?;

            if payload_block.cardinality > max_block_size {
                return Ok(());
            }

            let points_to_index = condition_points(
                payload_block.condition,
                payload_index,
                vector_storage,
                stopped,
            )?;

            // This is a heuristic to skip building graph for mostly deleted blocks.
            // It might be, that majority of points do not actually have vectors
            // (vectors marked as deleted), so we can avoid building graph for such blocks.
            //
            // FYI: query heuristic does account
            // for deleted vectors via [`adjust_to_available_vectors`]
            const DELETED_POINTS_FACTOR: usize = 4; // allow block to have up to 75% of deleted points and still be indexed

            if points_to_index.len() <= config.full_scan_threshold / DELETED_POINTS_FACTOR {
                return Ok(());
            }

            if !is_tenant
                && index_pos > 0
                && let Some(required_connectivity) = required_connectivity
            {
                // Always build for tenants
                let graph_connectivity =
                    graph_layers_builder.subgraph_connectivity(rng, &points_to_index, percolation);

                if graph_connectivity >= required_connectivity {
                    trace!(
                        "skip building additional HNSW links for {field}, connectivity {graph_connectivity:.4} >= {required_connectivity:.4}"
                    );
                    return Ok(());
                }
                trace!("graph connectivity: {graph_connectivity} for {field}");
            }

            // ToDo: reuse graph layer for same payload
            let mut additional_graph = GraphLayersBuilder::new_with_params(
                total_vector_count,
                payload_m,
                config.ef_construct,
                1,
                HNSW_USE_HEURISTIC,
                false,
            );

            build_filtered_graph(
                id_tracker,
                vector_storage,
                quantized_vectors,
                gpu_insert_context,
                payload_index,
                pool,
                stopped,
                &mut additional_graph,
                points_to_index,
                &mut block_filter_list,
                &mut indexed_vectors_set,
                &counter,
            )?;
            graph_layers_builder.merge_from_other(additional_graph);
            Ok(())
        };

        payload_index.with_view(|v| {
            v.for_each_payload_block(&field, config.full_scan_threshold, &mut process_block)
        })?;
    }
    Ok(indexed_vectors_set.count_ones())
}

/// Get list of points for indexing, associated with payload block filtering condition
fn condition_points(
    condition: FieldCondition,
    payload_index: &StructPayloadIndex,
    vector_storage: &VectorStorageEnum,
    stopped: &AtomicBool,
) -> OperationResult<Vec<PointOffsetType>> {
    let filter = Filter::new_must(Field(condition));

    let disposed_hw_counter = HardwareCounterCell::disposable(); // Internal operation. No measurements needed

    let deleted_bitslice = vector_storage.deleted_vector_bitslice();

    payload_index.with_view(|v| {
        let cardinality_estimation = v.estimate_cardinality(&filter, &disposed_hw_counter)?;
        Ok(v.iter_filtered_points(
            &filter,
            &cardinality_estimation,
            &disposed_hw_counter,
            stopped,
            DeferredBehavior::WithDeferred,
        )?
        .filter(|&point_id| !deleted_bitslice.get_bit(point_id as usize).unwrap_or(false))
        .collect())
    })
}

#[allow(clippy::too_many_arguments)]
#[allow(unused_variables)]
#[allow(clippy::needless_pass_by_ref_mut)]
fn build_filtered_graph(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    #[allow(unused_variables)] gpu_insert_context: &mut Option<GpuInsertContext<'_>>,
    payload_index: &StructPayloadIndex,
    pool: &ThreadPool,
    stopped: &AtomicBool,
    graph_layers_builder: &mut GraphLayersBuilder,
    points_to_index: Vec<PointOffsetType>,
    block_filter_list: &mut VisitedListHandle,
    indexed_vectors_set: &mut BitVec,
    counter: &AtomicU64,
) -> OperationResult<()> {
    block_filter_list.next_iteration();

    for block_point_id in points_to_index.iter().copied() {
        block_filter_list.check_and_update_visited(block_point_id);
        if !indexed_vectors_set.is_empty() {
            indexed_vectors_set.set(block_point_id as usize, true);
        }
    }

    #[cfg(feature = "gpu")]
    if let Some(gpu_constructed_graph) =
        crate::index::hnsw_index::hnsw::gpu_build::build_filtered_graph_on_gpu(
            id_tracker,
            vector_storage,
            quantized_vectors,
            gpu_insert_context.as_mut(),
            graph_layers_builder,
            block_filter_list,
            &points_to_index,
            stopped,
        )?
    {
        *graph_layers_builder = gpu_constructed_graph;
        return Ok(());
    }

    let insert_points = |block_point_id| {
        check_process_stopped(stopped)?;

        // This hardware counter can be discarded, since it is only used for internal operations
        let internal_hardware_counter = HardwareCounterCell::disposable();

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
            internal_hardware_counter,
        )?;

        graph_layers_builder.link_new_point(block_point_id, points_scorer);

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
            points_to_index
                .into_par_iter()
                .skip(first_points)
                .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                .try_for_each(insert_points)
        })?;
    }
    Ok(())
}
