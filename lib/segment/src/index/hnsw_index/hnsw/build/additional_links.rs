use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use common::ambient;
use common::bitvec::{BitSliceExt as _, BitVec};
use common::progress_tracker::ProgressTracker;
use common::reason::reason;
use common::types::{DeferredBehavior, PointOffsetType};
use log::{debug, trace};
use rand::Rng;
use rayon::ThreadPool;
use rayon::prelude::*;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::PayloadIndexRead;
use crate::index::field_index::PayloadBlockCondition;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::{
    HNSW_BUILD_MAX_PAR_LEN, HNSW_USE_HEURISTIC, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::Condition::Field;
use crate::types::{FieldCondition, Filter, PayloadFieldSchema, PayloadKeyType};
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
    let fields = in_build_order(fields)
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

/// Indexed fields in the order their additional links are built.
///
/// The order matters: every field after the first skips blocks the graph built so far already
/// connects well, so it decides which blocks get built. `indexed_fields` is a `HashMap`, whose
/// iteration order changes from one map to the next, so sort to make builds reproducible.
fn in_build_order(
    fields: HashMap<PayloadKeyType, PayloadFieldSchema>,
) -> Vec<(PayloadKeyType, PayloadFieldSchema)> {
    let mut fields = fields.into_iter().collect::<Vec<_>>();
    fields.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    fields
}

/// Build per-payload-block subgraphs for every field in `indexed_fields` and merge them
/// into `graph_layers_builder`.
///
/// Returns the number of vectors that got indexed through these subgraphs.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(
    not(feature = "gpu"),
    allow(unused_variables, clippy::needless_pass_by_ref_mut)
)]
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

    // The GPU stores vectors and links by segment id, so it builds blocks in a segment-sized
    // builder. This one only carries the block graph's settings: `payload_m`, `ef_construct`
    // and level 0 for every point.
    #[cfg(feature = "gpu")]
    let gpu_block_reference = gpu_insert_context.is_some().then(|| {
        GraphLayersBuilder::new_with_params(
            total_vector_count,
            payload_m,
            config.ef_construct,
            1,
            HNSW_USE_HEURISTIC,
            false,
        )
    });

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

            if !indexed_vectors_set.is_empty() {
                for &point_id in &points_to_index {
                    indexed_vectors_set.set(point_id as usize, true);
                }
            }

            #[cfg(feature = "gpu")]
            if let (Some(gpu_insert_context), Some(reference_graph)) =
                (gpu_insert_context.as_mut(), &gpu_block_reference)
                && let Some(gpu_graph) =
                    crate::index::hnsw_index::hnsw::gpu_build::build_block_on_gpu(
                        id_tracker,
                        vector_storage,
                        quantized_vectors,
                        gpu_insert_context,
                        reference_graph,
                        &points_to_index,
                        stopped,
                    )?
            {
                graph_layers_builder.merge_from_other(gpu_graph);
                return Ok(());
            }

            build_block(
                id_tracker,
                vector_storage,
                quantized_vectors,
                pool,
                stopped,
                graph_layers_builder,
                &points_to_index,
                payload_m,
                config.ef_construct,
                &counter,
            )
        };

        payload_index.with_view(|v| {
            v.for_each_payload_block(&field, config.full_scan_threshold, &mut process_block)
        })?;
    }
    Ok(indexed_vectors_set.count_ones())
}

/// Build one payload block, linking its points only to each other, and merge it into
/// `graph_layers_builder`.
///
/// Block points are numbered `0..points_to_index.len()`, so the builder's links and visited
/// lists are sized to the block, not the segment, and a search can only reach block points.
#[allow(clippy::too_many_arguments)]
fn build_block(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    pool: &ThreadPool,
    stopped: &AtomicBool,
    graph_layers_builder: &mut GraphLayersBuilder,
    points_to_index: &[PointOffsetType],
    payload_m: HnswM,
    ef_construct: usize,
    counter: &AtomicU64,
) -> OperationResult<()> {
    let vector_deleted = vector_storage.deleted_vector_bitslice();
    let point_deleted = id_tracker.deleted_point_bitslice();
    let block_deleted: BitVec = points_to_index
        .iter()
        .map(|&point_id| {
            vector_deleted.get_bit(point_id as usize).unwrap_or(false)
                || point_deleted.get_bit(point_id as usize).unwrap_or(true)
        })
        .collect();

    let block_graph = GraphLayersBuilder::new_with_params(
        points_to_index.len(),
        payload_m,
        ef_construct,
        1,
        HNSW_USE_HEURISTIC,
        false,
    );

    let insert_point = |local_id: PointOffsetType| {
        check_process_stopped(stopped)?;
        let _scope = ambient::unmeasured_guard(reason("Internal operation"));
        let points_scorer = FilteredScorer::new_block_scorer(
            points_to_index[local_id as usize],
            points_to_index,
            vector_storage,
            quantized_vectors.as_ref(),
            &block_deleted,
        )?;
        block_graph.link_new_point(local_id, points_scorer);
        counter.fetch_add(1, Ordering::Relaxed);
        Ok::<_, OperationError>(())
    };

    // First index points in single thread so ensure warm start for parallel indexing process.
    // Once initial structure is built, index remaining points in parallel, so that each thread
    // inserts points in different parts of the graph and is less likely to compete for locks.
    let num_points = points_to_index.len() as PointOffsetType;
    let first_points = num_points.min(SINGLE_THREADED_HNSW_BUILD_THRESHOLD as PointOffsetType);
    for local_id in 0..first_points {
        insert_point(local_id)?;
    }
    if num_points > first_points {
        pool.install(|| {
            (first_points..num_points)
                .into_par_iter()
                .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                .try_for_each(insert_point)
        })?;
    }

    graph_layers_builder.merge_block(block_graph, points_to_index);
    Ok(())
}

/// Get list of points for indexing, associated with payload block filtering condition
fn condition_points(
    condition: FieldCondition,
    payload_index: &StructPayloadIndex,
    vector_storage: &VectorStorageEnum,
    stopped: &AtomicBool,
) -> OperationResult<Vec<PointOffsetType>> {
    let filter = Filter::new_must(Field(condition));

    let _scope = ambient::unmeasured_guard(reason("Internal operation"));

    let deleted_bitslice = vector_storage.deleted_vector_bitslice();

    payload_index.with_view(|v| {
        let cardinality_estimation = v.estimate_cardinality(&filter)?;
        Ok(v.iter_filtered_points(
            &filter,
            &cardinality_estimation,
            stopped,
            DeferredBehavior::WithDeferred,
        )?
        .filter(|&point_id| !deleted_bitslice.get_bit(point_id as usize).unwrap_or(false))
        .collect())
    })
}
