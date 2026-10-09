use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::bitvec::{BitSliceExt as _, BitVec};
use common::counter::hw;
use common::reason::reason;
use common::types::{DeferredBehavior, PointOffsetType};
use log::{debug, trace};
use rand::Rng;

use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::PayloadBlockCondition;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::HNSW_USE_HEURISTIC;
use crate::index::hnsw_index::hnsw::build::graph_builder::{
    GraphBuildContext, GraphBuilder as _, GraphBuilderEnum,
};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::visited_pool::VisitedPool;
use crate::json_path::JsonPath;
use crate::types::Condition::Field;
use crate::types::{FieldCondition, Filter, PayloadFieldSchema, PayloadKeyType};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Fields that get additional HNSW links, in build order, each with the name of its progress
/// subtask. Empty when there is nothing to build.
pub(super) fn additional_links_fields(
    payload_index: &StructPayloadIndex,
    payload_m: HnswM,
) -> Vec<(String, JsonPath)> {
    if payload_m.m == 0 {
        return Vec::new();
    }
    let fields = payload_index.with_view(|v| v.indexed_fields());
    in_build_order(fields)
        .into_iter()
        .filter_map(|(field, payload_schema)| {
            if payload_schema.enable_hnsw() {
                Some((format!("{}:{field}", payload_schema.name()), field))
            } else {
                debug!("enable_hnsw=false. Skip building additional index for field {field}");
                None
            }
        })
        .collect()
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

/// Build per-payload-block subgraphs for every field in `fields`, from
/// [`additional_links_fields`], and merge them into `graph_layers_builder`.
///
/// Returns the number of vectors that got indexed through these subgraphs.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_additional_links<R: Rng + ?Sized>(
    context: GraphBuildContext<'_>,
    payload_index: &StructPayloadIndex,
    graph_builder: &mut GraphBuilderEnum,
    graph_layers_builder: &mut GraphLayersBuilder,
    config: &HnswGraphConfig,
    payload_m: HnswM,
    fields: Vec<(String, JsonPath)>,
    rng: &mut R,
) -> OperationResult<usize> {
    let GraphBuildContext {
        id_tracker,
        vector_storage,
        progress,
        stopped,
        ..
    } = context;
    let total_vector_count = vector_storage.total_vector_count();
    let deleted_bitslice = vector_storage.deleted_vector_bitslice();

    let progress_additional_links = progress.running_subtask("additional_links");
    let indexed_fields = fields
        .into_iter()
        .map(|(subtask_name, field)| (progress_additional_links.subtask(subtask_name), field))
        .collect::<Vec<_>>();

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

    // One builder serves every block. It is sized for the whole segment, so allocating it
    // per block, and merging it back by scanning every point, would cost O(segment) on a
    // single thread for each block. `merge_block_from` moves out only the block's links and
    // leaves the builder empty for the next block.
    let mut block_graph: Option<GraphLayersBuilder> = None;

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

            let additional_graph = block_graph.get_or_insert_with(|| {
                GraphLayersBuilder::new_with_params(
                    total_vector_count,
                    payload_m,
                    config.ef_construct,
                    1,
                    HNSW_USE_HEURISTIC,
                    false,
                )
            });

            block_filter_list.next_iteration();
            for block_point_id in points_to_index.iter().copied() {
                block_filter_list.check_and_update_visited(block_point_id);
                if !indexed_vectors_set.is_empty() {
                    indexed_vectors_set.set(block_point_id as usize, true);
                }
            }

            graph_builder.build_block_graph(
                context,
                graph_layers_builder,
                additional_graph,
                &block_filter_list,
                &points_to_index,
                &counter,
            )
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

    let _hw = hw::unmeasured_guard(reason("Internal operation"));

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
