use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use common::ambient;
use common::bitvec::{BitSliceExt as _, BitVec};
use common::reason::reason;
use common::types::PointOffsetType;
use rayon::ThreadPool;
use rayon::prelude::*;

use super::seeds::{BlockContext, BlockSeeds};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::{
    HNSW_BUILD_MAX_PAR_LEN, HNSW_USE_HEURISTIC, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::links_container::ItemsBuffer;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Build one payload block, linking its points only to each other, and merge it into
/// `graph_layers_builder`.
///
/// Block points are numbered `0..points_to_index.len()`, so the builder's links and visited
/// lists are sized to the block, not the segment, and a search can only reach block points.
///
/// With `seeds`, points that kept enough links to the block from the old graph are added with
/// those links, and only the rest are inserted by search. Returns the number of seeded points.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_block(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    pool: &ThreadPool,
    stopped: &AtomicBool,
    graph_layers_builder: &mut GraphLayersBuilder,
    points_to_index: &[PointOffsetType],
    seeds: Option<&mut BlockSeeds>,
    payload_m: HnswM,
    ef_construct: usize,
    counter: &AtomicU64,
) -> OperationResult<usize> {
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

    let num_points = points_to_index.len() as PointOffsetType;
    let to_insert: Vec<PointOffsetType> = match seeds {
        None => (0..num_points).collect(),
        Some(seeds) => {
            seeds.enter_block(points_to_index);
            let block_seeds = &*seeds;
            let payload_m0 = block_seeds.payload_m0;
            let block = BlockContext {
                points: points_to_index,
                deleted: &block_deleted,
                vector_storage,
                quantized_vectors,
            };
            let final_graph = &*graph_layers_builder;
            let seeded = pool.install(|| {
                (0..num_points)
                    .into_par_iter()
                    .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                    .map(|local_id| {
                        check_process_stopped(stopped)?;
                        let _scope = ambient::unmeasured_guard(reason("Internal operation"));
                        let replacements =
                            block_seeds.seed_point(local_id, &block, &block_graph, final_graph)?;
                        if replacements.is_some() {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok::<_, OperationError>((local_id, replacements))
                    })
                    .collect::<OperationResult<Vec<_>>>()
            });
            seeds.leave_block(points_to_index);
            let seeded = seeded?;

            // Link the replacements of removed points back. Those that are not seeded get
            // their links when they are inserted.
            let is_seeded: BitVec = seeded.iter().map(|(_, r)| r.is_some()).collect();
            pool.install(|| {
                seeded.par_iter().try_for_each(|(local_id, replacements)| {
                    let Some(replacements) = replacements.as_ref().filter(|r| !r.is_empty()) else {
                        return Ok(());
                    };
                    check_process_stopped(stopped)?;
                    let _scope = ambient::unmeasured_guard(reason("Internal operation"));
                    let scorer = block.scorer(points_to_index[*local_id as usize])?;
                    let mut items = ItemsBuffer::default();
                    for &other in replacements {
                        if !is_seeded[other as usize] {
                            continue;
                        }
                        let mut links = block_graph.links_mut(other, 0);
                        if !links.links().contains(local_id) {
                            links.connect_with_heuristic(
                                *local_id,
                                other,
                                payload_m0,
                                |a, b| scorer.score_internal(a, b),
                                &mut items,
                            );
                        }
                    }
                    Ok::<_, OperationError>(())
                })
            })?;
            seeded
                .into_iter()
                .filter_map(|(local_id, replacements)| replacements.is_none().then_some(local_id))
                .collect()
        }
    };

    // First index points in single thread so ensure warm start for parallel indexing process.
    // Once initial structure is built, index remaining points in parallel, so that each thread
    // inserts points in different parts of the graph and is less likely to compete for locks.
    let seeded_points = points_to_index.len() - to_insert.len();
    let first_points = SINGLE_THREADED_HNSW_BUILD_THRESHOLD
        .saturating_sub(seeded_points)
        .min(to_insert.len());
    let (first, rest) = to_insert.split_at(first_points);
    for &local_id in first {
        insert_point(local_id)?;
    }
    if !rest.is_empty() {
        pool.install(|| {
            rest.par_iter()
                .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                .try_for_each(|&local_id| insert_point(local_id))
        })?;
    }

    graph_layers_builder.merge_block(block_graph, points_to_index);
    Ok(seeded_points)
}
