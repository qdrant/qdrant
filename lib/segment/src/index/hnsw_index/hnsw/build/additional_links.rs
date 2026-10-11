use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use common::ambient;
use common::bitvec::{BitSliceExt as _, BitVec};
use common::progress_tracker::ProgressTracker;
use common::reason::reason;
use common::types::{DeferredBehavior, PointOffsetType, ScoredPointOffset};
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
use crate::index::hnsw_index::graph_links::GraphLinks;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::hnsw_index::hnsw::{
    HNSW_BUILD_MAX_PAR_LEN, HNSW_USE_HEURISTIC, SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::index::hnsw_index::links_container::LinksContainer;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::Condition::Field;
use crate::types::{FieldCondition, Filter, PayloadFieldSchema, PayloadKeyType, PayloadSchemaType};
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
/// With an `old_index` (whose payload links have the same `m`), each block starts from the
/// links its points had to each other in the old graph; see [`BlockSeeds`].
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
    old_index: Option<&OldIndex<'_>>,
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

    let mut seeds =
        old_index.map(|old_index| BlockSeeds::new(old_index, payload_m, total_vector_count));
    let mut seeded_points = 0;
    let mut block_points = 0;

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

    let field_schemas = old_index
        .map(|_| payload_index.with_view(|v| v.indexed_fields()))
        .unwrap_or_default();

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

            block_points += points_to_index.len();
            seeded_points += build_block(
                id_tracker,
                vector_storage,
                quantized_vectors,
                pool,
                stopped,
                graph_layers_builder,
                &points_to_index,
                seeds.as_mut(),
                payload_m,
                config.ef_construct,
                &counter,
            )?;
            Ok(())
        };

        let old_blocks = match (old_index, field_schemas.get(&field)) {
            (Some(old_index), Some(schema)) => {
                old_range_blocks(old_index, &field, schema, payload_index, config)?
            }
            _ => None,
        };
        match old_blocks {
            Some(blocks) => {
                debug!(
                    "healing {field} with the {} block ranges of the old index",
                    blocks.len()
                );
                blocks.into_iter().try_for_each(&mut process_block)?;
            }
            None => payload_index.with_view(|v| {
                v.for_each_payload_block(&field, config.full_scan_threshold, &mut process_block)
            })?,
        }
    }
    if seeds.is_some() {
        debug!("seeded {seeded_points} of {block_points} payload block points from the old graph");
    }
    Ok(indexed_vectors_set.count_ones())
}

/// Build one payload block, linking its points only to each other, and merge it into
/// `graph_layers_builder`.
///
/// Block points are numbered `0..points_to_index.len()`, so the builder's links and visited
/// lists are sized to the block, not the segment, and a search can only reach block points.
///
/// With `seeds`, points that kept enough links to the block from the old graph are added with
/// those links, and only the rest are inserted by search. Returns the number of seeded points.
#[allow(clippy::too_many_arguments)]
fn build_block(
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
                        let seeded =
                            block_seeds.seed_point(local_id, &block, &block_graph, final_graph)?;
                        if seeded {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok::<_, OperationError>((!seeded).then_some(local_id))
                    })
                    .collect::<OperationResult<Vec<_>>>()
            });
            seeds.leave_block(points_to_index);
            seeded?.into_iter().flatten().collect()
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

/// Payload blocks of a numeric `field` with the ranges the old index used, and their
/// cardinalities in the new segment.
///
/// Numeric blocks are histogram ranges, so they move when points are added or removed, and a
/// block that straddles two old ones has few old links between its points. Blocks with the old
/// ranges can start from the old graph like keyword blocks do. Returns `None`, to compute new
/// blocks, if the field is not numeric in both indexes, if the threshold changed, if the old
/// index had no blocks for it, or if a range grew to more than twice its old size.
fn old_range_blocks(
    old_index: &OldIndex<'_>,
    field: &PayloadKeyType,
    schema: &PayloadFieldSchema,
    payload_index: &StructPayloadIndex,
    config: &HnswGraphConfig,
) -> OperationResult<Option<Vec<PayloadBlockCondition>>> {
    let kind = schema.kind();
    if !matches!(
        kind,
        PayloadSchemaType::Integer | PayloadSchemaType::Float | PayloadSchemaType::Datetime
    ) || old_index.index.config.full_scan_threshold != config.full_scan_threshold
    {
        return Ok(None);
    }
    let old_payload_index = old_index.index.payload_index.borrow();
    let old_kind = old_payload_index.with_view(|v| v.indexed_fields().get(field).map(|s| s.kind()));
    if old_kind != Some(kind) {
        return Ok(None);
    }
    let mut blocks = Vec::new();
    old_payload_index.with_view(|v| {
        v.for_each_payload_block(field, config.full_scan_threshold, &mut |block| {
            blocks.push(block);
            Ok(())
        })
    })?;
    drop(old_payload_index);
    if blocks.is_empty() {
        return Ok(None);
    }

    let _scope = ambient::unmeasured_guard(reason("Internal operation"));
    for block in &mut blocks {
        let filter = Filter::new_must(Field(block.condition.clone()));
        let cardinality = payload_index
            .with_view(|v| v.estimate_cardinality(&filter))?
            .exp;
        if cardinality > 2 * block.cardinality.max(config.full_scan_threshold) {
            return Ok(None);
        }
        block.cardinality = cardinality;
    }
    Ok(Some(blocks))
}

/// Starts payload blocks from the graph of the index being healed.
///
/// The old graph keeps a point's main graph links and payload links in one list. Restricted to
/// the points of a block, that list holds the old links between block points: the payload links
/// of a similar old block, if there was one, and the main graph links that fall into the block.
/// A point removed since the old build is replaced by its link closest to the point, like the
/// main graph healer links through removed points. Taking all its links instead fills the lists,
/// and the backlinks of inserted points then prune them.
///
/// A point is seeded if this leaves it enough candidates, and keeps them as its links, except
/// for those it already has in the graph being built (main graph links, links of blocks merged
/// before), which the merge would drop anyway. Each of those takes one place of the
/// `payload_m0` the list may hold, so a seeded block adds as many links as the old one did,
/// and the free places take backlinks of the points inserted later. Re-running the heuristic on
/// the old links instead costs recall: the old lists hold backlinks, which a build adds without
/// the heuristic until a list is full. Only a point with more candidates than places gets the
/// heuristic's pick, filled up with the closest of the rest.
///
/// A point without enough candidates, e.g. if the old build had no block like this one, is
/// inserted by search.
struct BlockSeeds<'a> {
    old_links: &'a GraphLinks,
    old_to_new: &'a [Option<PointOffsetType>],
    new_to_old: &'a [Option<PointOffsetType>],
    /// Level 0 `m` of the old main graph, or of the payload links if it had none: the number of
    /// old links [`Self::min_candidates`] is relative to.
    old_m0: usize,
    /// Position of each segment point in the current block, [`NOT_IN_BLOCK`] for the others.
    to_local: Vec<PointOffsetType>,
    payload_m0: usize,
}

const NOT_IN_BLOCK: PointOffsetType = PointOffsetType::MAX;

impl<'a> BlockSeeds<'a> {
    fn new(old_index: &'a OldIndex<'_>, payload_m: HnswM, total_vector_count: usize) -> Self {
        let old_graph = old_index.graph();
        let old_m0 = match old_graph.hnsw_m.m0 {
            0 => payload_m.m0,
            m0 => m0,
        };
        Self {
            old_links: &old_graph.links,
            old_to_new: &old_index.old_to_new,
            new_to_old: &old_index.new_to_old,
            old_m0,
            to_local: vec![NOT_IN_BLOCK; total_vector_count],
            payload_m0: payload_m.m0,
        }
    }

    fn enter_block(&mut self, points: &[PointOffsetType]) {
        for (local_id, &point_id) in points.iter().enumerate() {
            self.to_local[point_id as usize] = local_id as PointOffsetType;
        }
    }

    fn leave_block(&mut self, points: &[PointOffsetType]) {
        for &point_id in points {
            self.to_local[point_id as usize] = NOT_IN_BLOCK;
        }
    }

    /// Block id of segment point `new_id`, if it is a block point other than `local_id` and has
    /// a vector.
    fn block_point(
        &self,
        new_id: PointOffsetType,
        local_id: PointOffsetType,
        block_deleted: &BitVec,
    ) -> Option<PointOffsetType> {
        let block_id = self.to_local[new_id as usize];
        (block_id != NOT_IN_BLOCK && block_id != local_id && !block_deleted[block_id as usize])
            .then_some(block_id)
    }

    /// Fewest candidates a point needs to be seeded: half of its old main graph links (or of
    /// its links, without a main graph).
    ///
    /// A block the old build had too gives about as many candidates as the point had links,
    /// while main graph links alone give a fraction proportional to the block size, which
    /// [`build_additional_links`] keeps below a few links.
    fn min_candidates(&self, old_links_count: usize) -> usize {
        old_links_count.min(self.old_m0).div_ceil(2).max(1)
    }

    /// Add block point `local_id` to `block_graph` with its links from the old graph, if it
    /// has enough of them. `final_graph` is the graph the block will be merged into. Returns
    /// whether it did.
    fn seed_point(
        &self,
        local_id: PointOffsetType,
        block: &BlockContext,
        block_graph: &GraphLayersBuilder,
        final_graph: &GraphLayersBuilder,
    ) -> OperationResult<bool> {
        let block_deleted = block.deleted;
        if block_deleted[local_id as usize] {
            return Ok(false);
        }
        let point_id = block.points[local_id as usize];
        let Some(old_id) = self.new_to_old[point_id as usize] else {
            return Ok(false);
        };

        // Old links to points of the block, and points removed since the old build.
        let mut candidates = Vec::new();
        let mut removed = Vec::new();
        let mut old_links_count = 0;
        for old_link in self.old_links.links(old_id, 0) {
            old_links_count += 1;
            match self.old_to_new[old_link as usize] {
                Some(new_link) => {
                    if let Some(candidate) = self.block_point(new_link, local_id, block_deleted)
                        && !candidates.contains(&candidate)
                    {
                        candidates.push(candidate);
                    }
                }
                None => removed.push(old_link),
            }
        }

        // Replace each removed point with its link that is closest to this point.
        if !removed.is_empty() {
            let mut scorer = block.scorer(point_id)?;
            let mut options = Vec::new();
            for old_link in removed {
                options.clear();
                for link in self.old_links.links(old_link, 0) {
                    if let Some(new_link) = self.old_to_new[link as usize]
                        && let Some(option) = self.block_point(new_link, local_id, block_deleted)
                        && !candidates.contains(&option)
                        && !options.contains(&option)
                    {
                        options.push(option);
                    }
                }
                let closest = scorer
                    .score_points_unfiltered(&options)?
                    .max_by(|a, b| a.score.total_cmp(&b.score));
                if let Some(closest) = closest {
                    candidates.push(closest.idx);
                }
            }
        }

        if candidates.len() < self.min_candidates(old_links_count) {
            return Ok(false);
        }

        let all_candidates = candidates.len();
        {
            let existing_links = final_graph.links(point_id, 0);
            candidates.retain(|&c| !existing_links.links().contains(&block.points[c as usize]));
        }
        let places = self
            .payload_m0
            .saturating_sub(all_candidates - candidates.len());

        if candidates.len() > places {
            candidates = select_links(places, &candidates, point_id, block)?;
        }
        block_graph.add_new_point(local_id, vec![candidates]);
        Ok(true)
    }
}

/// A payload block being built: its points, numbered by position, and what scores them.
struct BlockContext<'a> {
    /// Segment ids of the block points.
    points: &'a [PointOffsetType],
    /// One flag per block point.
    deleted: &'a BitVec,
    vector_storage: &'a VectorStorageEnum,
    quantized_vectors: &'a Option<QuantizedVectors>,
}

impl<'a> BlockContext<'a> {
    /// Scorer against segment point `point_id`, taking block point ids.
    fn scorer(&self, point_id: PointOffsetType) -> OperationResult<FilteredScorer<'a>> {
        FilteredScorer::new_block_scorer(
            point_id,
            self.points,
            self.vector_storage,
            self.quantized_vectors.as_ref(),
            self.deleted,
        )
    }
}

/// Pick `count` of block point candidates for segment point `point_id`: those the heuristic
/// selects, then the closest of the rest.
fn select_links(
    count: usize,
    candidates: &[PointOffsetType],
    point_id: PointOffsetType,
    block: &BlockContext,
) -> OperationResult<Vec<PointOffsetType>> {
    let mut scorer = block.scorer(point_id)?;
    let mut scored: Vec<ScoredPointOffset> = scorer.score_points_unfiltered(candidates)?.collect();
    scored.sort_unstable_by(|a, b| b.cmp(a));

    let mut container = LinksContainer::with_capacity(count);
    container.fill_from_sorted_with_heuristic(scored.iter().copied(), count, |a, b| {
        scorer.score_internal(a, b)
    });
    let mut links = container.into_vec();
    for candidate in &scored {
        if links.len() >= count {
            break;
        }
        if !links.contains(&candidate.idx) {
            links.push(candidate.idx);
        }
    }
    Ok(links)
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
