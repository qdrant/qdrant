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

    // One builder serves every block. It is sized for the whole segment, so allocating it
    // per block, and merging it back by scanning every point, would cost O(segment) on a
    // single thread for each block. `merge_block_from` moves out only the block's links and
    // leaves the builder empty for the next block.
    let mut block_graph: Option<GraphLayersBuilder> = None;

    // On CPU, blocks are built in their own id space instead (see `build_block_compact`).
    // The GPU builder needs a segment-sized builder, so it keeps the path above.
    let compact_blocks = gpu_insert_context.is_none() && !force_segment_wide_blocks();

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

            if compact_blocks {
                build_block_compact(
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
                )?;
                if !indexed_vectors_set.is_empty() {
                    for &point_id in &points_to_index {
                        indexed_vectors_set.set(point_id as usize, true);
                    }
                }
                return Ok(());
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

            let gpu_constructed_graph = build_filtered_graph(
                id_tracker,
                vector_storage,
                quantized_vectors,
                gpu_insert_context,
                payload_index,
                pool,
                stopped,
                additional_graph,
                &points_to_index,
                &mut block_filter_list,
                &mut indexed_vectors_set,
                &counter,
            )?;
            match gpu_constructed_graph {
                Some(gpu_graph) => graph_layers_builder.merge_from_other(gpu_graph),
                None => graph_layers_builder.merge_block_from(additional_graph, &points_to_index),
            }
            Ok(())
        };

        payload_index.with_view(|v| {
            v.for_each_payload_block(&field, config.full_scan_threshold, &mut process_block)
        })?;
    }
    Ok(indexed_vectors_set.count_ones())
}

/// Build one payload block in a builder sized to the block, and merge it into
/// `graph_layers_builder`.
///
/// Block points are numbered `0..points_to_index.len()`, so the builder's links and visited
/// lists are sized to the block, not the segment, and a search can only reach block points,
/// which makes the per-candidate block filter unnecessary. The graph is the one
/// [`build_filtered_graph`] builds into a segment-sized builder.
#[allow(clippy::too_many_arguments)]
fn build_block_compact(
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
        let _hw = hw::unmeasured_guard(reason("Internal operation"));
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

    // Same insertion order and parallelism as `build_filtered_graph`.
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

#[cfg(not(test))]
fn force_segment_wide_blocks() -> bool {
    false
}

#[cfg(test)]
thread_local! {
    /// Tests: build blocks with the segment-wide path, to compare it with the compact one.
    static FORCE_SEGMENT_WIDE_BLOCKS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn force_segment_wide_blocks() -> bool {
    FORCE_SEGMENT_WIDE_BLOCKS.get()
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

/// Insert `points_to_index` into `graph_layers_builder`, linking them only to each other.
///
/// Returns the graph instead when it was built on the GPU; `graph_layers_builder` is then
/// left untouched.
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
    graph_layers_builder: &GraphLayersBuilder,
    points_to_index: &[PointOffsetType],
    block_filter_list: &mut VisitedListHandle,
    indexed_vectors_set: &mut BitVec,
    counter: &AtomicU64,
) -> OperationResult<Option<GraphLayersBuilder>> {
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
            points_to_index,
            stopped,
        )?
    {
        return Ok(Some(gpu_constructed_graph));
    }

    let insert_points = |block_point_id| {
        check_process_stopped(stopped)?;

        let _scope = ambient::unmeasured_guard(reason(
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
            points_to_index[first_points..]
                .par_iter()
                .copied()
                .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                .try_for_each(insert_points)
        })?;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    // Config structs keep their deprecated placement fields until 2.0.
    #![allow(deprecated)]

    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use common::budget::ResourcePermit;
    use common::flags::FeatureFlags;
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
    use crate::entry::entry_point::SegmentEntry;
    use crate::fixtures::index_fixtures::random_vector;
    use crate::id_tracker::IdTracker;
    use crate::index::PayloadIndex;
    use crate::index::hnsw_index::graph::HnswGraph;
    use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
    use crate::payload_json;
    use crate::segment::Segment;
    use crate::segment_constructor::VectorIndexBuildArgs;
    use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
    use crate::types::{
        Distance, HnswConfig, HnswGlobalConfig, PayloadSchemaType, QuantizationConfig,
        SeqNumberType, TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization,
    };
    use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsStorageType;

    const DIM: usize = 8;
    const NUM_POINTS: u64 = 2_000;
    /// 8 values of 250 points each: every value makes a block.
    const POINTS_PER_VALUE: u64 = 250;

    #[derive(Clone, Copy, Debug)]
    enum Field {
        Keyword,
        /// Overlapping range blocks.
        Integer,
        /// Every 9th point dropped from the id tracker while its vector stays.
        KeywordWithStalePoints,
    }

    fn build_segment(dir: &std::path::Path, field: Field) -> Segment {
        let mut rng = StdRng::seed_from_u64(42);
        let mut segment = build_simple_segment(dir, DIM, Distance::Cosine).unwrap();
        for n in 0..NUM_POINTS {
            let value = n / POINTS_PER_VALUE;
            let payload = match field {
                Field::Integer => payload_json! {"field": value as i64},
                Field::Keyword | Field::KeywordWithStalePoints => {
                    payload_json! {"field": format!("value-{value}")}
                }
            };
            let vector = random_vector(&mut rng, DIM);
            let op = n as SeqNumberType;
            segment
                .upsert_point(op, n.into(), only_default_vector(&vector))
                .unwrap();
            segment.set_full_payload(op, n.into(), &payload).unwrap();
        }
        let schema = match field {
            Field::Integer => PayloadSchemaType::Integer,
            Field::Keyword | Field::KeywordWithStalePoints => PayloadSchemaType::Keyword,
        };
        segment
            .payload_index
            .borrow_mut()
            .set_indexed(&JsonPath::new("field"), schema)
            .unwrap();
        if let Field::KeywordWithStalePoints = field {
            let mut id_tracker = segment.id_tracker.borrow_mut();
            for n in (0..NUM_POINTS).step_by(9) {
                id_tracker.drop(n.into()).unwrap();
            }
        }
        segment
    }

    /// Build the index with one thread and return every point's sorted links per level.
    fn build_links(
        segment: &Segment,
        segment_wide: bool,
        quantize: bool,
        payload_m: usize,
    ) -> Vec<Vec<Vec<PointOffsetType>>> {
        let stopped = AtomicBool::new(false);
        let dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
        let storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
        let quantized = quantize.then(|| {
            let config = QuantizationConfig::Turbo(TurboQuantization {
                turbo: TurboQuantQuantizationConfig {
                    always_ram: None,
                    memory: None,
                    bits: Some(TurboQuantBitSize::Bits4),
                },
            });
            let storage_type = QuantizedVectorsStorageType::Immutable;
            QuantizedVectors::create(
                &storage.borrow(),
                &config,
                storage_type,
                dir.path(),
                1,
                &stopped,
            )
            .unwrap()
        });
        let hnsw_config = HnswConfig {
            m: 8,
            ef_construct: 16,
            // KB: far below a value's 250 points, so every value makes a block.
            full_scan_threshold: 1,
            max_indexing_threads: 1,
            on_disk: Some(false),
            memory: None,
            payload_m: Some(payload_m),
            inline_storage: None,
        };

        FORCE_SEGMENT_WIDE_BLOCKS.set(segment_wide);
        let index = HNSWIndex::build(
            HnswIndexOpenArgs {
                path: dir.path(),
                id_tracker: segment.id_tracker.clone(),
                vector_storage: storage.clone(),
                quantized_vectors: Arc::new(AtomicRefCell::new(quantized)),
                payload_index: segment.payload_index.clone(),
                hnsw_config,
            },
            VectorIndexBuildArgs {
                permit: Arc::new(ResourcePermit::dummy(1)),
                old_indices: &[],
                gpu_device: None,
                rng: &mut StdRng::seed_from_u64(42),
                stopped: &stopped,
                hnsw_global_config: &HnswGlobalConfig::default(),
                feature_flags: FeatureFlags::default(),
                inline_vectors: false,
                progress: ProgressTracker::new_for_test(),
            },
        )
        .unwrap();
        FORCE_SEGMENT_WIDE_BLOCKS.set(false);

        let HnswGraph::Direct(graph) = &index.graph else {
            panic!("a freshly built index is direct");
        };
        (0..graph.links.num_points() as PointOffsetType)
            .map(|point_id| {
                (0..=graph.links.point_level(point_id))
                    .map(|level| {
                        let mut links: Vec<_> = graph.links.links(point_id, level).collect();
                        links.sort_unstable();
                        links
                    })
                    .collect()
            })
            .collect()
    }

    /// With one build thread both paths are deterministic, so the compact path must produce
    /// exactly the links of the segment-wide path.
    #[rstest]
    #[case::keyword(Field::Keyword, false)]
    #[case::integer(Field::Integer, false)]
    #[case::stale_points(Field::KeywordWithStalePoints, false)]
    #[case::keyword_quantized(Field::Keyword, true)]
    #[case::integer_quantized(Field::Integer, true)]
    fn test_compact_blocks_match_segment_wide(#[case] field: Field, #[case] quantize: bool) {
        let _hw = hw::test_guard();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment = build_segment(dir.path(), field);

        let segment_wide = build_links(&segment, true, quantize, 8);
        let compact = build_links(&segment, false, quantize, 8);
        // Without blocks both builds would match trivially.
        let no_blocks = build_links(&segment, false, quantize, 0);
        assert_ne!(compact, no_blocks, "no payload blocks were built");

        for (point_id, (expected, actual)) in segment_wide.iter().zip(&compact).enumerate() {
            assert_eq!(expected, actual, "links of point {point_id} differ");
        }
        assert_eq!(segment_wide.len(), compact.len());
    }
}
