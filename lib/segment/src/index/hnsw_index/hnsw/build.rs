use std::cmp::Reverse;
use std::ops::Deref as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;

use common::bitvec::{BitSlice, BitSliceExt as _, BitVec};
use common::condition_checker::ConditionChecker as _;
use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(target_os = "linux")]
use common::cpu::linux_low_thread_priority;
use common::progress_tracker::ProgressTracker;
use common::types::{DeferredBehavior, PointOffsetType};
use fs_err as fs;
use log::{debug, trace};
use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use rayon::ThreadPool;
use rayon::prelude::*;

use super::old_index::OldIndexCandidate;
use super::telemetry::HNSWSearchesTelemetry;
use super::{
    FINISH_MAIN_GRAPH_LOG_MESSAGE, HNSW_USE_HEURISTIC, HNSWIndex, HnswIndexOpenArgs,
    SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
};
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::PayloadIndexRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::PayloadBlockCondition;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
#[cfg(feature = "gpu")]
use crate::index::hnsw_index::gpu::get_gpu_groups_count;
#[cfg(feature = "gpu")]
use crate::index::hnsw_index::gpu::gpu_graph_builder::GPU_MAX_VISITED_FLAGS_FACTOR;
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_layers_healer::GraphLayersHealer;
use crate::index::hnsw_index::graph_links::{GraphLinksFormatParam, StorageGraphLinksVectors};
use crate::index::hnsw_index::point_scorer::{
    BlockVectors, FilteredScorer, MAX_BLOCK_GATHER_BYTES, MIN_BLOCK_GATHER_BYTES,
};
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};
use crate::json_path::JsonPath;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::types::Condition::Field;
use crate::types::{FieldCondition, Filter};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{NotDeletedChecker, VectorStorageEnum, VectorStorageRead};

/// Blocks below this cardinality are cheap enough that giving each one a whole
/// thread beats splitting every one of them across the pool, whose serial
/// insertion prefix does not amortize on a short run.
const DEFAULT_LARGE_BLOCK_THRESHOLD: usize = 32_768;

/// Knobs that let tests reach into the payload-block stage of the build.
#[derive(Default)]
pub(crate) struct HnswBuildDebugOptions<'a> {
    /// Build payload block subgraphs with the legacy segment-sized builder,
    /// so its output can be compared against the compact one.
    pub force_legacy_payload_blocks: bool,
    /// Incremented once per payload block that is actually built.
    pub blocks_built: Option<&'a AtomicUsize>,
    /// Incremented once per payload block the connectivity shortcut skips.
    pub blocks_skipped_by_connectivity: Option<&'a AtomicUsize>,
    /// Incremented once per block routed through the large-block path, which
    /// spreads a single block's insertion run over the whole pool.
    pub large_blocks_built: Option<&'a AtomicUsize>,
    /// Incremented once per built block whose vectors were copied into a
    /// block-local buffer for scoring.
    pub blocks_gathered: Option<&'a AtomicUsize>,
    /// Incremented once per block handed over by the unified cross-field queue,
    /// and never by the legacy per-field path.
    ///
    /// Without it the queue's tests would pass just as well against a build that
    /// silently fell back to the legacy path, since the two agree on everything
    /// else those tests can see.
    pub blocks_via_queue: Option<&'a AtomicUsize>,
    /// Collects the block index each block is filtered under, in call order.
    /// Both build paths have to number a field's blocks the same way.
    pub block_indices: Option<&'a Mutex<Vec<usize>>>,
    /// Overrides [`DEFAULT_LARGE_BLOCK_THRESHOLD`], which is far above any
    /// cardinality a test fixture can reach.
    pub large_block_threshold: Option<usize>,
    /// Called with the finished graph, before it is serialized.
    pub inspect_builder: Option<&'a dyn Fn(&GraphLayersBuilder)>,
}

impl HNSWIndex {
    pub fn build<R: Rng + ?Sized>(
        open_args: HnswIndexOpenArgs<'_>,
        build_args: VectorIndexBuildArgs<'_, R>,
    ) -> OperationResult<Self> {
        Self::build_with_debug_options(open_args, build_args, HnswBuildDebugOptions::default())
    }

    pub(crate) fn build_with_debug_options<R: Rng + ?Sized>(
        open_args: HnswIndexOpenArgs<'_>,
        build_args: VectorIndexBuildArgs<'_, R>,
        debug_options: HnswBuildDebugOptions<'_>,
    ) -> OperationResult<Self> {
        if HnswGraphConfig::get_config_path(open_args.path).exists()
            || GraphLayers::get_path(open_args.path).exists()
        {
            log::warn!(
                "HNSW index already exists at {:?}, skipping building",
                open_args.path
            );
            debug_assert!(false);
            return Self::open(open_args);
        }

        let HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            hnsw_config,
        } = open_args;
        let VectorIndexBuildArgs {
            permit,
            old_indices,
            gpu_device,
            rng,
            stopped,
            hnsw_global_config,
            feature_flags,
            progress,
        } = build_args;

        fs::create_dir_all(path)?;

        let id_tracker_ref = id_tracker.borrow();
        let vector_storage_ref = vector_storage.borrow();
        let quantized_vectors_ref = quantized_vectors.borrow();
        let payload_index_ref = payload_index.borrow();

        let total_vector_count = vector_storage_ref.total_vector_count();

        let full_scan_threshold = vector_storage_ref
            .size_of_available_vectors_in_bytes()
            .checked_div(total_vector_count)
            .and_then(|avg_vector_size| {
                hnsw_config
                    .full_scan_threshold
                    .saturating_mul(BYTES_IN_KB)
                    .checked_div(avg_vector_size)
            })
            .unwrap_or(1);

        let mut config = HnswGraphConfig::new(
            hnsw_config.m,
            hnsw_config.ef_construct,
            full_scan_threshold,
            hnsw_config.max_indexing_threads,
            hnsw_config.payload_m,
            total_vector_count,
        );

        #[allow(unused_mut)]
        let mut build_main_graph = config.m > 0;
        if !build_main_graph {
            debug!("skip building main HNSW graph");
        }

        let payload_m = HnswM::new(
            config.payload_m.unwrap_or(config.m),
            config.payload_m0.unwrap_or(config.m0),
        );

        // Progress subtasks
        let progress_migrate = build_main_graph.then(|| progress.subtask("migrate"));
        let progress_main_graph = build_main_graph.then(|| progress.subtask("main_graph"));
        let additional_links_params: Option<(ProgressTracker, Vec<(ProgressTracker, JsonPath)>)> =
            (payload_m.m > 0)
                .then(|| payload_index_ref.with_view(|v| v.indexed_fields()))
                .filter(|fields| !fields.is_empty())
                .map(|fields| {
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
                    (progress_additional_links, fields)
                });

        let old_index = old_indices
            .iter()
            .filter_map(|old_index| {
                OldIndexCandidate::evaluate(
                    &feature_flags,
                    old_index,
                    &config,
                    hnsw_global_config,
                    &vector_storage_ref,
                    &quantized_vectors_ref,
                    id_tracker_ref.deref(),
                )
            })
            .max_by_key(|old_index| old_index.valid_points);

        // Build main index graph
        let deleted_bitslice = vector_storage_ref.deleted_vector_bitslice();

        #[cfg(feature = "gpu")]
        let gpu_name_postfix = if let Some(gpu_device) = gpu_device {
            format!(" and GPU {}", gpu_device.device().name())
        } else {
            Default::default()
        };
        #[cfg(not(feature = "gpu"))]
        let gpu_name_postfix = "";
        #[cfg(not(feature = "gpu"))]
        let _ = gpu_device;
        debug!(
            "building HNSW for {total_vector_count} vectors with {} CPUs{gpu_name_postfix}",
            permit.num_cpus,
        );

        let num_entries = std::cmp::max(
            1,
            total_vector_count
                .checked_div(full_scan_threshold)
                .unwrap_or(0)
                * 10,
        );
        let mut graph_layers_builder = GraphLayersBuilder::new(
            total_vector_count,
            HnswM::new(config.m, config.m0),
            config.ef_construct,
            num_entries,
            HNSW_USE_HEURISTIC,
        );

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(permit.num_cpus as usize)
            .spawn_handler(|thread| {
                let mut b = thread::Builder::new();
                if let Some(name) = thread.name() {
                    b = b.name(name.to_owned());
                }
                if let Some(stack_size) = thread.stack_size() {
                    b = b.stack_size(stack_size);
                }
                b.spawn(|| {
                    // On Linux, use lower thread priority so we interfere less with serving traffic
                    #[cfg(target_os = "linux")]
                    if let Err(err) = linux_low_thread_priority() {
                        log::debug!(
                            "Failed to set low thread priority for HNSW building, ignoring: {err}"
                        );
                    }

                    thread.run()
                })?;
                Ok(())
            })
            .build()?;

        let old_index = old_index.map(|old_index| old_index.reuse(total_vector_count));

        let mut indexed_vectors = 0;
        for vector_id in id_tracker_ref
            .point_mappings()
            .iter_internal_excluding(deleted_bitslice)
        {
            check_process_stopped(stopped)?;
            indexed_vectors += 1;

            let level = old_index
                .as_ref()
                .and_then(|old_index| old_index.point_level(vector_id))
                .unwrap_or_else(|| graph_layers_builder.get_random_layer(rng));
            graph_layers_builder.set_levels(vector_id, level);
        }

        // Try to build graphs on GPU if possible.
        // Store created gpu vectors to reuse them for payload links.
        #[cfg(feature = "gpu")]
        let needs_gpu_vectors = build_main_graph
            || additional_links_params
                .as_ref()
                .is_some_and(|(_, indexed_fields)| !indexed_fields.is_empty());

        #[cfg(feature = "gpu")]
        let gpu_vectors = if needs_gpu_vectors {
            let timer = std::time::Instant::now();
            let gpu_vectors = super::gpu_build::create_gpu_vectors(
                gpu_device,
                &vector_storage_ref,
                &quantized_vectors_ref,
                stopped,
            )?;
            if build_main_graph
                && let Some(gpu_constructed_graph) = super::gpu_build::build_main_graph_on_gpu(
                    id_tracker_ref.deref(),
                    &vector_storage_ref,
                    &quantized_vectors_ref,
                    gpu_vectors.as_ref(),
                    &graph_layers_builder,
                    deleted_bitslice,
                    num_entries,
                    stopped,
                )?
            {
                graph_layers_builder = gpu_constructed_graph;
                build_main_graph = false;
                debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
            }
            gpu_vectors
        } else {
            None
        };

        if build_main_graph {
            let progress_main_graph = progress_main_graph.unwrap();
            let progress_migrate = progress_migrate.unwrap();

            let mut ids = Vec::with_capacity(total_vector_count);
            let mut first_few_ids = Vec::with_capacity(SINGLE_THREADED_HNSW_BUILD_THRESHOLD);

            let mut ids_iter = id_tracker_ref
                .point_mappings()
                .iter_internal_excluding(deleted_bitslice);
            if let Some(old_index) = old_index {
                progress_migrate.start();

                let timer = std::time::Instant::now();

                let mut healer = GraphLayersHealer::new(
                    old_index.graph(),
                    &old_index.old_to_new,
                    config.ef_construct,
                );
                let old_vector_storage = old_index.index.vector_storage.borrow();
                let old_quantized_vectors = old_index.index.quantized_vectors.borrow();
                healer.heal(&pool, &old_vector_storage, old_quantized_vectors.as_ref())?;
                healer.save_into_builder(&graph_layers_builder);

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
            let counter = progress_main_graph
                .track_progress(Some(first_few_ids.len() as u64 + ids.len() as u64));
            let counter = counter.deref();

            let insert_point = |vector_id| {
                check_process_stopped(stopped)?;
                // No need to accumulate hardware, since this is an internal operation
                let internal_hardware_counter = HardwareCounterCell::disposable();

                let points_scorer = FilteredScorer::new_internal(
                    vector_id,
                    vector_storage_ref.deref(),
                    quantized_vectors_ref.as_ref(),
                    None,
                    id_tracker_ref.deleted_point_bitslice(),
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
                pool.install(|| ids.into_par_iter().try_for_each(insert_point))?;
            }

            drop(progress_main_graph);
            debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
        } else {
            drop(old_index);
        }

        if let Some((progress_additional_links, indexed_fields)) = additional_links_params {
            progress_additional_links.start();

            // Calculate true average number of links per vertex in the HNSW graph
            // to better estimate percolation threshold
            let average_links_per_0_level =
                graph_layers_builder.get_average_connectivity_on_level(0);
            let average_links_per_0_level_int = (average_links_per_0_level as usize).max(1);

            // Estimate connectivity of the main graph
            let all_points = id_tracker_ref
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
                // Each sample walks the whole segment, so run them concurrently.
                // Seeds are drawn up front to keep the parent RNG stream
                // independent of the scheduling order.
                let seeds: [u64; 3] = std::array::from_fn(|_| rng.random());
                let global_graph_connectivity = pool.install(|| {
                    seeds
                        .into_par_iter()
                        .map(|seed| {
                            let mut rng = SmallRng::seed_from_u64(seed);
                            graph_layers_builder.subgraph_connectivity(
                                &mut rng,
                                &all_points,
                                percolation,
                            )
                        })
                        .collect::<Vec<_>>()
                });

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

            #[cfg(feature = "gpu")]
            let mut gpu_insert_context = if let Some(gpu_vectors) = gpu_vectors.as_ref() {
                Some(GpuInsertContext::new(
                    gpu_vectors,
                    get_gpu_groups_count(),
                    payload_m,
                    config.ef_construct,
                    false,
                    1..=GPU_MAX_VISITED_FLAGS_FACTOR,
                )?)
            } else {
                None
            };
            #[cfg(not(feature = "gpu"))]
            let mut gpu_insert_context = None;

            // The compact path builds each block in its own id space, so it
            // does not pay for a segment-sized builder per block. It cannot
            // serve the two callers that need segment-wide ids: `m == 0` counts
            // indexed vectors into a segment-sized bitset, and the GPU builder
            // is welded to a segment-sized `GraphLayersBuilder`.
            let use_compact_blocks = config.m > 0
                && gpu_insert_context.is_none()
                && !debug_options.force_legacy_payload_blocks;

            let visited_pool = VisitedPool::new();
            let mut block_filter_list =
                (!use_compact_blocks).then(|| visited_pool.get(total_vector_count));

            let large_block_threshold = debug_options
                .large_block_threshold
                .unwrap_or(DEFAULT_LARGE_BLOCK_THRESHOLD);

            // Block-level parallelism needs threads to spare, and keeping the
            // single-threaded build sequential keeps it reproducible.
            let parallel_blocks = use_compact_blocks && pool.current_num_threads() > 1;

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

            // The cap is per block and the small-block drain runs one block per
            // thread, so without dividing it out the transient total would be
            // the pool size times the cap.
            let gather_budget = if parallel_blocks {
                (MAX_BLOCK_GATHER_BYTES / pool.current_num_threads().max(1))
                    .max(MIN_BLOCK_GATHER_BYTES)
            } else {
                MAX_BLOCK_GATHER_BYTES
            };
            let gather_declined = AtomicUsize::new(0);

            if use_compact_blocks {
                let block_builder = BlockBuilder {
                    vector_storage: &vector_storage_ref,
                    quantized_vectors: &quantized_vectors_ref,
                    graph: &graph_layers_builder,
                    vec_deleted: deleted_bitslice,
                    point_deleted: id_tracker_ref.deleted_point_bitslice(),
                    payload_m,
                    ef_construct: config.ef_construct,
                    gather_budget,
                    blocks_gathered: debug_options.blocks_gathered,
                    gather_declined: &gather_declined,
                    stopped,
                };

                // The witness that the queue actually ran: the legacy per-field
                // path never touches it.
                let count_queued = || {
                    if let Some(blocks_via_queue) = debug_options.blocks_via_queue {
                        blocks_via_queue.fetch_add(1, Ordering::Relaxed);
                    }
                };

                // Per-field state a block needs, in the field order below.
                let mut field_filters: Vec<(BlockFilter, Arc<AtomicU64>)> = Vec::new();
                // `(field position, block index within that field, block)`.
                let mut block_queue: Vec<(usize, usize, PayloadBlockCondition)> = Vec::new();

                // Every field's blocks go into one queue, so the parallel run
                // drains once for the whole segment instead of once per field.
                for (index_pos, (field_progress, field)) in indexed_fields.iter().enumerate() {
                    field_progress.start();

                    // Drawn per field: one base shared across fields would give
                    // field `i`'s block `j` and field `k`'s block `j` the same
                    // seed, correlating their connectivity estimates. Drawn up
                    // front so the seeds do not depend on scheduling order.
                    let block_seed_base: u64 = rng.random();

                    debug!("building additional index for field {field}");

                    let is_tenant = payload_index_ref.is_tenant(field);
                    let counter = field_progress.track_progress(None);

                    let before = block_queue.len();
                    payload_index_ref.with_view(|v| {
                        v.for_each_payload_block(field, full_scan_threshold, &mut |block| {
                            block_queue.push((index_pos, block_queue.len() - before, block));
                            Ok(())
                        })
                    })?;

                    field_filters.push((
                        BlockFilter {
                            field,
                            payload_index: &payload_index_ref,
                            vector_storage: &vector_storage_ref,
                            stopped,
                            full_scan_threshold,
                            max_block_size,
                            check_connectivity: !is_tenant && index_pos > 0,
                            required_connectivity,
                            percolation,
                            block_seed_base,
                            blocks_built: debug_options.blocks_built,
                            blocks_skipped_by_connectivity: debug_options
                                .blocks_skipped_by_connectivity,
                            block_indices: debug_options.block_indices,
                        },
                        counter,
                    ));
                }

                let build_queued = |(field_pos, block_index, block): (
                    usize,
                    usize,
                    PayloadBlockCondition,
                )|
                 -> OperationResult<()> {
                    count_queued();
                    let (filter, counter) = &field_filters[field_pos];
                    block_builder.build(filter, block_index, block, counter, None)
                };

                if parallel_blocks {
                    let (mut small, large): (Vec<_>, Vec<_>) = block_queue
                        .into_iter()
                        .partition(|(_, _, block)| block.cardinality < large_block_threshold);

                    // One large block at a time, spread over the whole pool.
                    for (field_pos, block_index, block) in large {
                        if let Some(large_blocks_built) = debug_options.large_blocks_built {
                            large_blocks_built.fetch_add(1, Ordering::Relaxed);
                        }
                        count_queued();
                        let (filter, counter) = &field_filters[field_pos];
                        block_builder.build(filter, block_index, block, counter, Some(&pool))?;
                    }

                    // Longest first, so the run does not end up waiting on a
                    // straggler that happened to be scheduled last.
                    small.sort_unstable_by_key(|(_, _, block)| Reverse(block.cardinality));
                    pool.install(|| small.into_par_iter().try_for_each(build_queued))?;
                } else {
                    // Same order the legacy per-field path uses, since the queue
                    // is fields in order and blocks in generation order.
                    for (field_pos, block_index, block) in block_queue {
                        count_queued();
                        let (filter, counter) = &field_filters[field_pos];
                        block_builder.build(filter, block_index, block, counter, Some(&pool))?;
                    }
                }

                drop(field_filters);
            } else {
                for (index_pos, (field_progress, field)) in indexed_fields.iter().enumerate() {
                    field_progress.start();

                    let block_seed_base: u64 = rng.random();

                    debug!("building additional index for field {field}");

                    let is_tenant = payload_index_ref.is_tenant(field);

                    let counter = field_progress.track_progress(None);
                    let counter = counter.deref();

                    // Numbered the same way the compact path numbers the blocks
                    // it collects, so for a given block both paths seed the
                    // connectivity shortcut with the same index and reach the
                    // same skip decision.
                    let block_filter = BlockFilter {
                        field,
                        payload_index: &payload_index_ref,
                        vector_storage: &vector_storage_ref,
                        stopped,
                        full_scan_threshold,
                        max_block_size,
                        check_connectivity: !is_tenant && index_pos > 0,
                        required_connectivity,
                        percolation,
                        block_seed_base,
                        blocks_built: debug_options.blocks_built,
                        blocks_skipped_by_connectivity: debug_options
                            .blocks_skipped_by_connectivity,
                        block_indices: debug_options.block_indices,
                    };

                    // Which *field* comes first is still up to `indexed_fields`,
                    // which hands back a `HashMap`.
                    let mut next_block_index = 0;

                    let mut process_block = |payload_block: PayloadBlockCondition| {
                        let block_index = next_block_index;
                        next_block_index += 1;

                        let cardinality = payload_block.cardinality;
                        let Some(points_to_index) = block_filter.points(
                            &graph_layers_builder,
                            block_index,
                            payload_block,
                        )?
                        else {
                            return Ok(());
                        };
                        let block_points = points_to_index.len();

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
                            id_tracker_ref.deref(),
                            &vector_storage_ref,
                            &quantized_vectors_ref,
                            &mut gpu_insert_context,
                            &payload_index_ref,
                            &pool,
                            stopped,
                            &mut additional_graph,
                            points_to_index,
                            block_filter_list.as_mut().unwrap(),
                            &mut indexed_vectors_set,
                            counter,
                        )?;
                        graph_layers_builder.merge_from_other(additional_graph);

                        debug!(
                            "payload block {field} [{cardinality}]: built {block_points} points (legacy)"
                        );
                        Ok(())
                    };

                    payload_index_ref.with_view(|v| {
                        v.for_each_payload_block(field, full_scan_threshold, &mut process_block)
                    })?;
                }
            }

            // A build where the block-local copy never engaged looks, from the
            // outside, exactly like one where it did. Say so once, at info, so
            // the no-op is visible without per-block debug logging.
            let declined = gather_declined.load(Ordering::Relaxed);
            if declined > 0 {
                log::info!(
                    "payload-block vector gather declined for {declined} block(s): {}",
                    BlockVectors::gather_constraints(
                        &vector_storage_ref,
                        quantized_vectors_ref.as_ref(),
                        gather_budget,
                    ),
                );
            }

            let indexed_payload_vectors = indexed_vectors_set.count_ones();

            debug_assert!(indexed_vectors >= indexed_payload_vectors || config.m == 0);
            indexed_vectors = indexed_vectors.max(indexed_payload_vectors);
            debug_assert!(indexed_payload_vectors <= total_vector_count);
        } else {
            debug!("skip building additional HNSW links");
        }

        config.indexed_vector_count.replace(indexed_vectors);

        // Always skip loading graph to RAM on build
        // as it will be discarded anyway
        let is_on_disk = true;

        let graph_links_vectors = hnsw_config
            .inline_storage
            .unwrap_or_default()
            .then(|| {
                // NOTE: the configuration is silently ignored if try_new fails.
                StorageGraphLinksVectors::try_new(
                    &vector_storage_ref,
                    quantized_vectors_ref.as_ref(),
                )
            })
            .flatten();
        let format_param = match graph_links_vectors.as_ref() {
            Some(v) => GraphLinksFormatParam::CompressedWithVectors(v),
            None => GraphLinksFormatParam::Compressed,
        };

        if let Some(inspect_builder) = debug_options.inspect_builder {
            inspect_builder(&graph_layers_builder);
        }

        let graph: GraphLayers =
            graph_layers_builder.into_graph_layers(path, format_param, is_on_disk)?;

        #[cfg(debug_assertions)]
        {
            for (idx, deleted) in deleted_bitslice.iter().enumerate() {
                if *deleted {
                    graph.links.for_each_link(idx as PointOffsetType, 0, |_| {
                        panic!("Deleted point in the graph");
                    });
                }
            }
        }

        debug!("finish additional payload field indexing");

        config.save(&HnswGraphConfig::get_config_path(path))?;

        drop(id_tracker_ref);
        drop(vector_storage_ref);
        drop(quantized_vectors_ref);
        drop(payload_index_ref);

        Ok(HNSWIndex {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            config,
            path: path.to_owned(),
            graph,
            searches_telemetry: HNSWSearchesTelemetry::new(),
            is_on_disk,
        })
    }
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

/// Decides which payload blocks of one field are worth indexing.
struct BlockFilter<'a> {
    field: &'a JsonPath,
    payload_index: &'a StructPayloadIndex,
    vector_storage: &'a VectorStorageEnum,
    stopped: &'a AtomicBool,
    full_scan_threshold: usize,
    max_block_size: usize,
    /// Whether the connectivity shortcut applies to this field at all.
    check_connectivity: bool,
    required_connectivity: Option<f32>,
    percolation: f32,
    block_seed_base: u64,
    blocks_built: Option<&'a AtomicUsize>,
    blocks_skipped_by_connectivity: Option<&'a AtomicUsize>,
    block_indices: Option<&'a Mutex<Vec<usize>>>,
}

impl BlockFilter<'_> {
    /// Points of `payload_block` worth indexing, or `None` if it is skipped.
    ///
    /// `block_index` is the block's position in its field's generation order.
    /// It seeds the connectivity shortcut, so both build paths have to number
    /// the blocks of a field the same way to reach the same skip decisions.
    fn points(
        &self,
        graph: &GraphLayersBuilder,
        block_index: usize,
        payload_block: PayloadBlockCondition,
    ) -> OperationResult<Option<Vec<PointOffsetType>>> {
        check_process_stopped(self.stopped)?;

        if let Some(block_indices) = self.block_indices {
            block_indices.lock().push(block_index);
        }

        let field = self.field;
        let cardinality = payload_block.cardinality;
        if cardinality > self.max_block_size {
            debug!(
                "payload block {field} [{cardinality}]: skip, over {}",
                self.max_block_size,
            );
            return Ok(None);
        }

        let points_to_index = condition_points(
            payload_block.condition,
            self.payload_index,
            self.vector_storage,
            self.stopped,
        )?;

        // This is a heuristic to skip building graph for mostly deleted blocks.
        // It might be, that majority of points do not actually have vectors
        // (vectors marked as deleted), so we can avoid building graph for such blocks.
        //
        // FYI: query heuristic does account
        // for deleted vectors via [`adjust_to_available_vectors`]
        const DELETED_POINTS_FACTOR: usize = 4; // allow block to have up to 75% of deleted points and still be indexed

        if points_to_index.len() <= self.full_scan_threshold / DELETED_POINTS_FACTOR {
            debug!(
                "payload block {field} [{cardinality}]: skip, only {} live points",
                points_to_index.len(),
            );
            return Ok(None);
        }

        if self.check_connectivity
            && let Some(required_connectivity) = self.required_connectivity
        {
            // Seeded per block, so the estimate does not depend on how the
            // blocks of this field were scheduled.
            let mut block_rng = SmallRng::seed_from_u64(self.block_seed_base ^ block_index as u64);
            let graph_connectivity =
                graph.subgraph_connectivity(&mut block_rng, &points_to_index, self.percolation);

            if graph_connectivity >= required_connectivity {
                trace!(
                    "skip building additional HNSW links for {field}, connectivity {graph_connectivity:.4} >= {required_connectivity:.4}"
                );
                debug!(
                    "payload block {field} [{cardinality}]: skip, connectivity {graph_connectivity:.4} >= {required_connectivity:.4}"
                );
                if let Some(blocks_skipped) = self.blocks_skipped_by_connectivity {
                    blocks_skipped.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(None);
            }
            trace!("graph connectivity: {graph_connectivity} for {field}");
        }

        if let Some(blocks_built) = self.blocks_built {
            blocks_built.fetch_add(1, Ordering::Relaxed);
        }

        Ok(Some(points_to_index))
    }
}

/// Builds one payload block's subgraph and merges it into the main graph.
struct BlockBuilder<'a> {
    vector_storage: &'a VectorStorageEnum,
    quantized_vectors: &'a Option<QuantizedVectors>,
    graph: &'a GraphLayersBuilder,
    vec_deleted: &'a BitSlice,
    point_deleted: &'a BitSlice,
    payload_m: HnswM,
    ef_construct: usize,
    /// Per-block ceiling on the vector copy for blocks built concurrently,
    /// already divided down by how many can be in flight at once. Blocks that
    /// run exclusively use [`MAX_BLOCK_GATHER_BYTES`] instead.
    gather_budget: usize,
    blocks_gathered: Option<&'a AtomicUsize>,
    /// Blocks the copy was wanted for but declined, reported once per build.
    gather_declined: &'a AtomicUsize,
    stopped: &'a AtomicBool,
}

impl BlockBuilder<'_> {
    /// Build `payload_block` and merge it, unless `filter` skips it.
    fn build(
        &self,
        filter: &BlockFilter,
        block_index: usize,
        payload_block: PayloadBlockCondition,
        counter: &AtomicU64,
        insert_pool: Option<&ThreadPool>,
    ) -> OperationResult<()> {
        let field = filter.field;
        let cardinality = payload_block.cardinality;
        let Some(points_to_index) = filter.points(self.graph, block_index, payload_block)? else {
            return Ok(());
        };

        let block_deleted =
            block_deleted_flags(&points_to_index, self.vec_deleted, self.point_deleted);

        // Copied once per block and shared by every insertion in it, so the copy
        // costs one pass over the block against the many searches that read it.
        //
        // A block that has the pool to itself also has the memory cap to
        // itself: no other block is gathering while it builds, so the divided
        // concurrent budget would decline it for a transient that cannot occur.
        let gather_budget = if insert_pool.is_some() {
            MAX_BLOCK_GATHER_BYTES
        } else {
            self.gather_budget
        };
        let block_vectors = BlockVectors::try_gather(
            &points_to_index,
            self.vector_storage,
            self.quantized_vectors.as_ref(),
            gather_budget,
        );

        if block_vectors.is_some() {
            if let Some(blocks_gathered) = self.blocks_gathered {
                blocks_gathered.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.gather_declined.fetch_add(1, Ordering::Relaxed);
        }

        let block_graph = build_filtered_graph_compact(
            self.vector_storage,
            self.quantized_vectors,
            block_vectors.as_ref(),
            insert_pool,
            self.stopped,
            self.payload_m,
            self.ef_construct,
            &points_to_index,
            &block_deleted,
            counter,
        )?;

        let links_added = self.graph.merge_block(block_graph, &points_to_index);

        debug!(
            "payload block {field} [{cardinality}]: built {} points, {links_added} links, \
             gathered={}",
            points_to_index.len(),
            block_vectors.is_some(),
        );
        Ok(())
    }
}

/// Per-block deletion flags, indexed by the local id of a block point.
fn block_deleted_flags(
    points_to_index: &[PointOffsetType],
    vec_deleted: &BitSlice,
    point_deleted: &BitSlice,
) -> BitVec {
    let not_deleted = NotDeletedChecker {
        point_deleted,
        vec_deleted,
    };
    let mut block_deleted = BitVec::repeat(false, points_to_index.len());
    for (local_id, &global_id) in points_to_index.iter().enumerate() {
        if !not_deleted.check_infallible(global_id) {
            block_deleted.set(local_id, true);
        }
    }
    debug_assert_eq!(
        block_deleted.count_ones(),
        0,
        "payload blocks are expected to hold only live points",
    );
    block_deleted
}

/// Build the subgraph of a payload block in its own id space.
///
/// Points are numbered `0..points_to_index.len()`, in `points_to_index` order,
/// which keeps the builder proportional to the block instead of to the whole
/// segment. Every candidate reachable in such a graph is a block member by
/// construction, so unlike [`build_filtered_graph`] this needs no filter
/// context to keep the search inside the block.
///
/// Passing no `pool` inserts every point on the calling thread.
#[allow(clippy::too_many_arguments)]
fn build_filtered_graph_compact(
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    block_vectors: Option<&BlockVectors>,
    pool: Option<&ThreadPool>,
    stopped: &AtomicBool,
    hnsw_m: HnswM,
    ef_construct: usize,
    points_to_index: &[PointOffsetType],
    block_deleted: &BitSlice,
    counter: &AtomicU64,
) -> OperationResult<GraphLayersBuilder> {
    let block_graph = GraphLayersBuilder::new_with_params(
        points_to_index.len(),
        hnsw_m,
        ef_construct,
        1,
        HNSW_USE_HEURISTIC,
        false,
    );

    let insert_point = |local_id: PointOffsetType| {
        check_process_stopped(stopped)?;

        // This hardware counter can be discarded, since it is only used for internal operations
        let internal_hardware_counter = HardwareCounterCell::disposable();

        let points_scorer = FilteredScorer::new_block_scorer(
            points_to_index[local_id as usize],
            points_to_index,
            vector_storage,
            quantized_vectors.as_ref(),
            block_vectors,
            block_deleted,
            internal_hardware_counter,
        )?;

        block_graph.link_new_point(local_id, points_scorer);

        counter.fetch_add(1, Ordering::Relaxed);

        Ok::<_, OperationError>(())
    };

    let block_len = points_to_index.len() as PointOffsetType;
    let first_points = block_len.min(SINGLE_THREADED_HNSW_BUILD_THRESHOLD as PointOffsetType);

    // First index points in single thread so ensure warm start for parallel indexing process
    for local_id in 0..first_points {
        insert_point(local_id)?;
    }
    // Once initial structure is built, index remaining points in parallel
    // So that each thread will insert points in different parts of the graph,
    // it is less likely that they will compete for the same locks
    match pool {
        Some(pool) if block_len > first_points => {
            pool.install(|| {
                (first_points..block_len)
                    .into_par_iter()
                    .try_for_each(insert_point)
            })?;
        }
        _ => {
            for local_id in first_points..block_len {
                insert_point(local_id)?;
            }
        }
    }

    Ok(block_graph)
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
    if let Some(gpu_constructed_graph) = super::gpu_build::build_filtered_graph_on_gpu(
        id_tracker,
        vector_storage,
        quantized_vectors,
        gpu_insert_context.as_mut(),
        graph_layers_builder,
        block_filter_list,
        &points_to_index,
        stopped,
    )? {
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
                .try_for_each(insert_points)
        })?;
    }
    Ok(())
}
