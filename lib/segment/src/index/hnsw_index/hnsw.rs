use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use bitvec::prelude::BitSlice;
use bitvec::vec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::cow::BoxCow;
#[cfg(target_os = "linux")]
use common::cpu::linux_low_thread_priority;
use common::ext::BitSliceExt as _;
use common::flags::FeatureFlags;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use fs_err as fs;
use itertools::EitherOrBoth;
use log::{debug, trace};
use memory::fadvise::clear_disk_cache;
use parking_lot::Mutex;
use rand::Rng;
use rayon::ThreadPool;
use rayon::prelude::*;

#[cfg(feature = "gpu")]
use super::gpu::gpu_devices_manager::LockedGpuDevice;
use super::gpu::gpu_insert_context::GpuInsertContext;
#[cfg(feature = "gpu")]
use super::gpu::gpu_vector_storage::GpuVectorStorage;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
use crate::id_tracker::IdTrackerSS;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
#[cfg(feature = "gpu")]
use crate::index::hnsw_index::gpu::gpu_graph_builder::GPU_MAX_VISITED_FLAGS_FACTOR;
#[cfg(feature = "gpu")]
use crate::index::hnsw_index::gpu::{get_gpu_groups_count, gpu_graph_builder::build_hnsw_on_gpu};
use crate::index::hnsw_index::graph_layers::{GraphLayers, GraphLayersWithVectors};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_layers_healer::GraphLayersHealer;
use crate::index::hnsw_index::graph_links::{GraphLinksFormatParam, StorageGraphLinksVectors};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::vector_index_search_common::{
    get_oversampled_top, is_quantized_search, postprocess_search_result,
};
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};
use crate::index::{PayloadIndex, VectorIndex, VectorIndexEnum};
use crate::payload_storage::FilterContext;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::Condition::Field;
use crate::types::{
    FieldCondition, Filter, HnswConfig, HnswGlobalConfig, QuantizationSearchParams, SearchParams,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::query::DiscoveryQuery;
use crate::vector_storage::{VectorStorage, VectorStorageEnum, new_raw_scorer};

const HNSW_USE_HEURISTIC: bool = true;
const FINISH_MAIN_GRAPH_LOG_MESSAGE: &str = "Finish main graph in time";

/// Build first N points in HNSW graph using only a single thread, to avoid
/// disconnected components in the graph.
#[cfg(debug_assertions)]
pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 32;
#[cfg(not(debug_assertions))]
pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

const LINK_COMPRESSION_CONVERT_EXISTING: bool = false;

#[derive(Debug)]
pub struct HNSWIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    config: HnswGraphConfig,
    path: PathBuf,
    graph: GraphLayers,
    searches_telemetry: HNSWSearchesTelemetry,
    is_on_disk: bool,
}

#[derive(Debug)]
struct HNSWSearchesTelemetry {
    unfiltered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    filtered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_hnsw: Arc<Mutex<OperationDurationsAggregator>>,
    small_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
    large_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
    exact_filtered: Arc<Mutex<OperationDurationsAggregator>>,
    exact_unfiltered: Arc<Mutex<OperationDurationsAggregator>>,
}

impl HNSWSearchesTelemetry {
    fn new() -> Self {
        Self {
            unfiltered_plain: OperationDurationsAggregator::new(),
            filtered_plain: OperationDurationsAggregator::new(),
            unfiltered_hnsw: OperationDurationsAggregator::new(),
            small_cardinality: OperationDurationsAggregator::new(),
            large_cardinality: OperationDurationsAggregator::new(),
            exact_filtered: OperationDurationsAggregator::new(),
            exact_unfiltered: OperationDurationsAggregator::new(),
        }
    }
}

pub struct HnswIndexOpenArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub hnsw_config: HnswConfig,
}

impl HNSWIndex {
    pub fn open(args: HnswIndexOpenArgs<'_>) -> OperationResult<Self> {
        let HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            hnsw_config,
        } = args;

        let config_path = HnswGraphConfig::get_config_path(path);
        let config = if config_path.exists() {
            HnswGraphConfig::load(&config_path)?
        } else {
            let vector_storage = vector_storage.borrow();
            let available_vectors = vector_storage.available_vector_count();
            let full_scan_threshold = vector_storage
                .size_of_available_vectors_in_bytes()
                .checked_div(available_vectors)
                .and_then(|avg_vector_size| {
                    hnsw_config
                        .full_scan_threshold
                        .saturating_mul(BYTES_IN_KB)
                        .checked_div(avg_vector_size)
                })
                .unwrap_or(1);

            HnswGraphConfig::new(
                hnsw_config.m,
                hnsw_config.ef_construct,
                full_scan_threshold,
                hnsw_config.max_indexing_threads,
                hnsw_config.payload_m,
                available_vectors,
            )
        };

        let do_convert = LINK_COMPRESSION_CONVERT_EXISTING;

        let is_on_disk = hnsw_config.on_disk.unwrap_or(false);

        let graph = GraphLayers::load(path, is_on_disk, do_convert)?;

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

    pub fn is_on_disk(&self) -> bool {
        self.is_on_disk
    }

    #[cfg(test)]
    pub(super) fn graph(&self) -> &GraphLayers {
        &self.graph
    }

    pub fn get_quantized_vectors(&self) -> Arc<AtomicRefCell<Option<QuantizedVectors>>> {
        self.quantized_vectors.clone()
    }

    pub fn build<R: Rng + ?Sized>(
        open_args: HnswIndexOpenArgs<'_>,
        build_args: VectorIndexBuildArgs<'_, R>,
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
        for vector_id in id_tracker_ref.iter_internal_excluding(deleted_bitslice) {
            check_process_stopped(stopped)?;
            indexed_vectors += 1;

            let level = old_index
                .as_ref()
                .and_then(|old_index| old_index.point_level(vector_id))
                .unwrap_or_else(|| graph_layers_builder.get_random_layer(rng));
            graph_layers_builder.set_levels(vector_id, level);
        }

        #[allow(unused_mut)]
        let mut build_main_graph = config.m > 0;
        if !build_main_graph {
            debug!("skip building main HNSW graph");
        }

        // Try to build the main graph on GPU if possible.
        // Store created gpu vectors to reuse them for payload links.
        #[cfg(feature = "gpu")]
        let gpu_vectors = if build_main_graph {
            let timer = std::time::Instant::now();
            let gpu_vectors = Self::create_gpu_vectors(
                gpu_device,
                &vector_storage_ref,
                &quantized_vectors_ref,
                stopped,
            )?;
            if let Some(gpu_constructed_graph) = Self::build_main_graph_on_gpu(
                id_tracker_ref.deref(),
                &vector_storage_ref,
                &quantized_vectors_ref,
                gpu_vectors.as_ref(),
                &graph_layers_builder,
                deleted_bitslice,
                num_entries,
                stopped,
            )? {
                graph_layers_builder = gpu_constructed_graph;
                build_main_graph = false;
                debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
            }
            gpu_vectors
        } else {
            None
        };

        if build_main_graph {
            let mut ids = Vec::with_capacity(total_vector_count);
            let mut first_few_ids = Vec::with_capacity(SINGLE_THREADED_HNSW_BUILD_THRESHOLD);

            let mut ids_iter = id_tracker_ref.iter_internal_excluding(deleted_bitslice);
            if let Some(old_index) = old_index {
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

            let timer = std::time::Instant::now();

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

                Ok::<_, OperationError>(())
            };

            for vector_id in first_few_ids {
                insert_point(vector_id)?;
            }

            if !ids.is_empty() {
                pool.install(|| ids.into_par_iter().try_for_each(insert_point))?;
            }

            debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
        } else {
            drop(old_index);
        }

        let payload_m = HnswM::new(
            config.payload_m.unwrap_or(config.m),
            config.payload_m0.unwrap_or(config.m0),
        );

        let indexed_fields = payload_index_ref.indexed_fields();

        if payload_m.m > 0 && !indexed_fields.is_empty() {
            // Calculate true average number of links per vertex in the HNSW graph
            // to better estimate percolation threshold
            let average_links_per_0_level =
                graph_layers_builder.get_average_connectivity_on_level(0);
            let average_links_per_0_level_int = (average_links_per_0_level as usize).max(1);

            // Estimate connectivity of the main graph
            let all_points = id_tracker_ref
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
                    graph_layers_builder.subgraph_connectivity(&all_points, percolation),
                    graph_layers_builder.subgraph_connectivity(&all_points, percolation),
                    graph_layers_builder.subgraph_connectivity(&all_points, percolation),
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

            for (index_pos, (field, _)) in indexed_fields.into_iter().enumerate() {
                debug!("building additional index for field {}", &field);

                let is_tenant = payload_index_ref.is_tenant(&field);

                // It is expected, that graph will become disconnected less than
                // $1/m$ points left.
                // So blocks larger than $1/m$ are not needed.
                // We add multiplier for the extra safety.
                let percolation_multiplier = 4;
                let max_block_size = if config.m > 0 {
                    total_vector_count / average_links_per_0_level_int * percolation_multiplier
                } else {
                    usize::MAX
                };

                for payload_block in payload_index_ref.payload_blocks(&field, full_scan_threshold) {
                    check_process_stopped(stopped)?;
                    if payload_block.cardinality > max_block_size {
                        continue;
                    }

                    let points_to_index = Self::condition_points(
                        payload_block.condition,
                        id_tracker_ref.deref(),
                        &payload_index_ref,
                        &vector_storage_ref,
                    );

                    if !is_tenant
                        && index_pos > 0
                        && let Some(required_connectivity) = required_connectivity
                    {
                        // Always build for tenants
                        let graph_connectivity = graph_layers_builder
                            .subgraph_connectivity(&points_to_index, percolation);

                        if graph_connectivity >= required_connectivity {
                            trace!(
                                "skip building additional HNSW links for {field}, connectivity {graph_connectivity:.4} >= {required_connectivity:.4}"
                            );
                            continue;
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

                    Self::build_filtered_graph(
                        id_tracker_ref.deref(),
                        &vector_storage_ref,
                        &quantized_vectors_ref,
                        &mut gpu_insert_context,
                        &payload_index_ref,
                        &pool,
                        stopped,
                        &mut additional_graph,
                        points_to_index,
                        &mut block_filter_list,
                        &mut indexed_vectors_set,
                    )?;
                    graph_layers_builder.merge_from_other(additional_graph);
                }
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

    /// Get list of points for indexing, associated with payload block filtering condition
    fn condition_points(
        condition: FieldCondition,
        id_tracker: &IdTrackerSS,
        payload_index: &StructPayloadIndex,
        vector_storage: &VectorStorageEnum,
    ) -> Vec<PointOffsetType> {
        let filter = Filter::new_must(Field(condition));

        let disposed_hw_counter = HardwareCounterCell::disposable(); // Internal operation. No measurements needed

        let deleted_bitslice = vector_storage.deleted_vector_bitslice();

        let cardinality_estimation =
            payload_index.estimate_cardinality(&filter, &disposed_hw_counter);

        payload_index
            .iter_filtered_points(
                &filter,
                id_tracker,
                &cardinality_estimation,
                &disposed_hw_counter,
            )
            .filter(|&point_id| !deleted_bitslice.get_bit(point_id as usize).unwrap_or(false))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(unused_variables)]
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn build_filtered_graph(
        id_tracker: &IdTrackerSS,
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
    ) -> OperationResult<()> {
        block_filter_list.next_iteration();

        for block_point_id in points_to_index.iter().copied() {
            block_filter_list.check_and_update_visited(block_point_id);
            if !indexed_vectors_set.is_empty() {
                indexed_vectors_set.set(block_point_id as usize, true);
            }
        }

        #[cfg(feature = "gpu")]
        if let Some(gpu_constructed_graph) = Self::build_filtered_graph_on_gpu(
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

            let block_condition_checker = BuildConditionChecker {
                filter_list: block_filter_list,
                current_point: block_point_id,
            };
            let points_scorer = FilteredScorer::new_internal(
                block_point_id,
                vector_storage,
                quantized_vectors.as_ref(),
                Some(BoxCow::Borrowed(&block_condition_checker)),
                id_tracker.deleted_point_bitslice(),
                internal_hardware_counter,
            )?;

            graph_layers_builder.link_new_point(block_point_id, points_scorer);

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

    #[cfg(feature = "gpu")]
    #[allow(clippy::too_many_arguments)]
    fn build_main_graph_on_gpu(
        id_tracker: &IdTrackerSS,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: &Option<QuantizedVectors>,
        gpu_vectors: Option<&GpuVectorStorage>,
        graph_layers_builder: &GraphLayersBuilder,
        deleted_bitslice: &BitSlice,
        entry_points_num: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        let points_scorer_builder = |vector_id| {
            let hardware_counter = HardwareCounterCell::disposable();
            FilteredScorer::new_internal(
                vector_id,
                vector_storage,
                quantized_vectors.as_ref(),
                None,
                id_tracker.deleted_point_bitslice(),
                hardware_counter,
            )
        };

        let mut gpu_insert_context = if let Some(gpu_vectors) = gpu_vectors {
            Some(GpuInsertContext::new(
                gpu_vectors,
                get_gpu_groups_count(),
                graph_layers_builder.hnsw_m(),
                graph_layers_builder.ef_construct(),
                false,
                1..=GPU_MAX_VISITED_FLAGS_FACTOR,
            )?)
        } else {
            None
        };

        Self::build_graph_on_gpu(
            gpu_insert_context.as_mut(),
            graph_layers_builder,
            id_tracker.iter_internal_excluding(deleted_bitslice),
            entry_points_num,
            points_scorer_builder,
            stopped,
        )
    }

    #[cfg(feature = "gpu")]
    #[allow(clippy::too_many_arguments)]
    fn build_filtered_graph_on_gpu(
        id_tracker: &IdTrackerSS,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: &Option<QuantizedVectors>,
        gpu_insert_context: Option<&mut GpuInsertContext<'_>>,
        graph_layers_builder: &GraphLayersBuilder,
        block_filter_list: &VisitedListHandle,
        points_to_index: &[PointOffsetType],
        stopped: &AtomicBool,
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        Self::build_graph_on_gpu(
            gpu_insert_context,
            graph_layers_builder,
            points_to_index.iter().copied(),
            1,
            |block_point_id| -> OperationResult<_> {
                let hardware_counter = HardwareCounterCell::disposable();
                let block_condition_checker: Box<dyn FilterContext> =
                    Box::new(BuildConditionChecker {
                        filter_list: block_filter_list,
                        current_point: block_point_id,
                    });
                FilteredScorer::new_internal(
                    block_point_id,
                    vector_storage,
                    quantized_vectors.as_ref(),
                    Some(BoxCow::Owned(block_condition_checker)),
                    id_tracker.deleted_point_bitslice(),
                    hardware_counter,
                )
            },
            stopped,
        )
    }

    #[cfg(feature = "gpu")]
    #[allow(clippy::too_many_arguments)]
    fn build_graph_on_gpu<'a, 'b>(
        gpu_insert_context: Option<&mut GpuInsertContext<'b>>,
        graph_layers_builder: &GraphLayersBuilder,
        points_to_index: impl Iterator<Item = PointOffsetType>,
        entry_points_num: usize,
        points_scorer_builder: impl Fn(PointOffsetType) -> OperationResult<FilteredScorer<'a>>
        + Send
        + Sync,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        if let Some(gpu_insert_context) = gpu_insert_context {
            let gpu_constructed_graph = build_hnsw_on_gpu(
                gpu_insert_context,
                graph_layers_builder,
                get_gpu_groups_count(),
                entry_points_num,
                SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
                points_to_index.collect::<Vec<_>>(),
                points_scorer_builder,
                stopped,
            );

            // GPU construction does not return an error. If it fails, it will fall back to CPU.
            // To cover stopping case, we need to check stopping flag here.
            check_process_stopped(stopped)?;

            match gpu_constructed_graph {
                Ok(gpu_constructed_graph) => Ok(Some(gpu_constructed_graph)),
                Err(gpu_error) => {
                    log::warn!("Failed to build HNSW on GPU: {gpu_error}. Falling back to CPU.");
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "gpu")]
    fn create_gpu_vectors(
        gpu_device: Option<&LockedGpuDevice>,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: &Option<QuantizedVectors>,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<GpuVectorStorage>> {
        use crate::index::hnsw_index::gpu::get_gpu_force_half_precision;
        if vector_storage.total_vector_count() < SINGLE_THREADED_HNSW_BUILD_THRESHOLD {
            return Ok(None);
        }

        if let Some(gpu_device) = gpu_device {
            let gpu_vectors = GpuVectorStorage::new(
                gpu_device.device(),
                vector_storage,
                quantized_vectors.as_ref(),
                get_gpu_force_half_precision(),
                stopped,
            );

            // GPU construction does not return an error. If it fails, it will fall back to CPU.
            // To cover stopping case, we need to check stopping flag here.
            check_process_stopped(stopped)?;

            match gpu_vectors {
                Ok(gpu_vectors) => Ok(Some(gpu_vectors)),
                Err(err) => {
                    log::error!("Failed to create GPU vectors, use CPU instead. Error: {err}.");
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn search_with_graph(
        &self,
        vector: &QueryVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        custom_entry_points: Option<&[PointOffsetType]>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let ef = params
            .and_then(|params| params.hnsw_ef)
            .unwrap_or(self.config.ef);

        let is_stopped = vector_query_context.is_stopped();

        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();
        let vector_storage = self.vector_storage.borrow();
        let quantized_vectors = self.quantized_vectors.borrow();

        let deleted_points = vector_query_context
            .deleted_points()
            .unwrap_or_else(|| id_tracker.deleted_point_bitslice());

        let hw_counter = vector_query_context.hardware_counter();
        let oversampled_top = get_oversampled_top(quantized_vectors.as_ref(), params, top);

        let search_with_vectors = || -> OperationResult<Option<Vec<ScoredPointOffset>>> {
            if !self.graph.has_vectors() || !is_quantized_search(quantized_vectors.as_ref(), params)
            {
                return Ok(None);
            }
            let Some(quantized_vectors) = quantized_vectors.as_ref() else {
                return Ok(None);
            };

            // Quantized vectors are "link vectors"
            let link_scorer_filtered = FilteredScorer::new(
                vector.to_owned(),
                &vector_storage,
                Some(quantized_vectors),
                filter.map(|f| BoxCow::Owned(payload_index.filter_context(f, &hw_counter))),
                deleted_points,
                vector_query_context.hardware_counter(),
            )?;
            let Some(link_scorer_filtered_bytes) = link_scorer_filtered.scorer_bytes() else {
                return Ok(None);
            };

            // Full vectors are "base vectors"
            let base_scorer = new_raw_scorer(
                vector.to_owned(),
                &vector_storage,
                vector_query_context.hardware_counter(),
            )?;
            let Some(base_scorer_bytes) = base_scorer.scorer_bytes() else {
                return Ok(None);
            };

            Ok(Some(self.graph.search_with_vectors(
                top,
                std::cmp::max(ef, oversampled_top),
                &link_scorer_filtered,
                &link_scorer_filtered_bytes,
                base_scorer_bytes,
                custom_entry_points,
                &vector_query_context.is_stopped(),
            )?))
        };

        let regular_search = || -> OperationResult<Vec<ScoredPointOffset>> {
            let filter_context = filter.map(|f| payload_index.filter_context(f, &hw_counter));
            let points_scorer = Self::construct_search_scorer(
                vector,
                &vector_storage,
                quantized_vectors.as_ref(),
                deleted_points,
                params,
                vector_query_context.hardware_counter(),
                filter_context,
            )?;

            let search_result = self.graph.search(
                oversampled_top,
                ef,
                points_scorer,
                custom_entry_points,
                &is_stopped,
            )?;

            postprocess_search_result(
                search_result,
                id_tracker.deleted_point_bitslice(),
                &vector_storage,
                quantized_vectors.as_ref(),
                vector,
                params,
                top,
                vector_query_context.hardware_counter(),
            )
        };

        // Try to use graph with vectors first.
        if let Some(search_result) = search_with_vectors()? {
            Ok(search_result)
        } else {
            // Graph with vectors is not available, fallback to regular graph search.
            regular_search()
        }
    }

    fn search_vectors_with_graph(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        vectors
            .iter()
            .map(|&vector| match vector {
                QueryVector::Discovery(discovery_query) => self.discovery_search_with_graph(
                    discovery_query.clone(),
                    filter,
                    top,
                    params,
                    vector_query_context,
                ),
                other => {
                    self.search_with_graph(other, filter, top, params, None, vector_query_context)
                }
            })
            .collect()
    }

    fn search_plain_iterator(
        &self,
        vector: &QueryVector,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let quantized_vectors = self.quantized_vectors.borrow();

        let deleted_points = vector_query_context
            .deleted_points()
            .unwrap_or_else(|| id_tracker.deleted_point_bitslice());

        let is_stopped = vector_query_context.is_stopped();

        let points_scorer = Self::construct_search_scorer(
            vector,
            &vector_storage,
            quantized_vectors.as_ref(),
            deleted_points,
            params,
            vector_query_context.hardware_counter(),
            None,
        )?;
        let oversampled_top = get_oversampled_top(quantized_vectors.as_ref(), params, top);

        let search_result = points_scorer.peek_top_iter(points, oversampled_top, &is_stopped)?;

        let res = postprocess_search_result(
            search_result,
            id_tracker.deleted_point_bitslice(),
            &vector_storage,
            quantized_vectors.as_ref(),
            vector,
            params,
            top,
            vector_query_context.hardware_counter(),
        )?;
        Ok(res)
    }

    fn search_plain(
        &self,
        vector: &QueryVector,
        filtered_points: &[PointOffsetType],
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        self.search_plain_iterator(
            vector,
            &mut filtered_points.iter().copied(),
            top,
            params,
            vector_query_context,
        )
    }

    fn search_plain_unfiltered(
        &self,
        vector: &QueryVector,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let id_tracker = self.id_tracker.borrow();
        let mut ids_iterator = id_tracker.iter_internal();
        self.search_plain_iterator(vector, &mut ids_iterator, top, params, vector_query_context)
    }

    fn search_vectors_plain(
        &self,
        vectors: &[&QueryVector],
        filter: &Filter,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let payload_index = self.payload_index.borrow();
        // share filtered points for all query vectors
        let filtered_points =
            payload_index.query_points(filter, &vector_query_context.hardware_counter());
        vectors
            .iter()
            .map(|vector| {
                self.search_plain(vector, &filtered_points, top, params, vector_query_context)
            })
            .collect()
    }

    fn discovery_search_with_graph(
        &self,
        discovery_query: DiscoveryQuery<VectorInternal>,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        // Stage 1: Find best entry points using Context search
        let query_vector = QueryVector::Context(discovery_query.pairs.clone().into());

        const DISCOVERY_ENTRY_POINT_COUNT: usize = 10;

        let custom_entry_points: Vec<_> = self
            .search_with_graph(
                &query_vector,
                filter,
                DISCOVERY_ENTRY_POINT_COUNT,
                params,
                None,
                vector_query_context,
            )
            .map(|search_result| search_result.iter().map(|x| x.idx).collect())?;

        // Stage 2: Discovery search with entry points
        let query_vector = QueryVector::Discovery(discovery_query);

        self.search_with_graph(
            &query_vector,
            filter,
            top,
            params,
            Some(&custom_entry_points),
            vector_query_context,
        )
    }

    fn construct_search_scorer<'a>(
        vector: &QueryVector,
        vector_storage: &'a VectorStorageEnum,
        quantized_storage: Option<&'a QuantizedVectors>,
        deleted_points: &'a BitSlice,
        params: Option<&SearchParams>,
        hardware_counter: HardwareCounterCell,
        filter_context: Option<Box<dyn FilterContext + 'a>>,
    ) -> OperationResult<FilteredScorer<'a>> {
        let quantization_enabled = is_quantized_search(quantized_storage, params);
        FilteredScorer::new(
            vector.to_owned(),
            vector_storage,
            quantization_enabled.then_some(quantized_storage).flatten(),
            filter_context.map(BoxCow::Owned),
            deleted_points,
            hardware_counter,
        )
    }

    /// Read underlying data from disk into disk cache.
    pub fn populate(&self) -> OperationResult<()> {
        self.graph.populate()
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        for file in self.graph.files(&self.path) {
            clear_disk_cache(&file)?
        }
        Ok(())
    }
}

impl VectorIndex for HNSWIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        // If neither `m` nor `payload_m` is set, HNSW doesn't have any links.
        // And if so, we need to fall back to plain search (optionally, with quantization).

        let is_hnsw_disabled = self.config.m == 0 && self.config.payload_m.unwrap_or(0) == 0;
        let exact = params.map(|params| params.exact).unwrap_or(false);

        let exact_params = if exact {
            params.map(|params| {
                let mut params = *params;
                params.quantization = Some(QuantizationSearchParams {
                    ignore: true,
                    rescore: Some(false),
                    oversampling: None,
                }); // disable quantization for exact search
                params
            })
        } else {
            None
        };

        match filter {
            None => {
                let vector_storage = self.vector_storage.borrow();

                // Determine whether to do a plain or graph search, and pick search timer aggregator
                // Because an HNSW graph is built, we'd normally always assume to search the graph.
                // But because a lot of points may be deleted in this graph, it may just be faster
                // to do a plain search instead.
                let plain_search = exact
                    || is_hnsw_disabled
                    || vector_storage.available_vector_count() < self.config.full_scan_threshold;

                // Do plain or graph search
                if plain_search {
                    let _timer = ScopeDurationMeasurer::new(if exact {
                        &self.searches_telemetry.exact_unfiltered
                    } else {
                        &self.searches_telemetry.unfiltered_plain
                    });

                    let params_ref = if exact { exact_params.as_ref() } else { params };

                    vectors
                        .iter()
                        .map(|&vector| {
                            self.search_plain_unfiltered(vector, top, params_ref, query_context)
                        })
                        .collect()
                } else {
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_hnsw);
                    self.search_vectors_with_graph(vectors, None, top, params, query_context)
                }
            }
            Some(query_filter) => {
                // depending on the amount of filtered-out points the optimal strategy could be
                // - to retrieve possible points and score them after
                // - to use HNSW index with filtering condition

                // if exact search is requested, we should not use HNSW index
                if exact || is_hnsw_disabled {
                    let _timer = ScopeDurationMeasurer::new(if exact {
                        &self.searches_telemetry.exact_filtered
                    } else {
                        &self.searches_telemetry.filtered_plain
                    });

                    let params_ref = if exact { exact_params.as_ref() } else { params };

                    return self.search_vectors_plain(
                        vectors,
                        query_filter,
                        top,
                        params_ref,
                        query_context,
                    );
                }

                let payload_index = self.payload_index.borrow();
                let vector_storage = self.vector_storage.borrow();
                let id_tracker = self.id_tracker.borrow();
                let available_vector_count = vector_storage.available_vector_count();

                let hw_counter = query_context.hardware_counter();

                let query_point_cardinality =
                    payload_index.estimate_cardinality(query_filter, &hw_counter);
                let query_cardinality = adjust_to_available_vectors(
                    query_point_cardinality,
                    available_vector_count,
                    id_tracker.available_point_count(),
                );

                if query_cardinality.max < self.config.full_scan_threshold {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    return self.search_vectors_plain(
                        vectors,
                        query_filter,
                        top,
                        params,
                        query_context,
                    );
                }

                if query_cardinality.min > self.config.full_scan_threshold {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.large_cardinality);
                    return self.search_vectors_with_graph(
                        vectors,
                        filter,
                        top,
                        params,
                        query_context,
                    );
                }

                let filter_context = payload_index.filter_context(query_filter, &hw_counter);

                // Fast cardinality estimation is not enough, do sample estimation of cardinality
                let id_tracker = self.id_tracker.borrow();
                if sample_check_cardinality(
                    id_tracker.sample_ids(Some(vector_storage.deleted_vector_bitslice())),
                    |idx| filter_context.check(idx),
                    self.config.full_scan_threshold,
                    available_vector_count, // Check cardinality among available vectors
                ) {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.large_cardinality);
                    self.search_vectors_with_graph(vectors, filter, top, params, query_context)
                } else {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    self.search_vectors_plain(vectors, query_filter, top, params, query_context)
                }
            }
        }
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        let tm = &self.searches_telemetry;
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: tm.unfiltered_plain.lock().get_statistics(detail),
            filtered_plain: tm.filtered_plain.lock().get_statistics(detail),
            unfiltered_hnsw: tm.unfiltered_hnsw.lock().get_statistics(detail),
            filtered_small_cardinality: tm.small_cardinality.lock().get_statistics(detail),
            filtered_large_cardinality: tm.large_cardinality.lock().get_statistics(detail),
            filtered_exact: tm.exact_filtered.lock().get_statistics(detail),
            filtered_sparse: Default::default(),
            unfiltered_exact: tm.exact_unfiltered.lock().get_statistics(detail),
            unfiltered_sparse: Default::default(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.graph.files(&self.path);
        let config_path = HnswGraphConfig::get_config_path(&self.path);
        if config_path.exists() {
            files.push(config_path);
        }
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.files() // All HNSW index files are immutable 😎
    }

    fn indexed_vector_count(&self) -> usize {
        self.config
            .indexed_vector_count
            // If indexed vector count is unknown, fall back to number of points
            .unwrap_or_else(|| self.graph.num_points())
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.vector_storage
            .borrow()
            .size_of_available_vectors_in_bytes()
    }

    fn update_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: Option<VectorRef>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error("Cannot update HNSW index"))
    }
}

/// Contains enough information to decide which one old index to use.
/// Once decided, it is converted to [`OldIndex`].
struct OldIndexCandidate<'a> {
    index: AtomicRef<'a, HNSWIndex>,
    /// Mapping from old index to new index.
    /// `old_to_new[old_idx] == Some(new_idx)`.
    old_to_new: Vec<Option<PointOffsetType>>,
    /// Count of successfully mapped points.
    valid_points: usize,
    /// Count of points that are missing in the new index.
    missing_points: usize,
}

pub(super) struct OldIndex<'a> {
    index: AtomicRef<'a, HNSWIndex>,
    /// Mapping from old index to new index.
    /// `old_to_new[old_idx] == Some(new_idx)`.
    pub old_to_new: Vec<Option<PointOffsetType>>,
    /// Mapping from new index to old index.
    /// `new_to_old[new_idx] == Some(old_idx)`.
    pub new_to_old: Vec<Option<PointOffsetType>>,
}

impl<'a> OldIndexCandidate<'a> {
    /// Evaluate whether we can use the old index.
    fn evaluate(
        feature_flags: &FeatureFlags,
        old_index: &'a Arc<AtomicRefCell<VectorIndexEnum>>,
        config: &HnswGraphConfig,
        hnsw_global_config: &HnswGlobalConfig,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: &Option<QuantizedVectors>,
        id_tracker: &IdTrackerSS,
    ) -> Option<Self> {
        if !feature_flags.incremental_hnsw_building {
            return None;
        }

        let old_index = AtomicRef::filter_map(old_index.borrow(), |index| match index {
            VectorIndexEnum::Hnsw(old_index) => Some(old_index),
            _ => None,
        })?;

        let old_quantized_vectors_ref = old_index.quantized_vectors.borrow();
        let old_quantization_config = old_quantized_vectors_ref
            .as_ref()
            .map(|qv| &qv.config().quantization_config);
        let new_quantization_config = quantized_vectors
            .as_ref()
            .map(|qv| &qv.config().quantization_config);

        let no_main_graph = config.m == 0;
        let configuration_mismatch = config.m != old_index.config.m
            || config.m0 != old_index.config.m0
            || config.ef_construct != old_index.config.ef_construct
            || new_quantization_config != old_quantization_config;
        if no_main_graph || configuration_mismatch {
            return None;
        }
        drop(old_quantized_vectors_ref);

        let new_deleted = vector_storage.deleted_vector_bitslice();
        let old_id_tracker = old_index.id_tracker.borrow();

        let healing_enabled = hnsw_global_config.healing_threshold > 0.0;

        if old_id_tracker.deleted_point_count() != 0 {
            // Old index has deleted points.
            if !healing_enabled {
                return None;
            }
        }

        // Rough check whether the point is included in the old graph.
        // If it's included, it almost certainly has at least one outgoing link at level 0.
        let old_graph_has_point =
            |id: PointOffsetType| old_index.graph.links.links(id, 0).next().is_some();

        // Build old_to_new mapping.
        let mut valid_points = 0;
        let mut missing_points = 0;
        let mut old_to_new = vec![None; old_id_tracker.total_point_count()];

        // Loop 1: Loop through all external ids, which present in either new or old index.
        // So that we have 3 possible cases: `in old`, `in new`, `in both`.
        //
        // If we have `in both` case, we need to fill the `old_to_new` mapping.
        // Otherwise, we are interested in counts of "missing" points - which absence in the new
        for item in itertools::merge_join_by(
            id_tracker.iter_from(None),
            old_id_tracker.iter_from(None),
            |(new_external_id, _), (old_external_id, _)| new_external_id.cmp(old_external_id),
        ) {
            let (new_offset, old_offset): (Option<PointOffsetType>, Option<PointOffsetType>) =
                match item {
                    EitherOrBoth::Both((_, new_offset), (_, old_offset)) => {
                        (Some(new_offset), Some(old_offset))
                    }
                    EitherOrBoth::Left((_, new_offset)) => (Some(new_offset), None),
                    EitherOrBoth::Right((_, old_offset)) => (None, Some(old_offset)),
                };

            let new_offset =
                new_offset.filter(|&id| !new_deleted.get_bit(id as usize).unwrap_or(false));

            // If vector is not present in the old graph, assume it's deleted.
            let old_offset = old_offset.filter(|&id| old_graph_has_point(id));

            match (new_offset, old_offset) {
                (_, None) => (),
                (None, Some(_)) => {
                    // Vector was in the old index, but not in the new one.
                    missing_points += 1;
                    if !healing_enabled {
                        return None;
                    }
                }
                (Some(new_offset), Some(old_offset)) => {
                    let new_version = id_tracker.internal_version(new_offset);
                    let old_version = old_id_tracker.internal_version(old_offset);

                    if old_version == new_version {
                        old_to_new[old_offset as usize] = Some(new_offset);
                        valid_points += 1;
                    } else {
                        // Version is different.
                        missing_points += 1;
                        if !healing_enabled {
                            return None;
                        }
                    }
                }
            }
        }

        // Loop 2: find points that once were in the old index, but deleted in
        // its id tracker.
        // Since loop 1 doesn't detect deleted points, we need to check them explicitly.
        for old_offset in 0..old_id_tracker.total_point_count() {
            let old_offset = old_offset as PointOffsetType;
            if old_id_tracker.is_deleted_point(old_offset) && old_graph_has_point(old_offset) {
                missing_points += 1;
                if !healing_enabled {
                    return None;
                }
            }
        }

        if valid_points == 0 {
            return None;
        }

        let missing_ratio = missing_points as f64 / (missing_points + valid_points) as f64;
        let do_heal = missing_ratio <= hnsw_global_config.healing_threshold;
        debug!(
            "valid points: {valid_points}, missing points: {missing_points}, missing ratio: {missing_ratio:.3}, do_heal: {do_heal}"
        );
        if !do_heal {
            return None;
        }

        drop(old_id_tracker);

        Some(OldIndexCandidate {
            index: old_index,
            old_to_new,
            valid_points,
            missing_points,
        })
    }

    fn reuse(self, total_vector_count: usize) -> OldIndex<'a> {
        let mut new_to_old = vec![None; total_vector_count];
        for (old_offset, new_offset) in self.old_to_new.iter().copied().enumerate() {
            if let Some(new_offset) = new_offset {
                new_to_old[new_offset as usize] = Some(old_offset as PointOffsetType);
            }
        }

        log::debug!(
            "Reusing {} points from the old index, healing {} points",
            self.valid_points,
            self.missing_points,
        );

        OldIndex {
            index: self.index,
            old_to_new: self.old_to_new,
            new_to_old,
        }
    }
}

impl OldIndex<'_> {
    fn point_level(&self, new_id: PointOffsetType) -> Option<usize> {
        let old_id = self.new_to_old[new_id as usize]?;
        Some(self.index.graph.links.point_level(old_id))
    }

    pub fn graph(&self) -> &GraphLayers {
        &self.index.graph
    }
}
