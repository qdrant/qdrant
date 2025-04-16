use std::fs::create_dir_all;
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use bitvec::prelude::BitSlice;
use bitvec::vec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(target_os = "linux")]
use common::cpu::linux_low_thread_priority;
use common::ext::BitSliceExt as _;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use itertools::EitherOrBoth;
use log::debug;
use memory::fadvise::clear_disk_cache;
use parking_lot::Mutex;
use rayon::ThreadPool;
use rayon::prelude::*;

#[cfg(feature = "gpu")]
use super::gpu::gpu_devices_manager::LockedGpuDevice;
use super::gpu::gpu_vector_storage::GpuVectorStorage;
use super::graph_links::GraphLinksFormat;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
use crate::id_tracker::IdTrackerSS;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
#[cfg(feature = "gpu")]
use crate::index::hnsw_index::gpu::{get_gpu_groups_count, gpu_graph_builder::build_hnsw_on_gpu};
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};
use crate::index::{PayloadIndex, VectorIndex, VectorIndexEnum};
#[cfg(feature = "gpu")]
use crate::payload_storage::FilterContext;
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::Condition::Field;
use crate::types::{
    FieldCondition, Filter, HnswConfig, QuantizationSearchParams, SearchParams,
    default_quantization_ignore_value, default_quantization_oversampling_value,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::query::DiscoveryQuery;
use crate::vector_storage::{RawScorer, VectorStorage, VectorStorageEnum, new_raw_scorer};

const HNSW_USE_HEURISTIC: bool = true;
const FINISH_MAIN_GRAPH_LOG_MESSAGE: &str = "Finish main graph in time";

/// Build first N points in HNSW graph using only a single thread, to avoid
/// disconnected components in the graph.
#[cfg(debug_assertions)]
pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 32;
#[cfg(not(debug_assertions))]
pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

const LINK_COMPRESSION_FORMAT: GraphLinksFormat = GraphLinksFormat::Compressed;
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

    pub fn build(
        open_args: HnswIndexOpenArgs<'_>,
        build_args: VectorIndexBuildArgs<'_>,
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
            stopped,
            feature_flags,
        } = build_args;

        create_dir_all(path)?;

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
                feature_flags.incremental_hnsw_building.then_some(())?;
                OldIndexCandidate::evaluate(
                    old_index,
                    &config,
                    &vector_storage_ref,
                    id_tracker_ref.deref(),
                )
            })
            .max_by_key(|old_index| old_index.valid_points);

        // Build main index graph
        let mut rng = rand::rng();
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
            config.m,
            config.m0,
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

        let old_index = old_index.map(|old_index| old_index.reuse(&config, total_vector_count));

        let mut indexed_vectors = 0;
        for vector_id in id_tracker_ref.iter_ids_excluding(deleted_bitslice) {
            check_process_stopped(stopped)?;
            indexed_vectors += 1;

            let level = old_index
                .as_ref()
                .and_then(|old_index| old_index.point_level(vector_id))
                .unwrap_or_else(|| graph_layers_builder.get_random_layer(&mut rng));
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
        #[cfg(not(feature = "gpu"))]
        let gpu_vectors = None;

        if build_main_graph {
            let timer = std::time::Instant::now();

            let mut ids = Vec::with_capacity(total_vector_count);
            let mut first_few_ids = Vec::with_capacity(SINGLE_THREADED_HNSW_BUILD_THRESHOLD);

            let mut ids_iter = id_tracker_ref.iter_ids_excluding(deleted_bitslice);
            if let Some(old_index) = old_index {
                for vector_id in ids_iter {
                    if let Some(links) = old_index.get_links(vector_id) {
                        graph_layers_builder.add_new_point(vector_id, links);
                    } else if first_few_ids.len() < SINGLE_THREADED_HNSW_BUILD_THRESHOLD {
                        first_few_ids.push(vector_id);
                    } else {
                        ids.push(vector_id);
                    }
                }
            } else {
                first_few_ids.extend(ids_iter.by_ref().take(SINGLE_THREADED_HNSW_BUILD_THRESHOLD));
                ids.extend(ids_iter);
            }

            let insert_point = |vector_id| {
                check_process_stopped(stopped)?;
                let vector = vector_storage_ref.get_vector(vector_id);
                let vector = vector.as_vec_ref().into();
                // No need to accumulate hardware, since this is an internal operation
                let internal_hardware_counter = HardwareCounterCell::disposable();

                let raw_scorer = if let Some(quantized_storage) = quantized_vectors_ref.as_ref() {
                    quantized_storage.raw_scorer(
                        vector,
                        id_tracker_ref.deleted_point_bitslice(),
                        vector_storage_ref.deleted_vector_bitslice(),
                        internal_hardware_counter,
                    )
                } else {
                    new_raw_scorer(
                        vector,
                        &vector_storage_ref,
                        id_tracker_ref.deleted_point_bitslice(),
                        internal_hardware_counter,
                    )
                }?;
                let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

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

        let visited_pool = VisitedPool::new();
        let mut block_filter_list = visited_pool.get(total_vector_count);

        let payload_m = config.payload_m.unwrap_or(config.m);

        if payload_m > 0 {
            // Calculate true average number of links per vertex in the HNSW graph
            // to better estimate percolation threshold
            let average_links_per_0_level =
                graph_layers_builder.get_average_connectivity_on_level(0);
            let average_links_per_0_level_int = (average_links_per_0_level as usize).max(1);

            let mut indexed_vectors_set = if config.m != 0 {
                // Every vector is already indexed in the main graph, so skip counting.
                BitVec::new()
            } else {
                BitVec::repeat(false, total_vector_count)
            };

            for (field, _) in payload_index_ref.indexed_fields() {
                debug!("building additional index for field {}", &field);

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
                    // ToDo: reuse graph layer for same payload
                    let mut additional_graph = GraphLayersBuilder::new_with_params(
                        total_vector_count,
                        payload_m,
                        config.payload_m0.unwrap_or(config.m0),
                        config.ef_construct,
                        1,
                        HNSW_USE_HEURISTIC,
                        false,
                    );
                    Self::build_filtered_graph(
                        id_tracker_ref.deref(),
                        &vector_storage_ref,
                        &quantized_vectors_ref,
                        gpu_vectors.as_ref(),
                        &payload_index_ref,
                        &pool,
                        stopped,
                        &mut additional_graph,
                        payload_block.condition,
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

        let graph: GraphLayers =
            graph_layers_builder.into_graph_layers(path, LINK_COMPRESSION_FORMAT, is_on_disk)?;

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

    #[allow(clippy::too_many_arguments)]
    #[allow(unused_variables)]
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn build_filtered_graph(
        id_tracker: &IdTrackerSS,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: &Option<QuantizedVectors>,
        #[allow(unused_variables)] gpu_vectors: Option<&GpuVectorStorage>,
        payload_index: &StructPayloadIndex,
        pool: &ThreadPool,
        stopped: &AtomicBool,
        graph_layers_builder: &mut GraphLayersBuilder,
        condition: FieldCondition,
        block_filter_list: &mut VisitedListHandle,
        indexed_vectors_set: &mut BitVec,
    ) -> OperationResult<()> {
        block_filter_list.next_iteration();

        let filter = Filter::new_must(Field(condition));

        let deleted_bitslice = vector_storage.deleted_vector_bitslice();

        let disposed_hw_counter = HardwareCounterCell::disposable(); // Internal operation. No measurements needed

        let cardinality_estimation =
            payload_index.estimate_cardinality(&filter, &disposed_hw_counter);

        let points_to_index: Vec<_> = payload_index
            .iter_filtered_points(
                &filter,
                id_tracker,
                &cardinality_estimation,
                &disposed_hw_counter,
            )
            .filter(|&point_id| !deleted_bitslice.get_bit(point_id as usize).unwrap_or(false))
            .collect();

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
            gpu_vectors,
            graph_layers_builder,
            block_filter_list,
            &points_to_index,
            deleted_bitslice,
            stopped,
        )? {
            *graph_layers_builder = gpu_constructed_graph;
            return Ok(());
        }

        let insert_points = |block_point_id| {
            check_process_stopped(stopped)?;

            let vector = vector_storage.get_vector(block_point_id);
            let vector = vector.as_vec_ref().into();

            // This hardware counter can be discarded, since it is only used for internal operations
            let internal_hardware_counter = HardwareCounterCell::disposable();

            let raw_scorer = match quantized_vectors.as_ref() {
                Some(quantized_storage) => quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_point_bitslice(),
                    deleted_bitslice,
                    internal_hardware_counter,
                ),
                None => new_raw_scorer(
                    vector,
                    vector_storage,
                    id_tracker.deleted_point_bitslice(),
                    internal_hardware_counter,
                ),
            }?;
            let block_condition_checker = BuildConditionChecker {
                filter_list: block_filter_list,
                current_point: block_point_id,
            };
            let points_scorer =
                FilteredScorer::new(raw_scorer.as_ref(), Some(&block_condition_checker));

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
            let vector = vector_storage.get_vector(vector_id);
            let vector = vector.as_vec_ref().into();
            let hardware_counter = HardwareCounterCell::disposable();
            let raw_scorer = if let Some(quantized_storage) = quantized_vectors.as_ref() {
                quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_point_bitslice(),
                    vector_storage.deleted_vector_bitslice(),
                    hardware_counter,
                )
            } else {
                new_raw_scorer(
                    vector,
                    vector_storage,
                    id_tracker.deleted_point_bitslice(),
                    hardware_counter,
                )
            }?;
            Ok((raw_scorer, None))
        };

        Self::build_graph_on_gpu(
            gpu_vectors,
            graph_layers_builder,
            id_tracker.iter_ids_excluding(deleted_bitslice),
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
        gpu_vectors: Option<&GpuVectorStorage>,
        graph_layers_builder: &GraphLayersBuilder,
        block_filter_list: &VisitedListHandle,
        points_to_index: &[PointOffsetType],
        deleted_bitslice: &BitSlice,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        let points_scorer_builder = |block_point_id| -> OperationResult<_> {
            let vector = vector_storage.get_vector(block_point_id);
            let vector = vector.as_vec_ref().into();
            let hardware_counter = HardwareCounterCell::disposable();
            let raw_scorer = match quantized_vectors.as_ref() {
                Some(quantized_storage) => quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_point_bitslice(),
                    deleted_bitslice,
                    hardware_counter,
                ),
                None => new_raw_scorer(
                    vector,
                    vector_storage,
                    id_tracker.deleted_point_bitslice(),
                    hardware_counter,
                ),
            }?;
            let block_condition_checker: Box<dyn FilterContext> = Box::new(BuildConditionChecker {
                filter_list: block_filter_list,
                current_point: block_point_id,
            });
            Ok((raw_scorer, Some(block_condition_checker)))
        };

        Self::build_graph_on_gpu(
            gpu_vectors,
            graph_layers_builder,
            points_to_index.iter().copied(),
            1,
            points_scorer_builder,
            stopped,
        )
    }

    #[cfg(feature = "gpu")]
    #[allow(clippy::too_many_arguments)]
    fn build_graph_on_gpu<'a>(
        gpu_vectors: Option<&GpuVectorStorage>,
        graph_layers_builder: &GraphLayersBuilder,
        points_to_index: impl Iterator<Item = PointOffsetType>,
        entry_points_num: usize,
        points_scorer_builder: impl Fn(
            PointOffsetType,
        ) -> OperationResult<(
            Box<dyn RawScorer + 'a>,
            Option<Box<dyn FilterContext + 'a>>,
        )> + Send
        + Sync,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<GraphLayersBuilder>> {
        if let Some(gpu_vectors) = gpu_vectors {
            let gpu_constructed_graph = build_hnsw_on_gpu(
                gpu_vectors,
                graph_layers_builder,
                get_gpu_groups_count(),
                entry_points_num,
                SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
                false,
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

        let raw_scorer = Self::construct_search_scorer(
            vector,
            &vector_storage,
            quantized_vectors.as_ref(),
            deleted_points,
            params,
            vector_query_context.hardware_counter(),
        )?;
        let oversampled_top = Self::get_oversampled_top(quantized_vectors.as_ref(), params, top);

        let hw_counter = vector_query_context.hardware_counter();

        let filter_context = filter.map(|f| payload_index.filter_context(f, &hw_counter));
        let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());

        let search_result = self.graph.search(
            oversampled_top,
            ef,
            points_scorer,
            custom_entry_points,
            &is_stopped,
        )?;

        let res = self.postprocess_search_result(
            search_result,
            vector,
            params,
            top,
            vector_query_context.hardware_counter(),
        )?;

        Ok(res)
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

        let raw_scorer = Self::construct_search_scorer(
            vector,
            &vector_storage,
            quantized_vectors.as_ref(),
            deleted_points,
            params,
            vector_query_context.hardware_counter(),
        )?;
        let oversampled_top = Self::get_oversampled_top(quantized_vectors.as_ref(), params, top);

        let search_result = raw_scorer.peek_top_iter(points, oversampled_top, &is_stopped)?;

        let res = self.postprocess_search_result(
            search_result,
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

    fn is_quantized_search(
        quantized_storage: Option<&QuantizedVectors>,
        params: Option<&SearchParams>,
    ) -> bool {
        let ignore_quantization = params
            .and_then(|p| p.quantization)
            .map(|q| q.ignore)
            .unwrap_or(default_quantization_ignore_value());
        quantized_storage.is_some() && !ignore_quantization
    }

    fn construct_search_scorer<'a>(
        vector: &QueryVector,
        vector_storage: &'a VectorStorageEnum,
        quantized_storage: Option<&'a QuantizedVectors>,
        deleted_points: &'a BitSlice,
        params: Option<&SearchParams>,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        let quantization_enabled = Self::is_quantized_search(quantized_storage, params);
        match quantized_storage {
            Some(quantized_storage) if quantization_enabled => quantized_storage.raw_scorer(
                vector.to_owned(),
                deleted_points,
                vector_storage.deleted_vector_bitslice(),
                hardware_counter,
            ),
            _ => new_raw_scorer(
                vector.to_owned(),
                vector_storage,
                deleted_points,
                hardware_counter,
            ),
        }
    }

    fn get_oversampled_top(
        quantized_storage: Option<&QuantizedVectors>,
        params: Option<&SearchParams>,
        top: usize,
    ) -> usize {
        let quantization_enabled = Self::is_quantized_search(quantized_storage, params);

        let oversampling_value = params
            .and_then(|p| p.quantization)
            .map(|q| q.oversampling)
            .unwrap_or(default_quantization_oversampling_value());

        match oversampling_value {
            Some(oversampling) if quantization_enabled && oversampling > 1.0 => {
                (oversampling * top as f64) as usize
            }
            _ => top,
        }
    }

    fn postprocess_search_result(
        &self,
        search_result: Vec<ScoredPointOffset>,
        vector: &QueryVector,
        params: Option<&SearchParams>,
        top: usize,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let quantized_vectors = self.quantized_vectors.borrow();

        let quantization_enabled = Self::is_quantized_search(quantized_vectors.as_ref(), params);

        let default_rescoring = quantized_vectors
            .as_ref()
            .map(|q| q.default_rescoring())
            .unwrap_or(false);
        let rescore = quantization_enabled
            && params
                .and_then(|p| p.quantization)
                .and_then(|q| q.rescore)
                .unwrap_or(default_rescoring);

        let mut postprocess_result = if rescore {
            let raw_scorer = new_raw_scorer(
                vector.to_owned(),
                &vector_storage,
                id_tracker.deleted_point_bitslice(),
                hardware_counter,
            )?;

            let mut ids_iterator = search_result.iter().map(|x| x.idx);
            let mut re_scored = raw_scorer.score_points_unfiltered(&mut ids_iterator);

            re_scored.sort_unstable();
            re_scored.reverse();
            re_scored
        } else {
            search_result
        };
        postprocess_result.truncate(top);
        Ok(postprocess_result)
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
}

struct OldIndex<'a> {
    index: AtomicRef<'a, HNSWIndex>,
    /// Mapping from old index to new index.
    /// `old_to_new[old_idx] == Some(new_idx)`.
    old_to_new: Vec<Option<PointOffsetType>>,
    /// Mapping from new index to old index.
    /// `new_to_old[new_idx] == Some(old_idx)`.
    new_to_old: Vec<Option<PointOffsetType>>,
    m: usize,
    m0: usize,
}

impl<'a> OldIndexCandidate<'a> {
    /// Evaluate whether we can use the old index.
    fn evaluate(
        old_index: &'a Arc<AtomicRefCell<VectorIndexEnum>>,
        config: &HnswGraphConfig,
        vector_storage: &VectorStorageEnum,
        id_tracker: &IdTrackerSS,
    ) -> Option<Self> {
        let old_index = AtomicRef::filter_map(old_index.borrow(), |index| match index {
            VectorIndexEnum::Hnsw(old_index) => Some(old_index),
            _ => None,
        })?;

        let no_main_graph = config.m == 0;
        let configuration_mismatch = config.m != old_index.config.m
            || config.m0 != old_index.config.m0
            || config.ef_construct != old_index.config.ef_construct;
        if no_main_graph || configuration_mismatch {
            return None;
        }

        let old_storage_ref = old_index.vector_storage.borrow();

        let new_deleted = vector_storage.deleted_vector_bitslice();
        let old_id_tracker = old_index.id_tracker.borrow();

        if old_id_tracker.deleted_point_count() != 0 {
            // Old index has deleted points.
            // FIXME: Not supported yet.
            return None;
        }

        // Build old_to_new mapping.
        let mut valid_points = 0;
        let mut old_to_new = vec![None; old_id_tracker.total_point_count()];
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

            // Even if the old vector is marked as deleted, we still might want
            // to reuse the graph built with this vector.
            // Thus, instead of checking `deleted_vector_bitslice`, we check
            // that the vector present in the graph, and it's value is the same.
            let old_offset =
                old_offset.filter(|&id| old_index.graph.links.links(id, 0).next().is_some());

            match (new_offset, old_offset) {
                (_, None) => (),
                (None, Some(_)) => {
                    // Vector was in the old index, but not in the new one.
                    // FIXME: Not supported yet.
                    return None;
                }
                (Some(new_offset), Some(old_offset)) => {
                    let new_vector = vector_storage.get_vector(new_offset);
                    let old_vector = old_storage_ref.get_vector(old_offset);
                    if old_vector == new_vector {
                        old_to_new[old_offset as usize] = Some(new_offset);
                        valid_points += 1;
                    } else {
                        // Vector is changed.
                        // FIXME: Not supported yet.
                        return None;
                    }
                }
            }
        }

        if valid_points == 0 {
            return None;
        }

        drop(old_id_tracker);
        drop(old_storage_ref);

        Some(OldIndexCandidate {
            index: old_index,
            old_to_new,
            valid_points,
        })
    }

    fn reuse(self, config: &HnswGraphConfig, total_vector_count: usize) -> OldIndex<'a> {
        let mut new_to_old = vec![None; total_vector_count];
        for (old_offset, new_offset) in self.old_to_new.iter().copied().enumerate() {
            if let Some(new_offset) = new_offset {
                new_to_old[new_offset as usize] = Some(old_offset as PointOffsetType);
            }
        }

        log::debug!("Reusing {} points from the old index", self.valid_points);

        OldIndex {
            index: self.index,
            old_to_new: self.old_to_new,
            new_to_old,
            m: config.m,
            m0: config.m0,
        }
    }
}

impl OldIndex<'_> {
    fn point_level(&self, new_id: PointOffsetType) -> Option<usize> {
        let old_id = self.new_to_old[new_id as usize]?;
        Some(self.index.graph.links.point_level(old_id))
    }

    fn get_links(&self, src_new: PointOffsetType) -> Option<Vec<Vec<PointOffsetType>>> {
        let src_old = self.new_to_old[src_new as usize]?;

        let links = &self.index.graph.links;
        let point_level = links.point_level(src_old);

        let links = (0..=point_level).map(|level| {
            links
                .links(src_old, level)
                .take(if level == 0 { self.m0 } else { self.m })
                .filter_map(|dst_old| self.old_to_new[dst_old as usize])
                .collect()
        });
        Some(links.collect())
    }
}
