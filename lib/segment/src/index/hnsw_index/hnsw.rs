use std::fs::create_dir_all;
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::BitSlice;
use bitvec::vec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(target_os = "linux")]
use common::cpu::linux_low_thread_priority;
use common::cpu::{get_num_cpus, CpuPermit};
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use log::debug;
use memory::mmap_ops;
use parking_lot::Mutex;
use rand::thread_rng;
use rayon::prelude::*;
use rayon::ThreadPool;

use super::gpu::gpu_devices_manager::LockedGpuDevice;
use super::gpu::gpu_vector_storage::GpuVectorStorage;
use super::graph_links::{GraphLinks, GraphLinksMmap};
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::common::BYTES_IN_KB;
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
use crate::index::{PayloadIndex, VectorIndex};
#[cfg(feature = "gpu")]
use crate::payload_storage::FilterContext;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::Condition::Field;
use crate::types::{
    default_quantization_ignore_value, default_quantization_oversampling_value, FieldCondition,
    Filter, HnswConfig, QuantizationSearchParams, SearchParams,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::query::DiscoveryQuery;
use crate::vector_storage::{
    new_raw_scorer, new_stoppable_raw_scorer, RawScorer, VectorStorage, VectorStorageEnum,
};

const HNSW_USE_HEURISTIC: bool = true;
const FINISH_MAIN_GRAPH_LOG_MESSAGE: &str = "Finish main graph in time";

/// Build first N points in HNSW graph using only a single thread, to avoid
/// disconnected components in the graph.
#[cfg(debug_assertions)]
const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 32;
#[cfg(not(debug_assertions))]
const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

#[derive(Debug)]
pub struct HNSWIndex<TGraphLinks: GraphLinks> {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    config: HnswGraphConfig,
    path: PathBuf,
    graph: GraphLayers<TGraphLinks>,
    searches_telemetry: HNSWSearchesTelemetry,
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

pub struct HnswIndexOpenArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub hnsw_config: HnswConfig,
    pub permit: Option<Arc<CpuPermit>>,
    pub gpu_device: Option<&'a LockedGpuDevice<'a>>,
    pub stopped: &'a AtomicBool,
}

impl<TGraphLinks: GraphLinks> HNSWIndex<TGraphLinks> {
    pub fn open(args: HnswIndexOpenArgs<'_>) -> OperationResult<Self> {
        let HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            hnsw_config,
            permit,
            gpu_device,
            stopped,
        } = args;

        create_dir_all(path)?;

        let config_path = HnswGraphConfig::get_config_path(path);
        let graph_path = GraphLayers::<TGraphLinks>::get_path(path);
        let graph_links_path = GraphLayers::<TGraphLinks>::get_links_path(path);
        let (config, graph) = if graph_path.exists() {
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

            (config, GraphLayers::load(&graph_path, &graph_links_path)?)
        } else {
            let num_cpus = match permit {
                Some(p) => p.num_cpus as usize,
                None => {
                    log::warn!("Rebuilding HNSW index");

                    // We have no CPU permit, meaning this call is not triggered by the segment
                    // optimizer which is supposed to be the only entity that builds an HNSW index.
                    // This should never be executed unless files are removed manually.
                    debug_assert!(false);

                    get_num_cpus()
                }
            };
            let (config, graph) = Self::build_index(
                path,
                id_tracker.as_ref().borrow().deref(),
                &vector_storage.borrow(),
                &quantized_vectors.borrow(),
                &payload_index.borrow(),
                &hnsw_config,
                num_cpus,
                gpu_device,
                stopped,
            )?;

            config.save(&config_path)?;
            graph.save(&graph_path)?;

            (config, graph)
        };

        Ok(HNSWIndex {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            config,
            path: path.to_owned(),
            graph,
            searches_telemetry: HNSWSearchesTelemetry {
                unfiltered_hnsw: OperationDurationsAggregator::new(),
                unfiltered_plain: OperationDurationsAggregator::new(),
                filtered_plain: OperationDurationsAggregator::new(),
                small_cardinality: OperationDurationsAggregator::new(),
                large_cardinality: OperationDurationsAggregator::new(),
                exact_filtered: OperationDurationsAggregator::new(),
                exact_unfiltered: OperationDurationsAggregator::new(),
            },
        })
    }

    #[cfg(test)]
    pub(super) fn graph(&self) -> &GraphLayers<TGraphLinks> {
        &self.graph
    }

    pub fn get_quantized_vectors(&self) -> Arc<AtomicRefCell<Option<QuantizedVectors>>> {
        self.quantized_vectors.clone()
    }

    #[allow(clippy::too_many_arguments)]
    fn build_index(
        path: &Path,
        id_tracker: &IdTrackerSS,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: &Option<QuantizedVectors>,
        payload_index: &StructPayloadIndex,
        hnsw_config: &HnswConfig,
        num_cpus: usize,
        #[allow(unused_variables)] gpu_device: Option<&LockedGpuDevice>,
        stopped: &AtomicBool,
    ) -> OperationResult<(HnswGraphConfig, GraphLayers<TGraphLinks>)> {
        let total_vector_count = vector_storage.total_vector_count();

        let full_scan_threshold = vector_storage
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

        // Build main index graph
        let mut rng = thread_rng();
        let deleted_bitslice = vector_storage.deleted_vector_bitslice();

        #[cfg(feature = "gpu")]
        let gpu_name_postfix = if let Some(gpu_device) = gpu_device {
            format!(" and GPU {}", gpu_device.device().name())
        } else {
            Default::default()
        };
        #[cfg(not(feature = "gpu"))]
        let gpu_name_postfix = "";
        debug!(
            "building HNSW for {total_vector_count} vectors with {num_cpus} CPUs{gpu_name_postfix}"
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
            .num_threads(num_cpus)
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

        let mut indexed_vectors = 0;
        for vector_id in id_tracker.iter_ids_excluding(deleted_bitslice) {
            check_process_stopped(stopped)?;
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(vector_id, level);
            indexed_vectors += 1;
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
            let gpu_vectors =
                Self::create_gpu_vectors(gpu_device, vector_storage, quantized_vectors, stopped)?;
            if let Some(gpu_constructed_graph) = Self::build_main_graph_on_gpu(
                id_tracker,
                vector_storage,
                quantized_vectors,
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

            let mut ids_iterator = id_tracker.iter_ids_excluding(deleted_bitslice);

            let first_few_ids: Vec<_> = ids_iterator
                .by_ref()
                .take(SINGLE_THREADED_HNSW_BUILD_THRESHOLD)
                .collect();
            let ids: Vec<_> = ids_iterator.collect();

            let insert_point = |vector_id| {
                check_process_stopped(stopped)?;
                let vector = vector_storage.get_vector(vector_id);
                let vector = vector.as_vec_ref().into();
                let raw_scorer = if let Some(quantized_storage) = quantized_vectors.as_ref() {
                    quantized_storage.raw_scorer(
                        vector,
                        id_tracker.deleted_point_bitslice(),
                        vector_storage.deleted_vector_bitslice(),
                        stopped,
                    )
                } else {
                    new_raw_scorer(vector, vector_storage, id_tracker.deleted_point_bitslice())
                }?;
                let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

                graph_layers_builder.link_new_point(vector_id, points_scorer);

                // Ignore hardware counter, for internal operations
                raw_scorer.take_hardware_counter().discard_results();

                Ok::<_, OperationError>(())
            };

            for vector_id in first_few_ids {
                insert_point(vector_id)?;
            }

            if !ids.is_empty() {
                pool.install(|| ids.into_par_iter().try_for_each(insert_point))?;
            }

            debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
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

            for (field, _) in payload_index.indexed_fields() {
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

                for payload_block in payload_index.payload_blocks(&field, full_scan_threshold) {
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
                        id_tracker,
                        vector_storage,
                        quantized_vectors,
                        gpu_vectors.as_ref(),
                        payload_index,
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

        let graph_links_path = GraphLayers::<TGraphLinks>::get_links_path(path);
        let graph: GraphLayers<TGraphLinks> =
            graph_layers_builder.into_graph_layers(Some(&graph_links_path))?;

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
        Ok((config, graph))
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

        let cardinality_estimation = payload_index.estimate_cardinality(&filter);

        let points_to_index: Vec<_> = payload_index
            .iter_filtered_points(&filter, id_tracker, &cardinality_estimation)
            .filter(|&point_id| {
                !deleted_bitslice
                    .get(point_id as usize)
                    .map(|x| *x)
                    .unwrap_or(false)
            })
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
            let raw_scorer = match quantized_vectors.as_ref() {
                Some(quantized_storage) => quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_point_bitslice(),
                    deleted_bitslice,
                    stopped,
                ),
                None => new_raw_scorer(vector, vector_storage, id_tracker.deleted_point_bitslice()),
            }?;
            let block_condition_checker = BuildConditionChecker {
                filter_list: block_filter_list,
                current_point: block_point_id,
            };
            let points_scorer =
                FilteredScorer::new(raw_scorer.as_ref(), Some(&block_condition_checker));

            graph_layers_builder.link_new_point(block_point_id, points_scorer);

            // Ignore hardware counter, for internal operations
            raw_scorer.take_hardware_counter().discard_results();

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
            let raw_scorer = if let Some(quantized_storage) = quantized_vectors.as_ref() {
                quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_point_bitslice(),
                    vector_storage.deleted_vector_bitslice(),
                    stopped,
                )
            } else {
                new_raw_scorer(vector, vector_storage, id_tracker.deleted_point_bitslice())
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
            let raw_scorer = match quantized_vectors.as_ref() {
                Some(quantized_storage) => quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_point_bitslice(),
                    deleted_bitslice,
                    stopped,
                ),
                None => new_raw_scorer(vector, vector_storage, id_tracker.deleted_point_bitslice()),
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
            )
                -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
            + Send
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
            &is_stopped,
        )?;
        let oversampled_top = Self::get_oversampled_top(quantized_vectors.as_ref(), params, top);

        let filter_context = filter.map(|f| payload_index.filter_context(f));
        let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());

        let search_result =
            self.graph
                .search(oversampled_top, ef, points_scorer, custom_entry_points);

        let hw_counter = HardwareCounterCell::new();
        let res = self.postprocess_search_result(
            search_result,
            vector,
            params,
            top,
            &is_stopped,
            &hw_counter,
        )?;

        vector_query_context.apply_hardware_counter(raw_scorer.take_hardware_counter());
        vector_query_context.apply_hardware_counter(hw_counter);
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
            &is_stopped,
        )?;
        let oversampled_top = Self::get_oversampled_top(quantized_vectors.as_ref(), params, top);

        let search_result = raw_scorer.peek_top_iter(points, oversampled_top);

        vector_query_context.apply_hardware_counter(raw_scorer.take_hardware_counter());

        let hw_counter = HardwareCounterCell::new();
        let res = self.postprocess_search_result(
            search_result,
            vector,
            params,
            top,
            &is_stopped,
            &hw_counter,
        )?;
        vector_query_context.apply_hardware_counter(hw_counter);
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
        let filtered_points = payload_index.query_points(filter);
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
        is_stopped: &'a AtomicBool,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        let quantization_enabled = Self::is_quantized_search(quantized_storage, params);
        match quantized_storage {
            Some(quantized_storage) if quantization_enabled => quantized_storage.raw_scorer(
                vector.to_owned(),
                deleted_points,
                vector_storage.deleted_vector_bitslice(),
                is_stopped,
            ),
            _ => new_stoppable_raw_scorer(
                vector.to_owned(),
                vector_storage,
                deleted_points,
                is_stopped,
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
        is_stopped: &AtomicBool,
        hardware_counter: &HardwareCounterCell,
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
            let raw_scorer = new_stoppable_raw_scorer(
                vector.to_owned(),
                &vector_storage,
                id_tracker.deleted_point_bitslice(),
                is_stopped,
            )?;

            let mut ids_iterator = search_result.iter().map(|x| x.idx);
            let mut re_scored = raw_scorer.score_points_unfiltered(&mut ids_iterator);

            hardware_counter.apply_from(raw_scorer.take_hardware_counter());

            re_scored.sort_unstable();
            re_scored.reverse();
            re_scored
        } else {
            search_result
        };
        postprocess_result.truncate(top);
        Ok(postprocess_result)
    }
}

impl HNSWIndex<GraphLinksMmap> {
    pub fn prefault_mmap_pages(&self) -> mmap_ops::PrefaultMmapPages {
        self.graph.prefault_mmap_pages(&self.path)
    }
}

impl<TGraphLinks: GraphLinks> VectorIndex for HNSWIndex<TGraphLinks> {
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
                let query_point_cardinality = payload_index.estimate_cardinality(query_filter);
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

                let filter_context = payload_index.filter_context(query_filter);

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
        [
            GraphLayers::<TGraphLinks>::get_path(&self.path),
            GraphLayers::<TGraphLinks>::get_links_path(&self.path),
            HnswGraphConfig::get_config_path(&self.path),
        ]
        .into_iter()
        .filter(|p| p.exists())
        .collect()
    }

    fn indexed_vector_count(&self) -> usize {
        self.config
            .indexed_vector_count
            // If indexed vector count is unknown, fall back to number of points
            .unwrap_or_else(|| self.graph.num_points())
    }

    fn update_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: Option<VectorRef>,
    ) -> OperationResult<()> {
        Err(OperationError::service_error("Cannot update HNSW index"))
    }
}
