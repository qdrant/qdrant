use std::cmp::max;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use log::debug;
use parking_lot::Mutex;
use rand::thread_rng;
use rayon::prelude::*;
use rayon::ThreadPool;

use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::visited_pool::VisitedList;
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::{
    TelemetryOperationAggregator, TelemetryOperationTimer, VectorIndexTelemetry,
};
use crate::types::Condition::Field;
use crate::types::{FieldCondition, Filter, HnswConfig, SearchParams, VECTOR_ELEMENT_SIZE};
use crate::vector_storage::{ScoredPointOffset, VectorStorageSS};

const HNSW_USE_HEURISTIC: bool = true;
const BYTES_IN_KB: usize = 1024;

pub struct HNSWIndex {
    vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    config: HnswGraphConfig,
    path: PathBuf,
    graph: GraphLayers,
    small_cardinality_search_telemetry: Arc<Mutex<TelemetryOperationAggregator>>,
    large_cardinality_search_telemetry: Arc<Mutex<TelemetryOperationAggregator>>,
    positive_check_cardinality_search_telemetry: Arc<Mutex<TelemetryOperationAggregator>>,
    negative_check_cardinality_search_telemetry: Arc<Mutex<TelemetryOperationAggregator>>,
}

impl HNSWIndex {
    pub fn open(
        path: &Path,
        vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
        hnsw_config: HnswConfig,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;

        let config_path = HnswGraphConfig::get_config_path(path);
        let config = if config_path.exists() {
            HnswGraphConfig::load(&config_path)?
        } else {
            let indexing_threshold = hnsw_config.full_scan_threshold.saturating_mul(BYTES_IN_KB)
                / (vector_storage.borrow().vector_dim() * VECTOR_ELEMENT_SIZE);

            HnswGraphConfig::new(
                hnsw_config.m,
                hnsw_config.ef_construct,
                indexing_threshold,
                hnsw_config.max_indexing_threads,
            )
        };

        let graph_path = GraphLayers::get_path(path);
        let graph = if graph_path.exists() {
            GraphLayers::load(&graph_path)?
        } else {
            let borrowed_vector_storage = vector_storage.borrow();
            let total_points = borrowed_vector_storage.total_vector_count();
            let vector_per_threshold = hnsw_config.full_scan_threshold.saturating_mul(BYTES_IN_KB)
                / (borrowed_vector_storage.vector_dim() * VECTOR_ELEMENT_SIZE);
            GraphLayers::new(
                borrowed_vector_storage.total_vector_count(),
                config.m,
                config.m0,
                config.ef_construct,
                max(1, total_points / vector_per_threshold * 10),
            )
        };

        Ok(HNSWIndex {
            vector_storage,
            payload_index,
            config,
            path: path.to_owned(),
            graph,
            small_cardinality_search_telemetry: TelemetryOperationAggregator::new(),
            large_cardinality_search_telemetry: TelemetryOperationAggregator::new(),
            positive_check_cardinality_search_telemetry: TelemetryOperationAggregator::new(),
            negative_check_cardinality_search_telemetry: TelemetryOperationAggregator::new(),
        })
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = HnswGraphConfig::get_config_path(&self.path);
        self.config.save(&config_path)
    }

    fn save_graph(&self) -> OperationResult<()> {
        let graph_path = GraphLayers::get_path(&self.path);
        self.graph.save(&graph_path)
    }

    pub fn save(&self) -> OperationResult<()> {
        self.save_config()?;
        self.save_graph()?;
        Ok(())
    }

    pub fn build_filtered_graph(
        &self,
        pool: &ThreadPool,
        stopped: &AtomicBool,
        graph_layers_builder: &mut GraphLayersBuilder,
        condition: FieldCondition,
        block_filter_list: &mut VisitedList,
    ) -> OperationResult<()> {
        block_filter_list.next_iteration();

        let filter = Filter::new_must(Field(condition));

        let payload_index = self.payload_index.borrow();
        let vector_storage = self.vector_storage.borrow();

        let points_to_index: Vec<_> = payload_index.query_points(&filter).collect();

        for block_point_id in points_to_index.iter().copied() {
            block_filter_list.check_and_update_visited(block_point_id);
        }

        for block_point_id in points_to_index.iter().copied() {
            // Use same levels, as in the original graph
            let level = self.graph.point_level(block_point_id);
            graph_layers_builder.set_levels(block_point_id, level);
        }

        pool.install(|| {
            points_to_index
                .into_par_iter()
                .try_for_each(|block_point_id| {
                    if stopped.load(Ordering::Relaxed) {
                        return Err(OperationError::Cancelled {
                            description: "Cancelled by external thread".to_string(),
                        });
                    }
                    let vector = vector_storage.get_vector(block_point_id).unwrap();
                    let raw_scorer = vector_storage.raw_scorer(vector);
                    let block_condition_checker = BuildConditionChecker {
                        filter_list: block_filter_list,
                        current_point: block_point_id,
                    };
                    let points_scorer =
                        FilteredScorer::new(raw_scorer.as_ref(), Some(&block_condition_checker));

                    graph_layers_builder.link_new_point(block_point_id, points_scorer);
                    Ok(())
                })
        })
    }

    pub fn search_with_graph(
        &self,
        vector: &[VectorElementType],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset> {
        let req_ef = params
            .and_then(|params| params.hnsw_ef)
            .unwrap_or(self.config.ef);

        // ef should always be bigger that required top
        let ef = max(req_ef, top);

        let vector_storage = self.vector_storage.borrow();
        let raw_scorer = vector_storage.raw_scorer(vector.to_owned());
        let payload_index = self.payload_index.borrow();

        let filter_context = filter.map(|f| payload_index.filter_context(f));

        let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());

        self.graph.search(top, ef, points_scorer)
    }

    fn search_vectors_with_graph(
        &self,
        vectors: &[&[VectorElementType]],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<Vec<ScoredPointOffset>> {
        vectors
            .iter()
            .map(|vector| self.search_with_graph(vector, filter, top, params))
            .collect()
    }
}

impl VectorIndex for HNSWIndex {
    fn search(
        &self,
        vectors: &[&[VectorElementType]],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<Vec<ScoredPointOffset>> {
        let exact = params.map(|params| params.exact).unwrap_or(false);
        match filter {
            None => {
                if exact {
                    let vector_storage = self.vector_storage.borrow();
                    vectors
                        .iter()
                        .map(|vector| vector_storage.score_all(vector, top))
                        .collect()
                } else {
                    self.search_vectors_with_graph(vectors, None, top, params)
                }
            }
            Some(query_filter) => {
                // depending on the amount of filtered-out points the optimal strategy could be
                // - to retrieve possible points and score them after
                // - to use HNSW index with filtering condition

                let payload_index = self.payload_index.borrow();
                let vector_storage = self.vector_storage.borrow();

                let plain_search = || -> Vec<Vec<ScoredPointOffset>> {
                    let mut filtered_iter = payload_index.query_points(query_filter);
                    return vectors
                        .iter()
                        .map(|vector| {
                            vector_storage.score_points(vector, filtered_iter.as_mut(), top)
                        })
                        .collect();
                };

                // if exact search is requested, we should not use HNSW index
                if exact {
                    return plain_search();
                }

                let query_cardinality = payload_index.estimate_cardinality(query_filter);

                // debug!("query_cardinality: {:#?}", query_cardinality);

                if query_cardinality.max < self.config.indexing_threshold {
                    // if cardinality is small - use plain index
                    let _timer =
                        TelemetryOperationTimer::new(&self.small_cardinality_search_telemetry);
                    return plain_search();
                }

                if query_cardinality.min > self.config.indexing_threshold {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        TelemetryOperationTimer::new(&self.large_cardinality_search_telemetry);
                    return self.search_vectors_with_graph(vectors, filter, top, params);
                }

                let filter_context = payload_index.filter_context(query_filter);

                // Fast cardinality estimation is not enough, do sample estimation of cardinality

                if sample_check_cardinality(
                    vector_storage.sample_ids(),
                    |idx| filter_context.check(idx),
                    self.config.indexing_threshold,
                    vector_storage.vector_count(),
                ) {
                    // if cardinality is high enough - use HNSW index
                    let _timer = TelemetryOperationTimer::new(
                        &self.positive_check_cardinality_search_telemetry,
                    );
                    self.search_vectors_with_graph(vectors, filter, top, params)
                } else {
                    // if cardinality is small - use plain index
                    let _timer = TelemetryOperationTimer::new(
                        &self.negative_check_cardinality_search_telemetry,
                    );
                    plain_search()
                }
            }
        }
    }

    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()> {
        // Build main index graph
        let vector_storage = self.vector_storage.borrow();
        let mut rng = thread_rng();

        let total_points = vector_storage.total_vector_count();

        debug!("building hnsw for {}", total_points);
        let mut graph_layers_builder = GraphLayersBuilder::new(
            total_points,
            self.config.m,
            self.config.m0,
            self.config.ef_construct,
            max(1, total_points / self.config.indexing_threshold * 10),
            HNSW_USE_HEURISTIC,
        );

        let mut ids = vec![];
        for vector_id in vector_storage.iter_ids() {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(vector_id, level);
            ids.push(vector_id);
        }

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.config.max_rayon_threads())
            .build()?;

        pool.install(|| {
            ids.into_par_iter().try_for_each(|vector_id| {
                if stopped.load(Ordering::Relaxed) {
                    return Err(OperationError::Cancelled {
                        description: "Cancelled by external thread".to_string(),
                    });
                }
                let vector = vector_storage.get_vector(vector_id).unwrap();
                let raw_scorer = vector_storage.raw_scorer(vector);
                let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

                graph_layers_builder.link_new_point(vector_id, points_scorer);
                Ok(())
            })
        })?;

        self.graph = graph_layers_builder.into_graph_layers();

        debug!("finish main graph");

        let total_vectors_count = vector_storage.total_vector_count();
        let mut block_filter_list = VisitedList::new(total_vectors_count);

        let payload_index = self.payload_index.borrow();

        for (field, _) in payload_index.indexed_fields() {
            debug!("building additional index for field {}", &field);

            // It is expected, that graph will become disconnected less than
            // $1/m$ points left.
            // So blocks larger than $1/m$ are not needed.
            // We add multiplier for the extra safety.
            let percolation_multiplier = 2;
            let max_block_size = total_points / self.config.m * percolation_multiplier;
            let min_block_size = self.config.indexing_threshold;

            for payload_block in payload_index.payload_blocks(&field, min_block_size) {
                if stopped.load(Ordering::Relaxed) {
                    return Err(OperationError::Cancelled {
                        description: "Cancelled by external thread".to_string(),
                    });
                }
                if payload_block.cardinality > max_block_size {
                    continue;
                }
                // ToDo: re-use graph layer for same payload
                let mut additional_graph = GraphLayersBuilder::new_with_params(
                    self.vector_storage.borrow().total_vector_count(),
                    self.config.m,
                    self.config.m0,
                    self.config.ef_construct,
                    1,
                    HNSW_USE_HEURISTIC,
                    false,
                );
                self.build_filtered_graph(
                    &pool,
                    stopped,
                    &mut additional_graph,
                    payload_block.condition,
                    &mut block_filter_list,
                )?;
                self.graph
                    .merge_from_other(additional_graph.into_graph_layers());
            }
        }
        debug!("finish additional payload field indexing");
        self.save()
    }

    fn get_telemetry_data(&self) -> VectorIndexTelemetry {
        VectorIndexTelemetry {
            small_cardinality_searches: self
                .small_cardinality_search_telemetry
                .lock()
                .get_statistics(),
            large_cardinality_searches: self
                .large_cardinality_search_telemetry
                .lock()
                .get_statistics(),
            positive_check_cardinality_searches: self
                .positive_check_cardinality_search_telemetry
                .lock()
                .get_statistics(),
            negative_check_cardinality_searches: self
                .negative_check_cardinality_search_telemetry
                .lock()
                .get_statistics(),
        }
    }
}
