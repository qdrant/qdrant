use std::cmp::max;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use log::debug;
use parking_lot::Mutex;
use rand::thread_rng;
use rayon::prelude::*;
use rayon::ThreadPool;

use super::graph_links::GraphLinks;
use crate::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{check_process_stopped, OperationError, OperationResult};
use crate::id_tracker::IdTrackerSS;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::visited_pool::VisitedList;
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::Condition::Field;
use crate::types::{
    default_quantization_ignore_value, default_quantization_rescore_value, FieldCondition, Filter,
    HnswConfig, QuantizationSearchParams, SearchParams, VECTOR_ELEMENT_SIZE,
};
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectors;
use crate::vector_storage::{new_raw_scorer, ScoredPointOffset, VectorStorage, VectorStorageEnum};

const HNSW_USE_HEURISTIC: bool = true;
const BYTES_IN_KB: usize = 1024;

pub struct HNSWIndex<TGraphLinks: GraphLinks> {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    config: HnswGraphConfig,
    path: PathBuf,
    graph: Option<GraphLayers<TGraphLinks>>,
    searches_telemetry: SearchesTelemetry,
}

struct SearchesTelemetry {
    unfiltered: Arc<Mutex<OperationDurationsAggregator>>,
    small_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
    large_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
    exact_filtered: Arc<Mutex<OperationDurationsAggregator>>,
    exact_unfiltered: Arc<Mutex<OperationDurationsAggregator>>,
}

impl<TGraphLinks: GraphLinks> HNSWIndex<TGraphLinks> {
    pub fn open(
        path: &Path,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
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
                hnsw_config.payload_m,
            )
        };

        let graph_path = GraphLayers::<TGraphLinks>::get_path(path);
        let graph_links_path = GraphLayers::<TGraphLinks>::get_links_path(path);
        let graph = if graph_path.exists() {
            Some(GraphLayers::load(&graph_path, &graph_links_path)?)
        } else {
            None
        };

        Ok(HNSWIndex {
            id_tracker,
            vector_storage,
            payload_index,
            config,
            path: path.to_owned(),
            graph,
            searches_telemetry: SearchesTelemetry {
                unfiltered: OperationDurationsAggregator::new(),
                small_cardinality: OperationDurationsAggregator::new(),
                large_cardinality: OperationDurationsAggregator::new(),
                exact_filtered: OperationDurationsAggregator::new(),
                exact_unfiltered: OperationDurationsAggregator::new(),
            },
        })
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = HnswGraphConfig::get_config_path(&self.path);
        self.config.save(&config_path)
    }

    fn save_graph(&self) -> OperationResult<()> {
        let graph_path = GraphLayers::<TGraphLinks>::get_path(&self.path);
        if let Some(graph) = &self.graph {
            graph.save(&graph_path)
        } else {
            Ok(())
        }
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
        let id_tracker = self.id_tracker.borrow();

        let points_to_index: Vec<_> = payload_index.query_points(&filter).collect();

        for block_point_id in points_to_index.iter().copied() {
            block_filter_list.check_and_update_visited(block_point_id);
        }

        if let Some(graph) = &self.graph {
            for &block_point_id in &points_to_index {
                // Use same levels, as in the original graph
                let level = graph.point_level(block_point_id);
                graph_layers_builder.set_levels(block_point_id, level);
            }
        }

        pool.install(|| {
            points_to_index
                .into_par_iter()
                .try_for_each(|block_point_id| {
                    check_process_stopped(stopped)?;

                    let vector = vector_storage.get_vector(block_point_id).to_vec();
                    let raw_scorer =
                        if let Some(quantized_storage) = vector_storage.quantized_storage() {
                            quantized_storage.raw_scorer(
                                &vector,
                                id_tracker.deleted_bitslice(),
                                vector_storage.deleted_bitslice(),
                            )
                        } else {
                            new_raw_scorer(
                                vector.to_owned(),
                                &vector_storage,
                                id_tracker.deleted_bitslice(),
                            )
                        };
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
        let id_tracker = self.id_tracker.borrow();
        let ignore_quantization = params
            .and_then(|p| p.quantization)
            .map(|q| q.ignore)
            .unwrap_or(default_quantization_ignore_value());

        let (raw_scorer, quantized) = if ignore_quantization {
            (
                new_raw_scorer(
                    vector.to_owned(),
                    &vector_storage,
                    id_tracker.deleted_bitslice(),
                ),
                false,
            )
        } else if let Some(quantized_storage) = vector_storage.quantized_storage() {
            (
                quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_bitslice(),
                    vector_storage.deleted_bitslice(),
                ),
                true,
            )
        } else {
            (
                new_raw_scorer(
                    vector.to_owned(),
                    &vector_storage,
                    id_tracker.deleted_bitslice(),
                ),
                false,
            )
        };
        let payload_index = self.payload_index.borrow();

        let filter_context = filter.map(|f| payload_index.filter_context(f));

        let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());

        if let Some(graph) = &self.graph {
            let mut search_result = graph.search(top, ef, points_scorer);
            let if_rescore = params
                .and_then(|p| p.quantization)
                .map(|q| q.rescore)
                .unwrap_or(default_quantization_rescore_value());
            if quantized && if_rescore {
                let raw_scorer = new_raw_scorer(
                    vector.to_owned(),
                    &vector_storage,
                    id_tracker.deleted_bitslice(),
                );
                search_result.iter_mut().for_each(|scored_point| {
                    scored_point.score = raw_scorer.score_point(scored_point.idx);
                });
                search_result
            } else {
                search_result
            }
        } else {
            Vec::new()
        }
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

    fn search_vectors_plain(
        &self,
        vectors: &[&[VectorElementType]],
        filter: &Filter,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<Vec<ScoredPointOffset>> {
        let payload_index = self.payload_index.borrow();
        let vector_storage = self.vector_storage.borrow();
        let id_tracker = self.id_tracker.borrow();
        let mut filtered_iter = payload_index.query_points(filter);
        let ignore_quantization = params
            .and_then(|p| p.quantization)
            .map(|q| q.ignore)
            .unwrap_or(default_quantization_ignore_value());
        if ignore_quantization {
            vectors
                .iter()
                .map(|vector| {
                    new_raw_scorer(
                        vector.to_vec(),
                        &vector_storage,
                        id_tracker.deleted_bitslice(),
                    )
                    .peek_top_iter(filtered_iter.as_mut(), top)
                })
                .collect()
        } else {
            vectors
                .iter()
                .map(|vector| {
                    if let Some(quantized_storage) = vector_storage.quantized_storage() {
                        quantized_storage
                            .raw_scorer(
                                vector,
                                id_tracker.deleted_bitslice(),
                                vector_storage.deleted_bitslice(),
                            )
                            .peek_top_iter(filtered_iter.as_mut(), top)
                    } else {
                        new_raw_scorer(
                            vector.to_vec(),
                            &vector_storage,
                            id_tracker.deleted_bitslice(),
                        )
                        .peek_top_iter(filtered_iter.as_mut(), top)
                    }
                })
                .collect()
        }
    }
}

impl<TGraphLinks: GraphLinks> VectorIndex for HNSWIndex<TGraphLinks> {
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
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.exact_unfiltered);
                    let vector_storage = self.vector_storage.borrow();
                    let id_tracker = self.id_tracker.borrow();
                    vectors
                        .iter()
                        .map(|vector| {
                            new_raw_scorer(
                                vector.to_vec(),
                                &vector_storage,
                                id_tracker.deleted_bitslice(),
                            )
                            .peek_top_all(top)
                        })
                        .collect()
                } else {
                    let _timer = ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered);
                    self.search_vectors_with_graph(vectors, None, top, params)
                }
            }
            Some(query_filter) => {
                // depending on the amount of filtered-out points the optimal strategy could be
                // - to retrieve possible points and score them after
                // - to use HNSW index with filtering condition

                // if exact search is requested, we should not use HNSW index
                if exact {
                    let exact_params = params.map(|params| {
                        let mut params = *params;
                        params.quantization = Some(QuantizationSearchParams {
                            ignore: true,
                            rescore: false,
                        }); // disable quantization for exact search
                        params
                    });
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.exact_filtered);
                    return self.search_vectors_plain(
                        vectors,
                        query_filter,
                        top,
                        exact_params.as_ref(),
                    );
                }

                let payload_index = self.payload_index.borrow();
                let query_cardinality = payload_index.estimate_cardinality(query_filter);

                // debug!("query_cardinality: {:#?}", query_cardinality);

                if query_cardinality.max < self.config.indexing_threshold {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    return self.search_vectors_plain(vectors, query_filter, top, params);
                }

                if query_cardinality.min > self.config.indexing_threshold {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.large_cardinality);
                    return self.search_vectors_with_graph(vectors, filter, top, params);
                }

                let filter_context = payload_index.filter_context(query_filter);

                // Fast cardinality estimation is not enough, do sample estimation of cardinality
                let id_tracker = self.id_tracker.borrow();
                if sample_check_cardinality(
                    id_tracker.sample_ids(),
                    |idx| filter_context.check(idx),
                    self.config.indexing_threshold,
                    id_tracker.points_count(),
                ) {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.large_cardinality);
                    self.search_vectors_with_graph(vectors, filter, top, params)
                } else {
                    // if cardinality is small - use plain index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    self.search_vectors_plain(vectors, query_filter, top, params)
                }
            }
        }
    }

    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()> {
        // Build main index graph
        let vector_storage = self.vector_storage.borrow();
        let id_tracker = self.id_tracker.borrow();
        let mut rng = thread_rng();

        let total_points = vector_storage.total_vector_count();

        debug!("building hnsw for {}", total_points);
        let mut graph_layers_builder = GraphLayersBuilder::new(
            total_points,
            self.config.m,
            self.config.m0,
            self.config.ef_construct,
            max(
                1,
                total_points
                    .checked_div(self.config.indexing_threshold)
                    .unwrap_or(0)
                    * 10,
            ),
            HNSW_USE_HEURISTIC,
        );

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(self.config.max_rayon_threads())
            .build()?;

        for vector_id in id_tracker.iter_ids() {
            check_process_stopped(stopped)?;
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(vector_id, level);
        }

        if self.config.m > 0 {
            let ids: Vec<_> = id_tracker.iter_ids().collect();

            pool.install(|| {
                ids.into_par_iter().try_for_each(|vector_id| {
                    check_process_stopped(stopped)?;
                    let vector = vector_storage.get_vector(vector_id).to_vec();
                    let raw_scorer =
                        if let Some(quantized_storage) = vector_storage.quantized_storage() {
                            quantized_storage.raw_scorer(
                                &vector,
                                id_tracker.deleted_bitslice(),
                                vector_storage.deleted_bitslice(),
                            )
                        } else {
                            new_raw_scorer(
                                vector.to_owned(),
                                &vector_storage,
                                id_tracker.deleted_bitslice(),
                            )
                        };
                    let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

                    graph_layers_builder.link_new_point(vector_id, points_scorer);
                    Ok::<_, OperationError>(())
                })
            })?;

            debug!("finish main graph");
        } else {
            debug!("skip building main HNSW graph");
        }

        let total_vectors_count = vector_storage.total_vector_count();
        let mut block_filter_list = VisitedList::new(total_vectors_count);

        let payload_index = self.payload_index.borrow();

        let payload_m = self.config.payload_m.unwrap_or(self.config.m);

        if payload_m > 0 {
            for (field, _) in payload_index.indexed_fields() {
                debug!("building additional index for field {}", &field);

                // It is expected, that graph will become disconnected less than
                // $1/m$ points left.
                // So blocks larger than $1/m$ are not needed.
                // We add multiplier for the extra safety.
                let percolation_multiplier = 2;
                let max_block_size = if self.config.m > 0 {
                    total_points / self.config.m * percolation_multiplier
                } else {
                    usize::MAX
                };
                let min_block_size = self.config.indexing_threshold;

                for payload_block in payload_index.payload_blocks(&field, min_block_size) {
                    check_process_stopped(stopped)?;
                    if payload_block.cardinality > max_block_size {
                        continue;
                    }
                    // ToDo: re-use graph layer for same payload
                    let mut additional_graph = GraphLayersBuilder::new_with_params(
                        self.vector_storage.borrow().total_vector_count(),
                        payload_m,
                        self.config.payload_m0.unwrap_or(self.config.m0),
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
                    graph_layers_builder.merge_from_other(additional_graph);
                }
            }
        } else {
            debug!("skip building additional HNSW links");
        }

        let graph_links_path = GraphLayers::<TGraphLinks>::get_links_path(&self.path);
        self.graph = Some(graph_layers_builder.into_graph_layers(Some(&graph_links_path))?);

        debug!("finish additional payload field indexing");
        self.save()
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        let tm = &self.searches_telemetry;

        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: Default::default(),
            filtered_plain: Default::default(),
            unfiltered_hnsw: tm.unfiltered.lock().get_statistics(),
            filtered_small_cardinality: tm.small_cardinality.lock().get_statistics(),
            filtered_large_cardinality: tm.large_cardinality.lock().get_statistics(),
            filtered_exact: tm.exact_filtered.lock().get_statistics(),
            unfiltered_exact: tm.exact_unfiltered.lock().get_statistics(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        if self.graph.is_some() {
            vec![
                GraphLayers::<TGraphLinks>::get_path(&self.path),
                GraphLayers::<TGraphLinks>::get_links_path(&self.path),
            ]
        } else {
            vec![]
        }
    }
}
