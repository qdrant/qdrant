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

use super::graph_links::{GraphLinks, GraphLinksMmap};
use crate::common::mmap_ops;
use crate::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{check_process_stopped, OperationResult};
use crate::id_tracker::IdTrackerSS;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_linear_builder::GraphLinearBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::visited_pool::VisitedList;
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::Condition::Field;
#[cfg(debug_assertions)]
use crate::types::PointOffsetType;
use crate::types::{
    default_quantization_ignore_value, FieldCondition, Filter, HnswConfig,
    QuantizationSearchParams, SearchParams, VECTOR_ELEMENT_SIZE,
};
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
    unfiltered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_hnsw: Arc<Mutex<OperationDurationsAggregator>>,
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
            let vector_storage = vector_storage.borrow();
            let available_vectors = vector_storage.available_vector_count();
            let full_scan_threshold = hnsw_config.full_scan_threshold.saturating_mul(BYTES_IN_KB)
                / (vector_storage.vector_dim() * VECTOR_ELEMENT_SIZE);

            HnswGraphConfig::new(
                hnsw_config.m,
                hnsw_config.ef_construct,
                full_scan_threshold,
                hnsw_config.max_indexing_threads,
                hnsw_config.payload_m,
                available_vectors,
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
                unfiltered_hnsw: OperationDurationsAggregator::new(),
                unfiltered_plain: OperationDurationsAggregator::new(),
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

        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();
        let vector_storage = self.vector_storage.borrow();

        let deleted_bitslice = vector_storage.deleted_vector_bitslice();

        let points_to_index: Vec<_> = payload_index
            .query_points(&filter)
            .into_iter()
            .filter(|&point_id| {
                !deleted_bitslice
                    .get(point_id as usize)
                    .map(|x| *x)
                    .unwrap_or(false)
            })
            .collect();

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
                                id_tracker.deleted_point_bitslice(),
                                deleted_bitslice,
                            )
                        } else {
                            new_raw_scorer(
                                vector.to_owned(),
                                &vector_storage,
                                id_tracker.deleted_point_bitslice(),
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
        let ef = params
            .and_then(|params| params.hnsw_ef)
            .unwrap_or(self.config.ef);

        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let quantized_storage = vector_storage.quantized_storage();

        // Check that:
        // - `params` is `Some`
        // - `params.quantization` is `Some`
        // - and `params.quantization.ignore` is `false`
        let quantization_params = params.and_then(|p| p.quantization).unwrap_or_default();

        let (raw_scorer, quantized) = match quantized_storage {
            // If `quantization_params` is `Some`, then quantization is *not* ignored
            Some(quantized_storage) if !quantization_params.ignore => {
                let scorer = quantized_storage.raw_scorer(
                    vector,
                    id_tracker.deleted_point_bitslice(),
                    vector_storage.deleted_vector_bitslice(),
                );

                (scorer, true)
            }

            _ => {
                let scorer = new_raw_scorer(
                    vector.to_owned(),
                    &vector_storage,
                    id_tracker.deleted_point_bitslice(),
                );

                (scorer, false)
            }
        };

        let payload_index = self.payload_index.borrow();
        let filter_context = filter.map(|f| payload_index.filter_context(f));
        let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());

        let Some(graph) = &self.graph else {
            return Vec::new();
        };

        if quantized && quantization_params.rescore {
            let oversampling = quantization_params.oversampling.unwrap_or(1.0);

            let oversampled_top = if oversampling > 1.0 {
                (oversampling * top as f64) as usize
            } else {
                // Very unlikely this is reached because validation enforces oversampling >= 1.0
                top
            };

            let search_result = graph.search(oversampled_top, ef, points_scorer);

            let raw_scorer = new_raw_scorer(
                vector.to_owned(),
                &vector_storage,
                id_tracker.deleted_point_bitslice(),
            );

            let mut ids_iterator = search_result.iter().map(|x| x.idx);
            let mut re_scored = raw_scorer.score_points_unfiltered(&mut ids_iterator);

            re_scored.sort_unstable();
            re_scored.truncate(top);
            re_scored
        } else {
            graph.search(top, ef, points_scorer)
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
        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();
        let vector_storage = self.vector_storage.borrow();
        let filtered_points = payload_index.query_points(filter);
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
                        id_tracker.deleted_point_bitslice(),
                    )
                    .peek_top_iter(&mut filtered_points.iter().copied(), top)
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
                                id_tracker.deleted_point_bitslice(),
                                vector_storage.deleted_vector_bitslice(),
                            )
                            .peek_top_iter(&mut filtered_points.iter().copied(), top)
                    } else {
                        new_raw_scorer(
                            vector.to_vec(),
                            &vector_storage,
                            id_tracker.deleted_point_bitslice(),
                        )
                        .peek_top_iter(&mut filtered_points.iter().copied(), top)
                    }
                })
                .collect()
        }
    }
}

impl HNSWIndex<GraphLinksMmap> {
    pub fn prefault_mmap_pages(&self) -> Option<mmap_ops::PrefaultMmapPages> {
        self.graph.as_ref()?.prefault_mmap_pages(&self.path)
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
                let id_tracker = self.id_tracker.borrow();
                let vector_storage = self.vector_storage.borrow();

                // Determine whether to do a plain or graph search, and pick search timer aggregator
                // Because an HNSW graph is built, we'd normally always assume to search the graph.
                // But because a lot of points may be deleted in this graph, it may just be faster
                // to do a plain search instead.
                let plain_search = exact
                    || vector_storage.available_vector_count() < self.config.full_scan_threshold;

                // Do plain or graph search
                if plain_search {
                    let _timer = ScopeDurationMeasurer::new(if exact {
                        &self.searches_telemetry.exact_unfiltered
                    } else {
                        &self.searches_telemetry.unfiltered_plain
                    });
                    vectors
                        .iter()
                        .map(|vector| {
                            new_raw_scorer(
                                vector.to_vec(),
                                &vector_storage,
                                id_tracker.deleted_point_bitslice(),
                            )
                            .peek_top_all(top)
                        })
                        .collect()
                } else {
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_hnsw);
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
                            oversampling: None,
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
                    return self.search_vectors_plain(vectors, query_filter, top, params);
                }

                if query_cardinality.min > self.config.full_scan_threshold {
                    // if cardinality is high enough - use HNSW index
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.large_cardinality);
                    return self.search_vectors_with_graph(vectors, filter, top, params);
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

    fn build_index(&mut self, _stopped: &AtomicBool) -> OperationResult<()> {
        // Build main index graph
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let mut rng = thread_rng();

        let total_vector_count = vector_storage.total_vector_count();
        let deleted_bitslice = vector_storage.deleted_vector_bitslice();

        debug!("building HNSW for {} vectors", total_vector_count);
        let indexing_threshold = self.config.full_scan_threshold;

        let vector = vector_storage.get_vector(0).to_vec();
        let raw_scorer =
            if let Some(quantized_storage) = vector_storage.quantized_storage() {
                quantized_storage.raw_scorer(
                    &vector,
                    id_tracker.deleted_point_bitslice(),
                    vector_storage.deleted_vector_bitslice(),
                )
            } else {
                new_raw_scorer(
                    vector.to_owned(),
                    &vector_storage,
                    id_tracker.deleted_point_bitslice(),
                )
            };
        let mut graph_layers_builder = GraphLinearBuilder::new(
            total_vector_count,
            self.config.m,
            self.config.m0,
            self.config.ef_construct,
            (total_vector_count
                .checked_div(indexing_threshold)
                .unwrap_or(0)
                * 10)
                .max(1),
            raw_scorer,
            &mut rng,
        );

        let mut indexed_vectors = 0;

        if self.config.m > 0 {
            let ids: Vec<_> = id_tracker.iter_ids_excluding(deleted_bitslice).collect();
            indexed_vectors = ids.len();

            graph_layers_builder.build();

            debug!("finish main graph");
        } else {
            debug!("skip building main HNSW graph");
        }

        let payload_index = self.payload_index.borrow();
        let payload_m = self.config.payload_m.unwrap_or(self.config.m);

        if payload_m > 0 {
            if !payload_index.indexed_fields().is_empty() {
                panic!("Payload HNSW disabled");
            }
        } else {
            debug!("skip building additional HNSW links");
        }

        self.config.indexed_vector_count.replace(indexed_vectors);

        let graph_links_path = GraphLayers::<TGraphLinks>::get_links_path(&self.path);
        self.graph = Some(graph_layers_builder.into_graph_layers(Some(&graph_links_path))?);

        #[cfg(debug_assertions)]
        {
            let graph = self.graph.as_ref().unwrap();
            for (idx, deleted) in deleted_bitslice.iter().enumerate() {
                if *deleted {
                    debug_assert!(graph.links.links(idx as PointOffsetType, 0).is_empty());
                }
            }
        }

        debug!("finish additional payload field indexing");
        self.save()
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        let tm = &self.searches_telemetry;

        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: tm.unfiltered_plain.lock().get_statistics(),
            filtered_plain: Default::default(),
            unfiltered_hnsw: tm.unfiltered_hnsw.lock().get_statistics(),
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

    fn indexed_vector_count(&self) -> usize {
        self.config
            .indexed_vector_count
            // If indexed vector count is unknown, fall back to number of points
            .or_else(|| self.graph.as_ref().map(|graph| graph.num_points()))
            .unwrap_or(0)
    }

    fn is_appendable(&self) -> bool {
        false
    }
}
