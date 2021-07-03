use crate::entry::entry_point::OperationResult;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::ConditionChecker;
use crate::types::Condition::Field;
use crate::types::{
    FieldCondition, Filter, HnswConfig, PointOffsetType, SearchParams, VectorElementType,
};
use crate::vector_storage::{ScoredPointOffset, VectorStorage};
use atomic_refcell::AtomicRefCell;
use log::debug;
use rand::prelude::ThreadRng;
use rand::thread_rng;
use std::cmp::max;
use std::fs::create_dir_all;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const HNSW_USE_HEURISTIC: bool = true;

pub struct HNSWIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    config: HnswGraphConfig,
    path: PathBuf,
    thread_rng: ThreadRng,
    graph: GraphLayers,
}

impl HNSWIndex {
    pub fn open(
        path: &Path,
        condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
        vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
        payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
        hnsw_config: HnswConfig,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let rng = thread_rng();

        let config_path = HnswGraphConfig::get_config_path(path);
        let config = if config_path.exists() {
            HnswGraphConfig::load(&config_path)?
        } else {
            HnswGraphConfig::new(
                hnsw_config.m,
                hnsw_config.ef_construct,
                hnsw_config.full_scan_threshold,
            )
        };

        let graph_path = GraphLayers::get_path(path);
        let graph = if graph_path.exists() {
            GraphLayers::load(graph_path.as_path())?
        } else {
            let total_points = vector_storage.borrow().total_vector_count();
            GraphLayers::new(
                vector_storage.borrow().total_vector_count(),
                config.m,
                config.m0,
                config.ef_construct,
                max(1, total_points / hnsw_config.full_scan_threshold * 10),
                HNSW_USE_HEURISTIC,
            )
        };

        Ok(HNSWIndex {
            condition_checker,
            vector_storage,
            payload_index,
            config,
            path: path.to_owned(),
            thread_rng: rng,
            graph,
        })
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = HnswGraphConfig::get_config_path(self.path.as_path());
        self.config.save(&config_path)
    }

    fn save_graph(&self) -> OperationResult<()> {
        let graph_path = GraphLayers::get_path(self.path.as_path());
        self.graph.save(&graph_path)
    }

    pub fn save(&self) -> OperationResult<()> {
        self.save_config()?;
        self.save_graph()?;
        Ok(())
    }

    pub fn link_point(&mut self, point_id: PointOffsetType, points_scorer: &FilteredScorer) {
        let point_level = self.graph.get_random_layer(&mut self.thread_rng);
        self.graph
            .link_new_point(point_id, point_level, points_scorer);
    }

    pub fn build_filtered_graph(
        &self,
        graph: &mut GraphLayers,
        condition: FieldCondition,
        block_condition_checker: &mut BuildConditionChecker,
    ) {
        block_condition_checker.filter_list.next_iteration();

        let filter = Filter::new_must(Field(condition));

        let payload_index = self.payload_index.borrow();
        let vector_storage = self.vector_storage.borrow();

        for block_point_id in payload_index.query_points(&filter) {
            block_condition_checker
                .filter_list
                .check_and_update_visited(block_point_id);
        }

        for block_point_id in payload_index.query_points(&filter) {
            let vector = vector_storage.get_vector(block_point_id).unwrap();
            let raw_scorer = vector_storage.raw_scorer(vector);
            block_condition_checker.current_point = block_point_id;
            let points_scorer = FilteredScorer {
                raw_scorer: raw_scorer.as_ref(),
                condition_checker: block_condition_checker,
                filter: None,
            };

            let level = self.graph.point_level(block_point_id);
            graph.link_new_point(block_point_id, level, &points_scorer);
        }
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
        let condition_checker = self.condition_checker.borrow();

        let points_scorer = FilteredScorer {
            raw_scorer: raw_scorer.as_ref(),
            condition_checker: condition_checker.deref(),
            filter,
        };

        self.graph.search(top, ef, &points_scorer)
    }
}

impl VectorIndex for HNSWIndex {
    fn search(
        &self,
        vector: &[VectorElementType],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset> {
        match filter {
            None => self.search_with_graph(vector, None, top, params),
            Some(query_filter) => {
                // depending on the amount of filtered-out points the optimal strategy could be
                // - to retrieve possible points and score them after
                // - to use HNSW index with filtering condition

                let payload_index = self.payload_index.borrow();
                let query_cardinality = payload_index.estimate_cardinality(query_filter);

                // debug!("query_cardinality: {:#?}", query_cardinality);

                let vector_storage = self.vector_storage.borrow();

                if query_cardinality.max < self.config.indexing_threshold {
                    // if cardinality is small - use plain index
                    let mut filtered_ids = payload_index.query_points(query_filter);
                    return vector_storage.score_points(vector, &mut filtered_ids, top);
                }

                if query_cardinality.min > self.config.indexing_threshold {
                    // if cardinality is high enough - use HNSW index
                    return self.search_with_graph(vector, filter, top, params);
                }

                // Fast cardinality estimation is not enough, do sample estimation of cardinality

                let condition_checker = self.condition_checker.borrow();
                return if sample_check_cardinality(
                    vector_storage.sample_ids(),
                    |idx| condition_checker.check(idx, query_filter),
                    self.config.indexing_threshold,
                    vector_storage.vector_count(),
                ) {
                    // if cardinality is high enough - use HNSW index
                    self.search_with_graph(vector, filter, top, params)
                } else {
                    // if cardinality is small - use plain index
                    let mut filtered_ids = payload_index.query_points(query_filter);
                    vector_storage.score_points(vector, &mut filtered_ids, top)
                };
            }
        }
    }

    fn build_index(&mut self) -> OperationResult<()> {
        // Build main index graph
        let vector_storage = self.vector_storage.borrow();
        let condition_checker = self.condition_checker.borrow();
        let mut rng = thread_rng();

        let total_points = vector_storage.total_vector_count();

        debug!("building hnsw for {}", total_points);
        self.graph = GraphLayers::new(
            total_points,
            self.config.m,
            self.config.m0,
            self.config.ef_construct,
            max(1, total_points / self.config.indexing_threshold * 10),
            HNSW_USE_HEURISTIC,
        );

        for vector_id in vector_storage.iter_ids() {
            let vector = vector_storage.get_vector(vector_id).unwrap();
            let raw_scorer = vector_storage.raw_scorer(vector);
            let points_scorer = FilteredScorer {
                raw_scorer: raw_scorer.as_ref(),
                condition_checker: condition_checker.deref(),
                filter: None,
            };

            let level = self.graph.get_random_layer(&mut rng);
            self.graph.link_new_point(vector_id, level, &points_scorer);
        }

        debug!("finish main graph");

        let total_vectors_count = vector_storage.total_vector_count();
        let mut block_condition_checker = BuildConditionChecker::new(total_vectors_count);

        let payload_index = self.payload_index.borrow();

        for field in payload_index.indexed_fields() {
            debug!("building additional index for field {}", &field);

            // ToDo: Think about using connectivity threshold (based on 1/m0) instead of `indexing_threshold`
            for payload_block in
                payload_index.payload_blocks(&field, self.config.indexing_threshold)
            {
                // ToDo: re-use graph layer for same payload
                let mut additional_graph = GraphLayers::new_with_params(
                    self.vector_storage.borrow().total_vector_count(),
                    self.config.m,
                    self.config.m0,
                    self.config.ef_construct,
                    1,
                    HNSW_USE_HEURISTIC,
                    false,
                );
                self.build_filtered_graph(
                    &mut additional_graph,
                    payload_block.condition,
                    &mut block_condition_checker,
                );
                self.graph.merge_from_other(additional_graph);
            }
        }
        debug!("finish additional payload field indexing");
        self.save()
    }
}
