use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::sample_estimation::sample_check_cardinality;
use crate::index::{PayloadIndex, VectorIndex};
use crate::types::Condition::Field;
use crate::types::{
    FieldCondition, Filter, HnswConfig, SearchParams, VectorElementType, VECTOR_ELEMENT_SIZE,
};
use crate::vector_storage::{ScoredPointOffset, VectorStorageSS};
use log::debug;
use tokio::runtime::Handle;

use crate::index::struct_payload_index::StructPayloadIndex;
use parking_lot::RwLock;
use rand::thread_rng;
use std::cmp::max;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const HNSW_USE_HEURISTIC: bool = true;
const BYTES_IN_KB: usize = 1024;

pub struct HNSWIndex {
    vector_storage: Arc<RwLock<VectorStorageSS>>,
    payload_index: Arc<RwLock<StructPayloadIndex>>,
    config: Arc<RwLock<HnswGraphConfig>>,
    path: PathBuf,
    graph: RwLock<GraphLayers>,
}

impl HNSWIndex {
    pub fn open(
        path: &Path,
        vector_storage: Arc<RwLock<VectorStorageSS>>,
        payload_index: Arc<RwLock<StructPayloadIndex>>,
        hnsw_config: RwLock<HnswConfig>,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;

        let config_path = HnswGraphConfig::get_config_path(path);
        let config = if config_path.exists() {
            HnswGraphConfig::load(&config_path)?
        } else {
            let indexing_threshold = hnsw_config.read().full_scan_threshold * BYTES_IN_KB
                / (vector_storage.read().vector_dim() * VECTOR_ELEMENT_SIZE);

            HnswGraphConfig::new(
                hnsw_config.read().m,
                hnsw_config.read().ef_construct,
                indexing_threshold,
            )
        };

        let graph_path = GraphLayers::get_path(path);
        let graph = if graph_path.exists() {
            GraphLayers::load(&graph_path)?
        } else {
            let borrowed_vector_storage = vector_storage.read();
            let total_points = borrowed_vector_storage.total_vector_count();
            let vector_per_threshold = hnsw_config.read().full_scan_threshold * BYTES_IN_KB
                / (borrowed_vector_storage.vector_dim() * VECTOR_ELEMENT_SIZE);
            GraphLayers::new(
                borrowed_vector_storage.total_vector_count(),
                config.m,
                config.m0,
                config.ef_construct,
                max(1, total_points / vector_per_threshold * 10),
                HNSW_USE_HEURISTIC,
            )
        };

        Ok(HNSWIndex {
            vector_storage,
            payload_index,
            config: Arc::new(RwLock::new(config)),
            path: path.to_owned(),
            graph,
        })
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = HnswGraphConfig::get_config_path(&self.path);
        self.config.write().save(&config_path)
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
        graph: &mut GraphLayers,
        condition: FieldCondition,
        block_condition_checker: &mut BuildConditionChecker,
    ) {
        block_condition_checker.filter_list.next_iteration();

        let filter = Filter::new_must(Field(condition));

        let payload_index = self.payload_index.read();
        let vector_storage = self.vector_storage.read();

        for block_point_id in payload_index.query_points(&filter) {
            block_condition_checker
                .filter_list
                .check_and_update_visited(block_point_id);
        }

        for block_point_id in payload_index.query_points(&filter) {
            let vector = vector_storage.get_vector(block_point_id).unwrap();
            let raw_scorer = vector_storage.raw_scorer(vector);
            block_condition_checker.current_point = block_point_id;
            let points_scorer =
                FilteredScorer::new(raw_scorer.as_ref(), Some(block_condition_checker));

            let level = self.graph.point_level(block_point_id);
            graph.link_new_point(block_point_id, level, points_scorer);
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
            .unwrap_or(self.config.read().ef);

        // ef should always be bigger that required top
        let ef = max(req_ef, top);

        let vector_storage = self.vector_storage.read();
        let raw_scorer = vector_storage.raw_scorer(vector.to_owned());
        let payload_index = self.payload_index.read();

        let filter_context = filter.map(|f| payload_index.filter_context(f));

        let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());

        self.graph.search(top, ef, points_scorer)
    }
}

impl VectorIndex for Arc<HNSWIndex> {
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

                let payload_index = self.payload_index.read();
                let query_cardinality = payload_index.estimate_cardinality(query_filter);

                // debug!("query_cardinality: {:#?}", query_cardinality);

                let vector_storage = self.vector_storage.read();

                if query_cardinality.max < self.config.read().indexing_threshold {
                    // if cardinality is small - use plain index
                    let mut filtered_ids = payload_index.query_points(query_filter);
                    return vector_storage.score_points(vector, &mut filtered_ids, top);
                }

                if query_cardinality.min > self.config.read().indexing_threshold {
                    // if cardinality is high enough - use HNSW index
                    return self.search_with_graph(vector, filter, top, params);
                }

                let filter_context = payload_index.filter_context(query_filter);

                // Fast cardinality estimation is not enough, do sample estimation of cardinality

                return if sample_check_cardinality(
                    vector_storage.sample_ids(),
                    |idx| filter_context.check(idx),
                    self.config.read().indexing_threshold,
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

    fn batch_search(
        &self,
        vectors: &[Vec<VectorElementType>],
        filters: &[Option<Filter>],
        top: usize,
        params: Option<&SearchParams>,
        runtime_handle: &Handle,
    ) -> Vec<Vec<ScoredPointOffset>> {
        let result = vectors
            .into_iter()
            .zip(filters.into_iter())
            .map(|(vector, filter)| {
                search(
                    self.clone(),
                    vector.to_owned(),
                    filter.to_owned(),
                    top,
                    params.cloned(),
                )
            })
            .map(|f| runtime_handle.spawn(f));

        todo!()
    }

    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()> {
        // Build main index graph
        let vector_storage = self.vector_storage.write();
        let mut rng = thread_rng();

        let total_points = vector_storage.total_vector_count();

        debug!("building hnsw for {}", total_points);
        let graph = GraphLayers::new(
            total_points,
            self.config.read().m,
            self.config.read().m0,
            self.config.read().ef_construct,
            max(1, total_points / self.config.read().indexing_threshold * 10),
            HNSW_USE_HEURISTIC,
        );

        self.graph = graph;

        for vector_id in vector_storage.iter_ids() {
            if stopped.load(Ordering::Relaxed) {
                return Err(OperationError::Cancelled {
                    description: "Cancelled by external thread".to_string(),
                });
            }
            let vector = vector_storage.get_vector(vector_id).unwrap();
            let raw_scorer = vector_storage.raw_scorer(vector);
            let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

            let level = self.graph.get_random_layer(&mut rng);
            self.graph.link_new_point(vector_id, level, points_scorer);
        }

        debug!("finish main graph");

        let total_vectors_count = vector_storage.total_vector_count();
        let mut block_condition_checker = BuildConditionChecker::new(total_vectors_count);

        let payload_index = self.payload_index.read();

        for (field, _) in payload_index.indexed_fields() {
            debug!("building additional index for field {}", &field);

            // ToDo: Think about using connectivity threshold (based on 1/m0) instead of `indexing_threshold`
            for payload_block in
                payload_index.payload_blocks(&field, self.config.read().indexing_threshold)
            {
                if stopped.load(Ordering::Relaxed) {
                    return Err(OperationError::Cancelled {
                        description: "Cancelled by external thread".to_string(),
                    });
                }
                // ToDo: re-use graph layer for same payload
                let mut additional_graph = GraphLayers::new_with_params(
                    self.vector_storage.read().total_vector_count(),
                    self.config.read().m,
                    self.config.read().m0,
                    self.config.read().ef_construct,
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

async fn search(
    index: Arc<HNSWIndex>,
    vector: Vec<VectorElementType>,
    filter: Option<Filter>,
    top: usize,
    params: Option<SearchParams>,
) -> Vec<ScoredPointOffset> {
    index.search(vector.as_slice(), filter.as_ref(), top, params.as_ref())
}
