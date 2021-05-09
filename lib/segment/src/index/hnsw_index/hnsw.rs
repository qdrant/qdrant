use crate::entry::entry_point::OperationResult;
use std::path::{Path, PathBuf};
use std::fs::create_dir_all;
use crate::index::index::{Index, PayloadIndex};
use crate::types::{SearchParams, Filter, PointOffsetType, Distance, Indexes, VectorElementType};
use crate::vector_storage::vector_storage::{ScoredPointOffset, VectorStorage};
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::payload_storage::payload_storage::ConditionChecker;
use std::cmp::max;
use std::ops::Deref;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use rand::thread_rng;
use rand::prelude::ThreadRng;
use crate::index::hnsw_index::config::HnswConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::spaces::metric::Metric;
use crate::spaces::tools::mertic_object;
use crate::types::Condition::Field;
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;

pub struct HNSWIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    config: HnswConfig,
    path: PathBuf,
    metric: Box<dyn Metric>,
    thread_rng: ThreadRng,
    graph: GraphLayers,
}


impl HNSWIndex {
    fn get_graph_path(&self) -> PathBuf { GraphLayers::get_path(self.path.as_path()) }

    pub fn open(
        path: &Path,
        distance: Distance,
        condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
        vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
        payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
        index_config: Option<Indexes>,
        indexing_threshold: usize,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let mut rng = thread_rng();

        let config_path = HnswConfig::get_config_path(path);
        let config = if config_path.exists() {
            HnswConfig::load(&config_path)?
        } else {
            let (m, ef_construct) = match index_config {
                None => match Indexes::default_hnsw() {
                    Indexes::Hnsw { m, ef_construct } => (m, ef_construct),
                    _ => panic!("Mismatch index config"),
                },
                Some(indx) => match indx {
                    Indexes::Hnsw { m, ef_construct } => (m, ef_construct),
                    _ => panic!("Mismatch index config"),
                }
            };
            HnswConfig::new(m, ef_construct, indexing_threshold)
        };

        let graph_path = GraphLayers::get_path(path);
        let graph = if graph_path.exists() {
            GraphLayers::load(graph_path.as_path())?
        } else {
            let entry_points_num = vector_storage.borrow().total_vector_count();
            GraphLayers::new(
                vector_storage.borrow().total_vector_count(),
                config.m,
                config.m0,
                config.ef_construct,
                max(1, entry_points_num / indexing_threshold * 10),
                true,
            )
        };

        let metric = mertic_object(&distance);

        Ok(HNSWIndex {
            condition_checker,
            vector_storage,
            payload_index,
            config,
            path: path.to_owned(),
            metric,
            thread_rng: rng,
            graph,
        })
    }

    pub fn build_and_save(&mut self) -> OperationResult<()> {
        unimplemented!()
    }

    pub fn search_with_condition(&self, top: usize, ef: usize, points_scorer: &FilteredScorer) -> Vec<ScoredPointOffset> {
        self.graph.search(top, ef, points_scorer)
    }

    pub fn link_point(&mut self, point_id: PointOffsetType, points_scorer: &FilteredScorer) {
        let point_level = self.graph.get_random_layer(&mut self.thread_rng);
        self.graph.link_new_point(point_id, point_level, points_scorer);
    }
}


impl Index for HNSWIndex {
    fn search(&self, vector: &Vec<VectorElementType>, filter: Option<&Filter>, top: usize, params: Option<&SearchParams>) -> Vec<ScoredPointOffset> {
        let req_ef = match params {
            None => self.config.ef,
            Some(request_params) => match request_params {
                SearchParams::Hnsw { ef } => *ef
            }
        };

        // ef should always be bigger that required top
        let ef = max(req_ef, top);

        let vector_storage = self.vector_storage.borrow();
        let raw_scorer = vector_storage.raw_scorer(vector.clone());
        let condition_checker = self.condition_checker.borrow();

        let points_scorer = FilteredScorer {
            raw_scorer: raw_scorer.as_ref(),
            condition_checker: condition_checker.deref(),
            filter,
        };

        self.graph.search(top, ef, &points_scorer)
    }

    fn build_index(&mut self) -> OperationResult<()> {
        // Build main index graph
        let vector_storage = self.vector_storage.borrow();
        let condition_checker = self.condition_checker.borrow();
        let mut rng = thread_rng();

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

        let total_vectors_count = vector_storage.total_vector_count();
        let mut block_condition_checker = BuildConditionChecker::new(total_vectors_count);
        let payload_index = self.payload_index.borrow();

        for payload_block in payload_index.payload_blocks(self.config.indexing_threshold) {
            block_condition_checker.filter_list.next_iteration();
            let filter = Filter::new_must(Field(payload_block.condition));

            for block_point_id in payload_index.query_points(&filter) {
                block_condition_checker.filter_list.check_and_update_visited(block_point_id);
            }

            let mut block_graph = GraphLayers::new(
                total_vectors_count,
                self.config.m,
                self.config.m0,
                self.config.ef_construct,
                1,
                true,
            );

            for block_point_id in payload_index.query_points(&filter) {
                let vector = vector_storage.get_vector(block_point_id).unwrap();
                let raw_scorer = vector_storage.raw_scorer(vector);
                let points_scorer = FilteredScorer {
                    raw_scorer: raw_scorer.as_ref(),
                    condition_checker: &block_condition_checker,
                    filter: None,
                };

                let level = self.graph.point_level(block_point_id);
                block_graph.link_new_point(block_point_id, level, &points_scorer);
            }

            self.graph.merge_from_other(block_graph);
        }
        Ok(())
    }
}
