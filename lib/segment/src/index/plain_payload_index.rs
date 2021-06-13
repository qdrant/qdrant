use crate::vector_storage::vector_storage::{ScoredPointOffset, VectorStorage};
use crate::index::index::{VectorIndex, PayloadIndex};
use crate::types::{Filter, VectorElementType, SearchParams, PointOffsetType, PayloadKeyType};
use crate::payload_storage::payload_storage::{ConditionChecker};

use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::entry::entry_point::OperationResult;
use crate::index::payload_config::PayloadConfig;
use std::path::{Path, PathBuf};
use std::fs::create_dir_all;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};


pub struct PlainPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    config: PayloadConfig,
    path: PathBuf
}


impl PlainPayloadIndex {

    fn config_path(&self) -> PathBuf {
        PayloadConfig::get_config_path(&self.path)
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = self.config_path();
        self.config.save(&config_path)
    }

    pub fn open(
        condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
        vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
        path: &Path,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let config_path = PayloadConfig::get_config_path(path);
        let config = if config_path.exists() {
            PayloadConfig::load(&config_path)?
        } else {
            PayloadConfig::default()
        };


        let index = PlainPayloadIndex {
            condition_checker,
            vector_storage,
            config,
            path: path.to_owned()
        };

        if !index.config_path().exists() {
            index.save_config()?
        }

        Ok(index)
    }
}

impl PayloadIndex for PlainPayloadIndex {
    fn indexed_fields(&self) -> Vec<PayloadKeyType> {
        self.config.indexed_fields.clone()
    }

    fn set_indexed(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        if !self.config.indexed_fields.contains(field) {
            self.config.indexed_fields.push(field.clone());
            return self.save_config()
        }
        Ok(())
    }

    fn drop_index(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
        self.config.indexed_fields = self.config.indexed_fields.iter().cloned().filter(|x| x != field).collect();
        self.save_config()
    }

    fn estimate_cardinality(&self, _query: &Filter) -> CardinalityEstimation {
        let total_points = self.vector_storage.borrow().vector_count();
        CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp: total_points / 2,
            max: total_points
        }
    }

    fn query_points<'a>(&'a self, query: &'a Filter) -> Box<dyn Iterator<Item=PointOffsetType> + 'a> {
        let mut matched_points = vec![];
        let condition_checker = self.condition_checker.borrow();
        for i in self.vector_storage.borrow().iter_ids() {
            if condition_checker.check(i, query) {
                matched_points.push(i);
            }
        }
        return Box::new(matched_points.into_iter());
    }

    fn payload_blocks(&self, _field: &PayloadKeyType, _threshold: usize) -> Box<dyn Iterator<Item=PayloadBlockCondition> + '_> {
        // No blocks for un-indexed payload
        Box::new(vec![].into_iter())
    }
}


pub struct PlainIndex {
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
}

impl PlainIndex {
    pub fn new(
        vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
        payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>
    ) -> PlainIndex {
        return PlainIndex {
            vector_storage,
            payload_index
        };
    }
}


impl VectorIndex for PlainIndex {
    fn search(
        &self,
        vector: &Vec<VectorElementType>,
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset> {
        match filter {
            Some(filter) => {
                let borrowed_payload_index = self.payload_index.borrow();
                let mut filtered_ids = borrowed_payload_index.query_points(filter);
                self.vector_storage.borrow().score_points(vector, &mut filtered_ids, top)
            }
            None => self.vector_storage.borrow().score_all(vector, top)
        }
    }

    fn build_index(&mut self) -> OperationResult<()> {
        Ok(())
    }
}