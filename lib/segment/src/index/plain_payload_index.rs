use crate::vector_storage::vector_storage::{ScoredPointOffset, VectorStorage};
use crate::index::index::{Index, PayloadIndex};
use crate::types::{Filter, VectorElementType, Distance, SearchParams, PointOffsetType, PayloadKeyType};
use crate::payload_storage::payload_storage::{ConditionChecker};

use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::entry::entry_point::OperationResult;
use crate::index::payload_config::PayloadConfig;
use std::path::{Path, PathBuf};
use std::fs::create_dir_all;

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
        let config_path = PayloadConfig::get_config_path(path);
        let config = PayloadConfig::load(&config_path)?;

        let index = PlainPayloadIndex {
            condition_checker,
            vector_storage,
            config,
            path: path.to_owned()
        };

        Ok(index)
    }

    pub fn new(
        condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
        vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
        path: &Path,
        config: Option<PayloadConfig>
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let payload_config = config.unwrap_or_default();

        let index = PlainPayloadIndex {
            condition_checker,
            vector_storage,
            config: payload_config,
            path: path.to_owned()
        };

        index.save_config()?;

        Ok(index)
    }
}

impl PayloadIndex for PlainPayloadIndex {
    fn indexed_fields(&self) -> Vec<PayloadKeyType> {
        self.config.indexed_fields.clone()
    }

    fn mark_indexed(&mut self, field: &PayloadKeyType) -> OperationResult<()> {
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

    fn query_points(&self, query: &Filter) -> Vec<PointOffsetType> {
        let mut matched_points = vec![];
        let condition_checker = self.condition_checker.borrow();
        for i in self.vector_storage.borrow().iter_ids() {
            if condition_checker.check(i, query) {
                matched_points.push(i);
            }
        }
        return matched_points;
    }
}


pub struct PlainIndex {
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    distance: Distance,
}

impl PlainIndex {
    pub fn new(
        vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
        payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
        distance: Distance,
    ) -> PlainIndex {
        return PlainIndex {
            vector_storage,
            payload_index,
            distance,
        };
    }
}


impl Index for PlainIndex {
    fn search(
        &self,
        vector: &Vec<VectorElementType>,
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset> {
        match filter {
            Some(filter) => {
                let filtered_ids = self.payload_index.borrow().query_points(filter);
                self.vector_storage.borrow().score_points(vector, &filtered_ids, top, &self.distance)
            }
            None => self.vector_storage.borrow().score_all(vector, top, &self.distance)
        }
    }

    fn build_index(&mut self) -> OperationResult<()> {
        Ok(())
    }
}