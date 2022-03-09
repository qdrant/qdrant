use crate::index::{PayloadIndex, PayloadIndexSS, VectorIndex};
use crate::payload_storage::ConditionCheckerSS;
use crate::types::{
    Filter, PayloadKeyType, PayloadKeyTypeRef, PointOffsetType, SearchParams, VectorElementType,
};
use crate::vector_storage::{ScoredPointOffset, VectorStorageSS};

use crate::entry::entry_point::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::PayloadConfig;
use atomic_refcell::AtomicRefCell;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Implementation of `PayloadIndex` which does not really indexes anything.
///
/// Used for small segments, which are easier to keep simple for faster updates,
/// rather than spend time for index re-building
pub struct PlainPayloadIndex {
    condition_checker: Arc<ConditionCheckerSS>,
    vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    config: PayloadConfig,
    path: PathBuf,
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
        condition_checker: Arc<ConditionCheckerSS>,
        vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
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
            path: path.to_owned(),
        };

        if !index.config_path().exists() {
            index.save_config()?;
        }

        Ok(index)
    }
}

impl PayloadIndex for PlainPayloadIndex {
    fn indexed_fields(&self) -> Vec<PayloadKeyType> {
        self.config.indexed_fields.clone()
    }

    fn set_indexed(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()> {
        if !self.config.indexed_fields.iter().any(|x| x == field) {
            self.config.indexed_fields.push(field.into());
            return self.save_config();
        }
        Ok(())
    }

    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()> {
        self.config.indexed_fields = self
            .config
            .indexed_fields
            .iter()
            .cloned()
            .filter(|x| x != field)
            .collect();
        self.save_config()
    }

    fn estimate_cardinality(&self, _query: &Filter) -> CardinalityEstimation {
        let total_points = self.vector_storage.borrow().vector_count();
        CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp: total_points / 2,
            max: total_points,
        }
    }

    fn query_points<'a>(
        &'a self,
        query: &'a Filter,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        let mut matched_points = vec![];
        for i in self.vector_storage.borrow().iter_ids() {
            if self.condition_checker.check(i, query) {
                matched_points.push(i);
            }
        }
        Box::new(matched_points.into_iter())
    }

    fn payload_blocks(
        &self,
        _field: PayloadKeyTypeRef,
        _threshold: usize,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        // No blocks for un-indexed payload
        Box::new(vec![].into_iter())
    }
}

pub struct PlainIndex {
    vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    payload_index: Arc<AtomicRefCell<PayloadIndexSS>>,
}

impl PlainIndex {
    pub fn new(
        vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
        payload_index: Arc<AtomicRefCell<PayloadIndexSS>>,
    ) -> PlainIndex {
        PlainIndex {
            vector_storage,
            payload_index,
        }
    }
}

impl VectorIndex for PlainIndex {
    fn search(
        &self,
        vector: &[VectorElementType],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset> {
        match filter {
            Some(filter) => {
                let borrowed_payload_index = self.payload_index.borrow();
                let mut filtered_ids = borrowed_payload_index.query_points(filter);
                self.vector_storage
                    .borrow()
                    .score_points(vector, &mut filtered_ids, top)
            }
            None => self.vector_storage.borrow().score_all(vector, top),
        }
    }

    fn build_index(&mut self, _stopped: &AtomicBool) -> OperationResult<()> {
        Ok(())
    }
}
