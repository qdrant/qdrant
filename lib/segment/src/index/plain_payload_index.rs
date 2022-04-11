use crate::index::{PayloadIndex, PayloadIndexSS, VectorIndex};
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::types::{
    Filter, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, PointOffsetType, SearchParams,
    VectorElementType,
};
use crate::vector_storage::{ScoredPointOffset, VectorStorageSS};
use std::collections::HashMap;

use crate::common::arc_atomic_ref_cell_iterator::ArcAtomicRefCellIterator;
use crate::entry::entry_point::OperationResult;
use crate::id_tracker::points_iterator::PointsIteratorSS;
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
    points_iterator: Arc<AtomicRefCell<PointsIteratorSS>>,
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
        points_iterator: Arc<AtomicRefCell<PointsIteratorSS>>,
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
            points_iterator,
            config,
            path: path.to_owned(),
        };

        if !index.config_path().exists() {
            index.save_config()?;
        }

        Ok(index)
    }

    pub fn query_points_callback<'a, F: FnMut(PointOffsetType)>(
        &'a self,
        query: &'a Filter,
        mut callback: F,
    ) {
        for id in self.points_iterator.borrow().iter_ids() {
            if self.condition_checker.check(id, query) {
                callback(id)
            }
        }
    }
}

impl PayloadIndex for PlainPayloadIndex {
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadSchemaType> {
        self.config.indexed_fields.clone()
    }

    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_type: PayloadSchemaType,
    ) -> OperationResult<()> {
        if self
            .config
            .indexed_fields
            .insert(field.to_owned(), payload_type)
            .is_none()
        {
            return self.save_config();
        }

        Ok(())
    }

    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()> {
        self.config.indexed_fields.remove(field);
        self.save_config()
    }

    fn estimate_cardinality(&self, _query: &Filter) -> CardinalityEstimation {
        let total_points = self.points_iterator.borrow().points_count();
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
        Box::new(ArcAtomicRefCellIterator::new(
            self.points_iterator.clone(),
            |points_iterator| {
                points_iterator
                    .iter_ids()
                    .filter(|id| self.condition_checker.check(*id, query))
            },
        ))
    }

    fn filter_context<'a>(&'a self, filter: &'a Filter) -> Box<dyn FilterContext + 'a> {
        Box::new(PlainFilterContext {
            filter,
            condition_checker: self.condition_checker.clone(),
        })
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

pub struct PlainFilterContext<'a> {
    condition_checker: Arc<ConditionCheckerSS>,
    filter: &'a Filter,
}

impl<'a> FilterContext for PlainFilterContext<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        self.condition_checker.check(point_id, self.filter)
    }
}
