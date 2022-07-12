use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::types::{
    Filter, Payload, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, PointOffsetType,
    SearchParams, VectorElementType,
};
use crate::vector_storage::{ScoredPointOffset, VectorStorageSS};
use std::collections::HashMap;

use crate::common::arc_atomic_ref_cell_iterator::ArcAtomicRefCellIterator;
use crate::entry::entry_point::OperationResult;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::StructPayloadIndex;
use atomic_refcell::AtomicRefCell;
use parking_lot::RwLock;
use schemars::_serde_json::Value;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::runtime::Handle;

/// Implementation of `PayloadIndex` which does not really indexes anything.
///
/// Used for small segments, which are easier to keep simple for faster updates,
/// rather than spend time for index re-building
pub struct PlainPayloadIndex {
    condition_checker: Arc<ConditionCheckerSS>,
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
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
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
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
            id_tracker,
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
        let total_points = self.id_tracker.borrow().points_count();
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
        let filter_context = self.filter_context(query);
        Box::new(ArcAtomicRefCellIterator::new(
            self.id_tracker.clone(),
            move |points_iterator| {
                points_iterator
                    .iter_ids()
                    .filter(move |id| filter_context.check(*id))
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

    fn assign(&mut self, _point_id: PointOffsetType, _payload: &Payload) -> OperationResult<()> {
        todo!()
    }

    fn payload(&self, _point_id: PointOffsetType) -> OperationResult<Payload> {
        todo!()
    }

    fn delete(
        &mut self,
        _point_id: PointOffsetType,
        _key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<Value>> {
        todo!()
    }

    fn drop(&mut self, _point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        todo!()
    }

    fn wipe(&mut self) -> OperationResult<()> {
        todo!()
    }

    fn flush(&self) -> OperationResult<()> {
        todo!()
    }

    fn infer_payload_type(
        &self,
        _key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<PayloadSchemaType>> {
        todo!()
    }
}

pub struct PlainIndex {
    vector_storage: Arc<RwLock<VectorStorageSS>>,
    payload_index: Arc<RwLock<StructPayloadIndex>>,
}

impl PlainIndex {
    pub fn new(
        vector_storage: Arc<RwLock<VectorStorageSS>>,
        payload_index: Arc<RwLock<StructPayloadIndex>>,
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
                let borrowed_payload_index = self.payload_index.read();
                let mut filtered_ids = borrowed_payload_index.query_points(filter);
                self.vector_storage
                    .read()
                    .score_points(vector, &mut filtered_ids, top)
            }
            None => self.vector_storage.read().score_all(vector, top),
        }
    }

    fn batch_search(
        &self,
        _vectors: &[Vec<VectorElementType>],
        _filters: &[Option<Filter>],
        _top: usize,
        _params: Option<&SearchParams>,
        _runtime_handle: Handle,
    ) -> Vec<Vec<ScoredPointOffset>> {
        todo!()
    }

    fn build_index(&self, _stopped: &AtomicBool) -> OperationResult<()> {
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
