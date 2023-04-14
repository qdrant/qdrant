use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use parking_lot::Mutex;
use schemars::_serde_json::Value;

use crate::common::arc_atomic_ref_cell_iterator::ArcAtomicRefCellIterator;
use crate::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
    PointOffsetType, SearchParams,
};
use crate::vector_storage::{new_raw_scorer, ScoredPointOffset, VectorStorageEnum};

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
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.config.indexed_fields.clone()
    }

    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_schema: PayloadFieldSchema,
    ) -> OperationResult<()> {
        if self
            .config
            .indexed_fields
            .insert(field.to_owned(), payload_schema)
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

    fn indexed_points(&self, _field: PayloadKeyTypeRef) -> usize {
        0 // No points are indexed in the plain index
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
        unreachable!()
    }

    fn payload(&self, _point_id: PointOffsetType) -> OperationResult<Payload> {
        unreachable!()
    }

    fn delete(
        &mut self,
        _point_id: PointOffsetType,
        _key: PayloadKeyTypeRef,
    ) -> OperationResult<Vec<Value>> {
        unreachable!()
    }

    fn drop(&mut self, _point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        unreachable!()
    }

    fn wipe(&mut self) -> OperationResult<()> {
        unreachable!()
    }

    fn flusher(&self) -> Flusher {
        unreachable!()
    }

    fn infer_payload_type(
        &self,
        _key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<PayloadSchemaType>> {
        unreachable!()
    }

    fn take_database_snapshot(&self, _: &Path) -> OperationResult<()> {
        unreachable!()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.config_path()]
    }
}

pub struct PlainIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    filtered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
}

impl PlainIndex {
    pub fn new(
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    ) -> PlainIndex {
        PlainIndex {
            id_tracker,
            vector_storage,
            payload_index,
            filtered_searches_telemetry: OperationDurationsAggregator::new(),
            unfiltered_searches_telemetry: OperationDurationsAggregator::new(),
        }
    }
}

impl VectorIndex for PlainIndex {
    fn search(
        &self,
        vectors: &[&[VectorElementType]],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
    ) -> Vec<Vec<ScoredPointOffset>> {
        match filter {
            Some(filter) => {
                let _timer = ScopeDurationMeasurer::new(&self.filtered_searches_telemetry);
                let payload_index = self.payload_index.borrow();
                let vector_storage = self.vector_storage.borrow();
                let id_tracker = self.id_tracker.borrow();
                let filtered_ids_vec: Vec<_> = payload_index.query_points(filter).collect();
                vectors
                    .iter()
                    .map(|vector| {
                        new_raw_scorer(
                            vector.to_vec(),
                            &vector_storage,
                            id_tracker.deleted_bitslice(),
                        )
                        .peek_top_iter(&mut filtered_ids_vec.iter().copied(), top)
                    })
                    .collect()
            }
            None => {
                let _timer = ScopeDurationMeasurer::new(&self.unfiltered_searches_telemetry);
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
            }
        }
    }

    fn build_index(&mut self, _stopped: &AtomicBool) -> OperationResult<()> {
        Ok(())
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: self.unfiltered_searches_telemetry.lock().get_statistics(),
            filtered_plain: self.filtered_searches_telemetry.lock().get_statistics(),
            unfiltered_hnsw: OperationDurationStatistics::default(),
            filtered_small_cardinality: OperationDurationStatistics::default(),
            filtered_large_cardinality: OperationDurationStatistics::default(),
            filtered_exact: OperationDurationStatistics::default(),
            unfiltered_exact: OperationDurationStatistics::default(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
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
