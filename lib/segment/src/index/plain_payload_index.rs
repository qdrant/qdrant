use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::cpu::CpuPermit;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use parking_lot::Mutex;
use schemars::_serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::common::{Flusher, BYTES_IN_KB};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndex};
use crate::json_path::JsonPath;
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
    SearchParams,
};
use crate::vector_storage::{new_stoppable_raw_scorer, VectorStorage, VectorStorageEnum};

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
        if let Some(prev_schema) = self
            .config
            .indexed_fields
            .insert(field.to_owned(), payload_schema.clone())
        {
            // the field is already present with the same schema, no need to save the config
            if prev_schema == payload_schema {
                return Ok(());
            }
        }
        self.save_config()?;

        Ok(())
    }

    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()> {
        self.config.indexed_fields.remove(field);
        self.save_config()
    }

    fn estimate_cardinality(&self, _query: &Filter) -> CardinalityEstimation {
        let available_points = self.id_tracker.borrow().available_point_count();
        CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp: available_points / 2,
            max: available_points,
        }
    }

    /// Forward to non nested implementation.
    fn estimate_nested_cardinality(
        &self,
        query: &Filter,
        _nested_path: &JsonPath,
    ) -> CardinalityEstimation {
        self.estimate_cardinality(query)
    }

    fn query_points(&self, query: &Filter) -> Vec<PointOffsetType> {
        let filter_context = self.filter_context(query);
        self.id_tracker
            .borrow()
            .iter_ids()
            .filter(|id| filter_context.check(*id))
            .collect()
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

    fn assign(
        &mut self,
        _point_id: PointOffsetType,
        _payload: &Payload,
        _key: &Option<JsonPath>,
    ) -> OperationResult<()> {
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

    pub fn is_small_enough_for_unindexed_search(
        &self,
        search_optimized_threshold_kb: usize,
        filter: Option<&Filter>,
    ) -> bool {
        let vector_storage = self.vector_storage.borrow();
        let vector_size_bytes =
            vector_storage.available_size_in_bytes() / vector_storage.available_vector_count();
        let indexing_threshold_bytes = search_optimized_threshold_kb * BYTES_IN_KB;

        if let Some(payload_filter) = filter {
            let payload_index = self.payload_index.borrow();
            let cardinality = payload_index.estimate_cardinality(payload_filter);
            let scan_size = vector_size_bytes.saturating_mul(cardinality.max);
            scan_size <= indexing_threshold_bytes
        } else {
            let vector_count = vector_storage.available_vector_count();
            let vector_storage_size = vector_size_bytes.saturating_mul(vector_count);
            vector_storage_size <= indexing_threshold_bytes
        }
    }
}

impl VectorIndex for PlainIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let is_indexed_only = params.map(|p| p.indexed_only).unwrap_or(false);
        if is_indexed_only
            && !self.is_small_enough_for_unindexed_search(
                query_context.search_optimized_threshold_kb(),
                filter,
            )
        {
            return Ok(vec![vec![]; vectors.len()]);
        }

        let is_stopped = query_context.is_stopped();

        match filter {
            Some(filter) => {
                let _timer = ScopeDurationMeasurer::new(&self.filtered_searches_telemetry);
                let id_tracker = self.id_tracker.borrow();
                let payload_index = self.payload_index.borrow();
                let vector_storage = self.vector_storage.borrow();
                let filtered_ids_vec = payload_index.query_points(filter);
                let deleted_points = query_context
                    .deleted_points()
                    .unwrap_or(id_tracker.deleted_point_bitslice());
                vectors
                    .iter()
                    .map(|&vector| {
                        new_stoppable_raw_scorer(
                            vector.to_owned(),
                            &vector_storage,
                            deleted_points,
                            &is_stopped,
                        )
                        .map(|scorer| {
                            scorer.peek_top_iter(&mut filtered_ids_vec.iter().copied(), top)
                        })
                    })
                    .collect()
            }
            None => {
                let _timer = ScopeDurationMeasurer::new(&self.unfiltered_searches_telemetry);
                let vector_storage = self.vector_storage.borrow();
                let id_tracker = self.id_tracker.borrow();
                let deleted_points = query_context
                    .deleted_points()
                    .unwrap_or(id_tracker.deleted_point_bitslice());
                vectors
                    .iter()
                    .map(|&vector| {
                        new_stoppable_raw_scorer(
                            vector.to_owned(),
                            &vector_storage,
                            deleted_points,
                            &is_stopped,
                        )
                        .map(|scorer| scorer.peek_top_all(top))
                    })
                    .collect()
            }
        }
    }

    fn build_index_with_progress(
        &mut self,
        _permit: Arc<CpuPermit>,
        _stopped: &AtomicBool,
        _tick_progress: impl FnMut(),
    ) -> OperationResult<()> {
        Ok(())
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: self
                .unfiltered_searches_telemetry
                .lock()
                .get_statistics(detail),
            filtered_plain: self
                .filtered_searches_telemetry
                .lock()
                .get_statistics(detail),
            unfiltered_hnsw: OperationDurationStatistics::default(),
            filtered_small_cardinality: OperationDurationStatistics::default(),
            filtered_large_cardinality: OperationDurationStatistics::default(),
            filtered_exact: OperationDurationStatistics::default(),
            filtered_sparse: Default::default(),
            unfiltered_exact: OperationDurationStatistics::default(),
            unfiltered_sparse: OperationDurationStatistics::default(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn indexed_vector_count(&self) -> usize {
        0
    }

    fn update_vector(&mut self, _id: PointOffsetType, _vector: VectorRef) -> OperationResult<()> {
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
