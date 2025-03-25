use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use schemars::_serde_json::Value;

use super::field_index::FieldIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerSS;
use crate::index::PayloadIndex;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::PayloadConfig;
use crate::json_path::JsonPath;
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
};

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

    fn build_index(
        &self,
        _field: PayloadKeyTypeRef,
        _payload_schema: &PayloadFieldSchema,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<FieldIndex>>> {
        Ok(Some(Vec::new()))
    }

    fn apply_index(
        &mut self,
        field: PayloadKeyType,
        payload_schema: PayloadFieldSchema,
        _field_index: Vec<FieldIndex>,
    ) -> OperationResult<()> {
        if let Some(prev_schema) = self
            .config
            .indexed_fields
            .insert(field, payload_schema.clone())
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

    fn estimate_cardinality(
        &self,
        _query: &Filter,
        _hw_counter: &HardwareCounterCell, // No measurements needed here.
    ) -> CardinalityEstimation {
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
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        self.estimate_cardinality(query, hw_counter)
    }

    fn query_points(
        &self,
        query: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointOffsetType> {
        let filter_context = self.filter_context(query, hw_counter);
        self.id_tracker
            .borrow()
            .iter_ids()
            .filter(|id| filter_context.check(*id))
            .collect()
    }

    fn indexed_points(&self, _field: PayloadKeyTypeRef) -> usize {
        0 // No points are indexed in the plain index
    }

    fn filter_context<'a>(
        &'a self,
        filter: &'a Filter,
        _: &HardwareCounterCell,
    ) -> Box<dyn FilterContext + 'a> {
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

    fn overwrite_payload(
        &mut self,
        _point_id: PointOffsetType,
        _payload: &Payload,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        unreachable!()
    }

    fn set_payload(
        &mut self,
        _point_id: PointOffsetType,
        _payload: &Payload,
        _key: &Option<JsonPath>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        unreachable!()
    }

    fn get_payload(
        &self,
        _point_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        unreachable!()
    }

    fn delete_payload(
        &mut self,
        _point_id: PointOffsetType,
        _key: PayloadKeyTypeRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        unreachable!()
    }

    fn clear_payload(
        &mut self,
        _point_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        unreachable!()
    }

    fn flusher(&self) -> Flusher {
        unreachable!()
    }

    fn infer_payload_type(
        &self,
        _key: PayloadKeyTypeRef,
        _hw_counter: &HardwareCounterCell,
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

pub struct PlainFilterContext<'a> {
    condition_checker: Arc<ConditionCheckerSS>,
    filter: &'a Filter,
}

impl FilterContext for PlainFilterContext<'_> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        self.condition_checker.check(point_id, self.filter)
    }
}
