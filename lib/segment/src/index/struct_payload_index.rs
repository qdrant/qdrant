use std::collections::{HashMap, HashSet};
use std::fs::create_dir_all;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use log::debug;
use parking_lot::RwLock;
use rocksdb::DB;
use schemars::_serde_json::Value;

use crate::common::arc_atomic_ref_cell_iterator::ArcAtomicRefCellIterator;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
use crate::common::utils::IndexesMap;
use crate::common::Flusher;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::index_selector::index_selector;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PrimaryCondition,
};
use crate::index::payload_config::PayloadConfig;
use crate::index::query_estimator::estimate_filter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_filter_context::StructFilterContext;
use crate::index::visited_pool::VisitedPool;
use crate::index::PayloadIndex;
use crate::json_path::JsonPath;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::{FilterContext, PayloadStorage};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    infer_collection_value_type, infer_value_type, Condition, FieldCondition, Filter,
    IsEmptyCondition, IsNullCondition, Payload, PayloadContainer, PayloadField, PayloadFieldSchema,
    PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
};

pub const PAYLOAD_FIELD_INDEX_PATH: &str = "fields";

/// `PayloadIndex` implementation, which actually uses index structures for providing faster search
pub struct StructPayloadIndex {
    /// Payload storage
    payload: Arc<AtomicRefCell<PayloadStorageEnum>>,
    /// Used for `has_id` condition and estimating cardinality
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    /// Indexes, associated with fields
    pub field_indexes: IndexesMap,
    config: PayloadConfig,
    /// Root of index persistence dir
    path: PathBuf,
    /// Used to select unique point ids
    visited_pool: VisitedPool,
    db: Arc<RwLock<DB>>,
}

impl StructPayloadIndex {
    pub fn estimate_field_condition(
        &self,
        condition: &FieldCondition,
        nested_path: Option<&JsonPath>,
    ) -> Option<CardinalityEstimation> {
        let full_path = JsonPath::extend_or_new(nested_path, &condition.key);
        self.field_indexes.get(&full_path).and_then(|indexes| {
            // rewrite condition with fullpath to enable cardinality estimation
            let full_path_condition = FieldCondition {
                key: full_path,
                ..condition.clone()
            };

            indexes
                .iter()
                .find_map(|index| index.estimate_cardinality(&full_path_condition).ok())
        })
    }

    fn query_field<'a>(
        &'a self,
        field_condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let indexes = self
            .field_indexes
            .get(&field_condition.key)
            .and_then(|indexes| {
                indexes
                    .iter()
                    .find_map(|field_index| field_index.filter(field_condition).ok())
            });
        indexes
    }

    fn config_path(&self) -> PathBuf {
        PayloadConfig::get_config_path(&self.path)
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = self.config_path();
        self.config.save(&config_path)
    }

    fn load_all_fields(&mut self, is_appendable: bool) -> OperationResult<()> {
        let mut field_indexes: IndexesMap = Default::default();

        for (field, payload_schema) in &self.config.indexed_fields {
            let field_index = self.load_from_db(field, payload_schema.to_owned(), is_appendable)?;
            field_indexes.insert(field.clone(), field_index);
        }
        self.field_indexes = field_indexes;
        Ok(())
    }

    fn load_from_db(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: PayloadFieldSchema,
        is_appendable: bool,
    ) -> OperationResult<Vec<FieldIndex>> {
        let mut indexes = index_selector(field, &payload_schema, self.db.clone(), is_appendable);

        let mut is_loaded = true;
        for ref mut index in indexes.iter_mut() {
            if !index.load()? {
                is_loaded = false;
                break;
            }
        }
        if !is_loaded {
            debug!("Index for `{field}` was not loaded. Building...");
            // todo(ivan): decide what to do with indexes, which were not loaded
            indexes = self.build_field_indexes(field, payload_schema)?;
        }

        Ok(indexes)
    }

    pub fn open(
        payload: Arc<AtomicRefCell<PayloadStorageEnum>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        path: &Path,
        is_appendable: bool,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let config_path = PayloadConfig::get_config_path(path);
        let config = if config_path.exists() {
            PayloadConfig::load(&config_path)?
        } else {
            PayloadConfig::default()
        };

        let db = open_db_with_existing_cf(path)
            .map_err(|err| OperationError::service_error(format!("RocksDB open error: {err}")))?;

        let mut index = StructPayloadIndex {
            payload,
            id_tracker,
            field_indexes: Default::default(),
            config,
            path: path.to_owned(),
            visited_pool: Default::default(),
            db,
        };

        if !index.config_path().exists() {
            // Save default config
            index.save_config()?;
        }

        index.load_all_fields(is_appendable)?;

        Ok(index)
    }

    pub fn build_field_indexes(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndex>> {
        let payload_storage = self.payload.borrow();
        let mut field_indexes = index_selector(field, &payload_schema, self.db.clone(), true);
        for index in &field_indexes {
            index.recreate()?;
        }

        payload_storage.iter(|point_id, point_payload| {
            let field_value = &point_payload.get_value(field);
            for field_index in field_indexes.iter_mut() {
                field_index.add_point(point_id, field_value)?;
            }
            Ok(true)
        })?;
        Ok(field_indexes)
    }

    fn build_and_save(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_schema: PayloadFieldSchema,
    ) -> OperationResult<()> {
        let field_indexes = self.build_field_indexes(field, payload_schema)?;
        self.field_indexes.insert(field.clone(), field_indexes);
        Ok(())
    }

    /// Number of available points
    ///
    /// - excludes soft deleted points
    pub fn available_point_count(&self) -> usize {
        self.id_tracker.borrow().available_point_count()
    }

    fn struct_filtered_context<'a>(&'a self, filter: &'a Filter) -> StructFilterContext<'a> {
        let estimator = |condition: &Condition| self.condition_cardinality(condition, None);
        let id_tracker = self.id_tracker.borrow();
        let payload_provider = PayloadProvider::new(self.payload.clone());
        StructFilterContext::new(
            filter,
            id_tracker.deref(),
            payload_provider,
            &self.field_indexes,
            &estimator,
            self.available_point_count(),
        )
    }

    fn condition_cardinality(
        &self,
        condition: &Condition,
        nested_path: Option<&JsonPath>,
    ) -> CardinalityEstimation {
        match condition {
            Condition::Filter(_) => panic!("Unexpected branching"),
            Condition::Nested(nested) => {
                // propagate complete nested path in case of multiple nested layers
                let full_path = JsonPath::extend_or_new(nested_path, &nested.array_key());
                self.estimate_nested_cardinality(nested.filter(), &full_path)
            }
            Condition::IsEmpty(IsEmptyCondition { is_empty: field }) => {
                let available_points = self.available_point_count();
                let full_path = JsonPath::extend_or_new(nested_path, &field.key);

                let mut indexed_points = 0;
                if let Some(field_indexes) = self.field_indexes.get(&full_path) {
                    for index in field_indexes {
                        indexed_points = indexed_points.max(index.count_indexed_points())
                    }
                    CardinalityEstimation {
                        primary_clauses: vec![PrimaryCondition::IsEmpty(IsEmptyCondition {
                            is_empty: PayloadField { key: full_path },
                        })],
                        min: 0, // It is possible, that some non-empty payloads are not indexed
                        exp: available_points.saturating_sub(indexed_points), // Expect field type consistency
                        max: available_points.saturating_sub(indexed_points),
                    }
                } else {
                    CardinalityEstimation {
                        primary_clauses: vec![PrimaryCondition::IsEmpty(IsEmptyCondition {
                            is_empty: PayloadField { key: full_path },
                        })],
                        min: 0,
                        exp: available_points / 2,
                        max: available_points,
                    }
                }
            }
            Condition::IsNull(IsNullCondition { is_null: field }) => {
                let available_points = self.available_point_count();
                let full_path = JsonPath::extend_or_new(nested_path, &field.key);

                let mut indexed_points = 0;
                if let Some(field_indexes) = self.field_indexes.get(&full_path) {
                    for index in field_indexes {
                        indexed_points = indexed_points.max(index.count_indexed_points())
                    }
                    CardinalityEstimation {
                        primary_clauses: vec![PrimaryCondition::IsNull(IsNullCondition {
                            is_null: PayloadField { key: full_path },
                        })],
                        min: 0,
                        exp: available_points.saturating_sub(indexed_points),
                        max: available_points.saturating_sub(indexed_points),
                    }
                } else {
                    CardinalityEstimation {
                        primary_clauses: vec![PrimaryCondition::IsNull(IsNullCondition {
                            is_null: PayloadField { key: full_path },
                        })],
                        min: 0,
                        exp: available_points / 2,
                        max: available_points,
                    }
                }
            }
            Condition::HasId(has_id) => {
                let id_tracker_ref = self.id_tracker.borrow();
                let mapped_ids: HashSet<PointOffsetType> = has_id
                    .has_id
                    .iter()
                    .filter_map(|external_id| id_tracker_ref.internal_id(*external_id))
                    .collect();
                let num_ids = mapped_ids.len();
                CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Ids(mapped_ids)],
                    min: num_ids,
                    exp: num_ids,
                    max: num_ids,
                }
            }
            Condition::Field(field_condition) => self
                .estimate_field_condition(field_condition, nested_path)
                .unwrap_or_else(|| CardinalityEstimation::unknown(self.available_point_count())),
        }
    }

    pub fn get_telemetry_data(&self) -> Vec<PayloadIndexTelemetry> {
        self.field_indexes
            .iter()
            .flat_map(|(name, field)| -> Vec<PayloadIndexTelemetry> {
                field
                    .iter()
                    .map(|field| field.get_telemetry_data().set_name(name.to_string()))
                    .collect()
            })
            .collect()
    }

    pub fn restore_database_snapshot(
        snapshot_path: &Path,
        segment_path: &Path,
    ) -> OperationResult<()> {
        crate::rocksdb_backup::restore(snapshot_path, &segment_path.join("payload_index"))
    }
}

impl PayloadIndex for StructPayloadIndex {
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
            // the field is already indexed with the same schema
            // no need to rebuild index and to save the config
            if prev_schema == payload_schema {
                return Ok(());
            }
        }
        self.build_and_save(field, payload_schema)?;
        self.save_config()?;

        Ok(())
    }

    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()> {
        self.config.indexed_fields.remove(field);
        let removed_indexes = self.field_indexes.remove(field);

        if let Some(indexes) = removed_indexes {
            for index in indexes {
                index.clear()?;
            }
        }

        self.save_config()?;
        Ok(())
    }

    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation {
        let available_points = self.available_point_count();
        let estimator = |condition: &Condition| self.condition_cardinality(condition, None);
        estimate_filter(&estimator, query, available_points)
    }

    fn estimate_nested_cardinality(
        &self,
        query: &Filter,
        nested_path: &JsonPath,
    ) -> CardinalityEstimation {
        let available_points = self.available_point_count();
        let estimator =
            |condition: &Condition| self.condition_cardinality(condition, Some(nested_path));
        estimate_filter(&estimator, query, available_points)
    }

    fn query_points(&self, query: &Filter) -> Vec<PointOffsetType> {
        // Assume query is already estimated to be small enough so we can iterate over all matched ids

        let query_cardinality = self.estimate_cardinality(query);

        if query_cardinality.primary_clauses.is_empty() {
            let full_scan_iterator =
                ArcAtomicRefCellIterator::new(self.id_tracker.clone(), |points_iterator| {
                    points_iterator.iter_ids()
                });

            let struct_filtered_context = self.struct_filtered_context(query);
            // Worst case: query expected to return few matches, but index can't be used
            let matched_points =
                full_scan_iterator.filter(move |i| struct_filtered_context.check(*i));

            matched_points.collect()
        } else {
            let points_iterator_ref = self.id_tracker.borrow();
            let struct_filtered_context = self.struct_filtered_context(query);

            // CPU-optimized strategy here: points are made unique before applying other filters.
            // TODO: Implement iterator which holds the `visited_pool` and borrowed `vector_storage_ref` to prevent `preselected` array creation
            let mut visited_list = self
                .visited_pool
                .get(points_iterator_ref.total_point_count());

            let preselected: Vec<PointOffsetType> = query_cardinality
                .primary_clauses
                .iter()
                .flat_map(|clause| {
                    match clause {
                        PrimaryCondition::Condition(field_condition) => {
                            self.query_field(field_condition).unwrap_or_else(
                                || points_iterator_ref.iter_ids(), /* index is not built */
                            )
                        }
                        PrimaryCondition::Ids(ids) => Box::new(ids.iter().copied()),
                        PrimaryCondition::IsEmpty(_) => points_iterator_ref.iter_ids(), /* there are no fast index for IsEmpty */
                        PrimaryCondition::IsNull(_) => points_iterator_ref.iter_ids(),  /* no fast index for IsNull too */
                    }
                })
                .filter(|&id| !visited_list.check_and_update_visited(id))
                .filter(move |&i| struct_filtered_context.check(i))
                .collect();
            preselected
        }
    }

    fn indexed_points(&self, field: PayloadKeyTypeRef) -> usize {
        self.field_indexes.get(field).map_or(0, |indexes| {
            // Assume that multiple field indexes are applied to the same data type,
            // so the points indexed with those indexes are the same.
            // We will return minimal number as a worst case, to highlight possible errors in the index early.
            indexes
                .iter()
                .map(|index| index.count_indexed_points())
                .min()
                .unwrap_or(0)
        })
    }

    fn filter_context<'a>(&'a self, filter: &'a Filter) -> Box<dyn FilterContext + 'a> {
        Box::new(self.struct_filtered_context(filter))
    }

    fn payload_blocks(
        &self,
        field: PayloadKeyTypeRef,
        threshold: usize,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        match self.field_indexes.get(field) {
            None => Box::new(vec![].into_iter()),
            Some(indexes) => {
                let field_clone = field.to_owned();
                Box::new(indexes.iter().flat_map(move |field_index| {
                    field_index.payload_blocks(threshold, field_clone.clone())
                }))
            }
        }
    }

    fn assign(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &Option<JsonPath>,
    ) -> OperationResult<()> {
        if let Some(key) = key {
            self.payload
                .borrow_mut()
                .assign_by_key(point_id, payload, key)?;
        } else {
            self.payload.borrow_mut().assign(point_id, payload)?;
        };

        let updated_payload = self.payload(point_id)?;
        for (field, field_index) in &mut self.field_indexes {
            let field_value = updated_payload.get_value(field);
            if !field_value.is_empty() {
                for index in field_index {
                    index.add_point(point_id, &field_value)?;
                }
            } else {
                for index in field_index {
                    index.remove_point(point_id)?;
                }
            }
        }
        Ok(())
    }

    fn payload(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        self.payload.borrow().payload(point_id)
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Vec<Value>> {
        if let Some(indexes) = self.field_indexes.get_mut(key) {
            for index in indexes {
                index.remove_point(point_id)?;
            }
        }
        self.payload.borrow_mut().delete(point_id, key)
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        for (_, field_indexes) in self.field_indexes.iter_mut() {
            for index in field_indexes {
                index.remove_point(point_id)?;
            }
        }
        self.payload.borrow_mut().drop(point_id)
    }

    fn flusher(&self) -> Flusher {
        let mut flushers = Vec::new();
        for field_indexes in self.field_indexes.values() {
            for index in field_indexes {
                flushers.push(index.flusher());
            }
        }
        flushers.push(self.payload.borrow().flusher());
        Box::new(move || {
            for flusher in flushers {
                flusher()?
            }
            Ok(())
        })
    }

    fn infer_payload_type(
        &self,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<PayloadSchemaType>> {
        let mut schema = None;
        self.payload.borrow().iter(|_id, payload: &Payload| {
            let field_value = payload.get_value(key);
            schema = match field_value.as_slice() {
                [] => None,
                [single] => infer_value_type(single),
                multiple => infer_collection_value_type(multiple.iter().copied()),
            };
            Ok(false)
        })?;
        Ok(schema)
    }

    fn take_database_snapshot(&self, path: &Path) -> OperationResult<()> {
        crate::rocksdb_backup::create(&self.db.read(), path)
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.config_path()]
    }
}
