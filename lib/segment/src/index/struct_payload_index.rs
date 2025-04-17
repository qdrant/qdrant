use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::AHashSet;
use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::flags::feature_flags;
use common::types::PointOffsetType;
use itertools::Either;
use log::debug;
use parking_lot::RwLock;
use rocksdb::DB;
use schemars::_serde_json::Value;

use super::field_index::FieldIndexBuilderTrait as _;
use super::field_index::facet_index::FacetIndexEnum;
use super::field_index::index_selector::{IndexSelector, IndexSelectorMmap, IndexSelectorRocksDb};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
use crate::common::utils::IndexesMap;
use crate::id_tracker::IdTrackerSS;
use crate::index::PayloadIndex;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PrimaryCondition,
};
use crate::index::payload_config::PayloadConfig;
use crate::index::query_estimator::estimate_filter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_filter_context::StructFilterContext;
use crate::index::visited_pool::VisitedPool;
use crate::json_path::JsonPath;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::{FilterContext, PayloadStorage};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition, Payload,
    PayloadContainer, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
    VectorNameBuf, infer_collection_value_type, infer_value_type,
};
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

#[derive(Debug)]
enum StorageType {
    Appendable(Arc<RwLock<DB>>),
    NonAppendableRocksDb(Arc<RwLock<DB>>),
    NonAppendable,
}

/// `PayloadIndex` implementation, which actually uses index structures for providing faster search
#[derive(Debug)]
pub struct StructPayloadIndex {
    /// Payload storage
    pub(super) payload: Arc<AtomicRefCell<PayloadStorageEnum>>,
    /// Used for `has_id` condition and estimating cardinality
    pub(super) id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    /// Vector storages for each field, used for `has_vector` condition
    pub(super) vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
    /// Indexes, associated with fields
    pub field_indexes: IndexesMap,
    config: PayloadConfig,
    /// Root of index persistence dir
    path: PathBuf,
    /// Used to select unique point ids
    visited_pool: VisitedPool,
    storage_type: StorageType,
}

impl StructPayloadIndex {
    pub fn estimate_field_condition(
        &self,
        condition: &FieldCondition,
        nested_path: Option<&JsonPath>,
        hw_counter: &HardwareCounterCell,
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
                .find_map(|index| index.estimate_cardinality(&full_path_condition, hw_counter))
        })
    }

    fn query_field<'a>(
        &'a self,
        condition: &'a PrimaryCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match condition {
            PrimaryCondition::Condition(field_condition) => {
                let field_key = &field_condition.key;
                let field_indexes = self.field_indexes.get(field_key)?;
                field_indexes
                    .iter()
                    .find_map(|field_index| field_index.filter(field_condition, hw_counter))
            }
            PrimaryCondition::Ids(ids) => Some(Box::new(ids.iter().copied())),
            PrimaryCondition::HasVector(_) => None,
        }
    }

    fn config_path(&self) -> PathBuf {
        PayloadConfig::get_config_path(&self.path)
    }

    fn save_config(&self) -> OperationResult<()> {
        let config_path = self.config_path();
        self.config.save(&config_path)
    }

    fn load_all_fields(&mut self) -> OperationResult<()> {
        let mut field_indexes: IndexesMap = Default::default();

        for (field, payload_schema) in &self.config.indexed_fields {
            let field_index = self.load_from_db(field, payload_schema)?;
            field_indexes.insert(field.clone(), field_index);
        }
        self.field_indexes = field_indexes;
        Ok(())
    }

    fn load_from_db(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Vec<FieldIndex>> {
        let mut indexes = self
            .selector(payload_schema)
            .new_index(field, payload_schema)?;

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
            indexes = self.build_field_indexes(
                field,
                payload_schema,
                &HardwareCounterCell::disposable(), // Internal operation.
            )?;
        }

        Ok(indexes)
    }

    pub fn open(
        payload: Arc<AtomicRefCell<PayloadStorageEnum>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
        path: &Path,
        is_appendable: bool,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;
        let config_path = PayloadConfig::get_config_path(path);
        let config = if config_path.exists() {
            PayloadConfig::load(&config_path)?
        } else {
            let mut new_config = PayloadConfig::default();
            if feature_flags().payload_index_skip_rocksdb && !is_appendable {
                new_config.skip_rocksdb = Some(true);
            }
            new_config
        };

        let skip_rocksdb = config.skip_rocksdb.unwrap_or(false);

        let storage_type = if is_appendable {
            let db = open_db_with_existing_cf(path).map_err(|err| {
                OperationError::service_error(format!("RocksDB open error: {err}"))
            })?;
            StorageType::Appendable(db)
        } else if skip_rocksdb {
            StorageType::NonAppendable
        } else {
            let db = open_db_with_existing_cf(path).map_err(|err| {
                OperationError::service_error(format!("RocksDB open error: {err}"))
            })?;
            StorageType::NonAppendableRocksDb(db)
        };

        let mut index = StructPayloadIndex {
            payload,
            id_tracker,
            vector_storages,
            field_indexes: Default::default(),
            config,
            path: path.to_owned(),
            visited_pool: Default::default(),
            storage_type,
        };

        if !index.config_path().exists() {
            // Save default config
            index.save_config()?;
        }

        index.load_all_fields()?;

        Ok(index)
    }

    pub fn build_field_indexes(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<FieldIndex>> {
        let payload_storage = self.payload.borrow();
        let mut builders = self
            .selector(payload_schema)
            .index_builder(field, payload_schema)?;

        for index in &mut builders {
            index.init()?;
        }

        payload_storage.iter(
            |point_id, point_payload| {
                let field_value = &point_payload.get_value(field);
                for builder in builders.iter_mut() {
                    builder.add_point(point_id, field_value, hw_counter)?;
                }
                Ok(true)
            },
            hw_counter,
        )?;

        builders
            .into_iter()
            .map(|builder| builder.finalize())
            .collect()
    }

    /// Number of available points
    ///
    /// - excludes soft deleted points
    pub fn available_point_count(&self) -> usize {
        self.id_tracker.borrow().available_point_count()
    }

    pub fn struct_filtered_context<'a>(
        &'a self,
        filter: &'a Filter,
        hw_counter: &HardwareCounterCell,
    ) -> StructFilterContext<'a> {
        let payload_provider = PayloadProvider::new(self.payload.clone());

        let (optimized_filter, _) = self.optimize_filter(
            filter,
            payload_provider,
            self.available_point_count(),
            hw_counter,
        );

        StructFilterContext::new(optimized_filter)
    }

    pub(super) fn condition_cardinality(
        &self,
        condition: &Condition,
        nested_path: Option<&JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        match condition {
            Condition::Filter(_) => panic!("Unexpected branching"),
            Condition::Nested(nested) => {
                // propagate complete nested path in case of multiple nested layers
                let full_path = JsonPath::extend_or_new(nested_path, &nested.array_key());
                self.estimate_nested_cardinality(nested.filter(), &full_path, hw_counter)
            }
            Condition::IsEmpty(IsEmptyCondition { is_empty: field }) => {
                let available_points = self.available_point_count();
                let condition = FieldCondition::new_is_empty(field.key.clone());

                self.estimate_field_condition(&condition, nested_path, hw_counter)
                    .unwrap_or_else(|| CardinalityEstimation::unknown(available_points))
            }
            Condition::IsNull(IsNullCondition { is_null: field }) => {
                let available_points = self.available_point_count();
                let condition = FieldCondition::new_is_null(field.key.clone());

                self.estimate_field_condition(&condition, nested_path, hw_counter)
                    .unwrap_or_else(|| CardinalityEstimation::unknown(available_points))
            }
            Condition::HasId(has_id) => {
                let id_tracker_ref = self.id_tracker.borrow();
                let mapped_ids: AHashSet<PointOffsetType> = has_id
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
            Condition::HasVector(has_vectors) => {
                if let Some(vector_storage) = self.vector_storages.get(&has_vectors.has_vector) {
                    let vector_storage = vector_storage.borrow();
                    let vectors = vector_storage.available_vector_count();
                    CardinalityEstimation::exact(vectors).with_primary_clause(
                        PrimaryCondition::HasVector(has_vectors.has_vector.clone()),
                    )
                } else {
                    CardinalityEstimation::exact(0)
                }
            }
            Condition::Field(field_condition) => self
                .estimate_field_condition(field_condition, nested_path, hw_counter)
                .unwrap_or_else(|| CardinalityEstimation::unknown(self.available_point_count())),

            Condition::CustomIdChecker(cond) => {
                cond.estimate_cardinality(self.id_tracker.borrow().available_point_count())
            }
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

    fn clear_index_for_point(&mut self, point_id: PointOffsetType) -> OperationResult<()> {
        for (_, field_indexes) in self.field_indexes.iter_mut() {
            for index in field_indexes {
                index.remove_point(point_id)?;
            }
        }
        Ok(())
    }
    pub fn config(&self) -> &PayloadConfig {
        &self.config
    }

    pub fn iter_filtered_points<'a>(
        &'a self,
        filter: &'a Filter,
        id_tracker: &'a IdTrackerSS,
        query_cardinality: &'a CardinalityEstimation,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        let struct_filtered_context = self.struct_filtered_context(filter, hw_counter);

        if query_cardinality.primary_clauses.is_empty() {
            let full_scan_iterator = id_tracker.iter_ids();

            // Worst case: query expected to return few matches, but index can't be used
            let matched_points =
                full_scan_iterator.filter(move |i| struct_filtered_context.check(*i));

            Either::Left(matched_points)
        } else {
            // CPU-optimized strategy here: points are made unique before applying other filters.
            let mut visited_list = self.visited_pool.get(id_tracker.total_point_count());

            let iter = query_cardinality
                .primary_clauses
                .iter()
                .flat_map(move |clause| {
                    self.query_field(clause, hw_counter).unwrap_or_else(|| {
                        // index is not built
                        Box::new(id_tracker.iter_ids().measure_hw_with_cell(
                            hw_counter,
                            size_of::<PointOffsetType>(),
                            |i| i.cpu_counter(),
                        ))
                    })
                })
                .filter(move |&id| !visited_list.check_and_update_visited(id))
                .filter(move |&i| struct_filtered_context.check(i));

            Either::Right(iter)
        }
    }

    /// Select which type of PayloadIndex to use for the field
    fn selector(&self, payload_schema: &PayloadFieldSchema) -> IndexSelector {
        let is_on_disk = payload_schema.is_on_disk();

        match &self.storage_type {
            StorageType::Appendable(db) => IndexSelector::RocksDb(IndexSelectorRocksDb {
                db,
                is_appendable: true,
            }),
            StorageType::NonAppendableRocksDb(db) => {
                // legacy logic: we keep rocksdb, but load mmap indexes
                if is_on_disk {
                    IndexSelector::Mmap(IndexSelectorMmap {
                        dir: &self.path,
                        is_on_disk,
                    })
                } else {
                    IndexSelector::RocksDb(IndexSelectorRocksDb {
                        db,
                        is_appendable: false,
                    })
                }
            }
            StorageType::NonAppendable => IndexSelector::Mmap(IndexSelectorMmap {
                dir: &self.path,
                is_on_disk,
            }),
        }
    }

    pub fn get_facet_index(&self, key: &JsonPath) -> OperationResult<FacetIndexEnum> {
        self.field_indexes
            .get(key)
            .and_then(|index| index.iter().find_map(|index| index.as_facet_index()))
            .ok_or_else(|| OperationError::MissingMapIndexForFacet {
                key: key.to_string(),
            })
    }

    pub fn populate(&self) -> OperationResult<()> {
        for (_, field_indexes) in self.field_indexes.iter() {
            for index in field_indexes {
                index.populate()?;
            }
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        for (_, field_indexes) in self.field_indexes.iter() {
            for index in field_indexes {
                index.clear_cache()?;
            }
        }
        Ok(())
    }

    pub fn clear_cache_if_on_disk(&self) -> OperationResult<()> {
        for (_, field_indexes) in self.field_indexes.iter() {
            for index in field_indexes {
                if index.is_on_disk() {
                    index.clear_cache()?;
                }
            }
        }
        Ok(())
    }
}

impl PayloadIndex for StructPayloadIndex {
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.config.indexed_fields.clone()
    }

    fn build_index(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Vec<FieldIndex>>> {
        if let Some(prev_schema) = self.config.indexed_fields.get(field) {
            // the field is already indexed with the same schema
            // no need to rebuild index and to save the config
            if prev_schema == payload_schema {
                return Ok(None);
            }
        }

        let indexes = self.build_field_indexes(field, payload_schema, hw_counter)?;

        Ok(Some(indexes))
    }

    fn apply_index(
        &mut self,
        field: PayloadKeyType,
        payload_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<()> {
        self.field_indexes.insert(field.clone(), field_index);

        self.config.indexed_fields.insert(field, payload_schema);

        self.save_config()?;

        Ok(())
    }

    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()> {
        self.config.indexed_fields.remove(field);
        let removed_indexes = self.field_indexes.remove(field);

        if let Some(indexes) = removed_indexes {
            for index in indexes {
                index.cleanup()?;
            }
        }

        self.save_config()?;
        Ok(())
    }

    fn estimate_cardinality(
        &self,
        query: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let available_points = self.available_point_count();
        let estimator =
            |condition: &Condition| self.condition_cardinality(condition, None, hw_counter);
        estimate_filter(&estimator, query, available_points)
    }

    fn estimate_nested_cardinality(
        &self,
        query: &Filter,
        nested_path: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let available_points = self.available_point_count();
        let estimator = |condition: &Condition| {
            self.condition_cardinality(condition, Some(nested_path), hw_counter)
        };
        estimate_filter(&estimator, query, available_points)
    }

    fn query_points(
        &self,
        query: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointOffsetType> {
        // Assume query is already estimated to be small enough so we can iterate over all matched ids
        let query_cardinality = self.estimate_cardinality(query, hw_counter);
        let id_tracker = self.id_tracker.borrow();
        self.iter_filtered_points(query, &*id_tracker, &query_cardinality, hw_counter)
            .collect()
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

    fn filter_context<'a>(
        &'a self,
        filter: &'a Filter,
        hw_counter: &HardwareCounterCell,
    ) -> Box<dyn FilterContext + 'a> {
        Box::new(self.struct_filtered_context(filter, hw_counter))
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

    fn overwrite_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.payload
            .borrow_mut()
            .overwrite(point_id, payload, hw_counter)?;

        for (field, field_index) in &mut self.field_indexes {
            let field_value = payload.get_value(field);
            if !field_value.is_empty() {
                for index in field_index {
                    index.add_point(point_id, &field_value, hw_counter)?;
                }
            } else {
                for index in field_index {
                    index.remove_point(point_id)?;
                }
            }
        }
        Ok(())
    }

    fn set_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &Option<JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let Some(key) = key {
            self.payload
                .borrow_mut()
                .set_by_key(point_id, payload, key, hw_counter)?;
        } else {
            self.payload
                .borrow_mut()
                .set(point_id, payload, hw_counter)?;
        };

        let updated_payload = self.get_payload(point_id, hw_counter)?;
        for (field, field_index) in &mut self.field_indexes {
            if !field.is_affected_by_value_set(&payload.0, key.as_ref()) {
                continue;
            }
            let field_value = updated_payload.get_value(field);
            if !field_value.is_empty() {
                for index in field_index {
                    index.add_point(point_id, &field_value, hw_counter)?;
                }
            } else {
                for index in field_index {
                    index.remove_point(point_id)?;
                }
            }
        }
        Ok(())
    }

    fn get_payload(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.payload.borrow().get(point_id, hw_counter)
    }

    fn delete_payload(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        if let Some(indexes) = self.field_indexes.get_mut(key) {
            for index in indexes {
                index.remove_point(point_id)?;
            }
        }
        self.payload.borrow_mut().delete(point_id, key, hw_counter)
    }

    fn clear_payload(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        self.clear_index_for_point(point_id)?;
        self.payload.borrow_mut().clear(point_id, hw_counter)
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
                match flusher() {
                    Ok(_) => {}
                    Err(OperationError::RocksDbColumnFamilyNotFound { name }) => {
                        // It is possible, that the index was removed during the flush by user or another thread.
                        // In this case, non-existing column family is not an error, but an expected behavior.

                        // Still we want to log this event, for potential debugging.
                        log::warn!(
                            "Flush: RocksDB cf_handle error: Cannot find column family {name}. Assume index is removed.",
                        );
                    }
                    Err(err) => {
                        return Err(OperationError::service_error(format!(
                            "Failed to flush payload_index: {err}"
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    fn infer_payload_type(
        &self,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PayloadSchemaType>> {
        let mut schema = None;
        self.payload.borrow().iter(
            |_id, payload: &Payload| {
                let field_value = payload.get_value(key);
                schema = match field_value.as_slice() {
                    [] => None,
                    [single] => infer_value_type(single),
                    multiple => infer_collection_value_type(multiple.iter().copied()),
                };
                Ok(false)
            },
            hw_counter,
        )?;
        Ok(schema)
    }

    fn take_database_snapshot(&self, path: &Path) -> OperationResult<()> {
        match &self.storage_type {
            StorageType::Appendable(db) => {
                let db_guard = db.read();
                crate::rocksdb_backup::create(&db_guard, path)
            }
            StorageType::NonAppendableRocksDb(db) => {
                let db_guard = db.read();
                crate::rocksdb_backup::create(&db_guard, path)
            }
            StorageType::NonAppendable => Ok(()),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self
            .field_indexes
            .values()
            .flat_map(|indexes| indexes.iter().flat_map(|index| index.files().into_iter()))
            .collect::<Vec<PathBuf>>();
        files.push(self.config_path());
        files
    }
}
