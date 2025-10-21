use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::either_variant::EitherVariant;
use common::types::PointOffsetType;
use fs_err as fs;
use schemars::_serde_json::Value;

use super::field_index::facet_index::FacetIndexEnum;
#[cfg(feature = "rocksdb")]
use super::field_index::index_selector::IndexSelectorRocksDb;
use super::field_index::index_selector::{
    IndexSelector, IndexSelectorGridstore, IndexSelectorMmap,
};
use super::field_index::{FieldIndexBuilderTrait as _, ResolvedHasId};
use super::payload_config::{FullPayloadIndexType, PayloadFieldSchemaWithIndexType};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::IndexesMap;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PrimaryCondition,
};
use crate::index::payload_config::{self, PayloadConfig};
use crate::index::query_estimator::estimate_filter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_filter_context::StructFilterContext;
use crate::index::visited_pool::VisitedPool;
use crate::index::{BuildIndexResult, PayloadIndex};
use crate::json_path::JsonPath;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::{FilterContext, PayloadStorage};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition, Payload,
    PayloadContainer, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, VectorNameBuf,
};
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum StorageType {
    #[cfg(feature = "rocksdb")]
    RocksDbAppendable(std::sync::Arc<parking_lot::RwLock<rocksdb::DB>>),
    GridstoreAppendable,
    #[cfg(feature = "rocksdb")]
    RocksDbNonAppendable(Arc<parking_lot::RwLock<rocksdb::DB>>),
    GridstoreNonAppendable,
}

impl StorageType {
    #[cfg(feature = "rocksdb")]
    pub fn is_appendable(&self) -> bool {
        match self {
            StorageType::RocksDbAppendable(_) => true,
            StorageType::GridstoreAppendable => true,
            StorageType::RocksDbNonAppendable(_) => false,
            StorageType::GridstoreNonAppendable => false,
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self {
            StorageType::RocksDbAppendable(_) => true,
            StorageType::RocksDbNonAppendable(_) => true,
            StorageType::GridstoreAppendable => false,
            StorageType::GridstoreNonAppendable => false,
        }
    }
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
    /// Desired storage type for payload indices, used in builder to pick correct type
    storage_type: StorageType,
    /// RocksDB instance, if any index is using it
    #[cfg(feature = "rocksdb")]
    db: Option<Arc<parking_lot::RwLock<rocksdb::DB>>>,
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
            PrimaryCondition::Ids(ids) => {
                Some(Box::new(ids.resolved_point_offsets.iter().copied()))
            }
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

    fn load_all_fields(&mut self, create_if_missing: bool) -> OperationResult<()> {
        let mut field_indexes: IndexesMap = Default::default();

        let mut indices = std::mem::take(&mut self.config.indices);
        let mut is_dirty = false;

        for (field, payload_schema) in indices.iter_mut() {
            let (field_index, dirty) =
                self.load_from_db(field, payload_schema, create_if_missing)?;
            field_indexes.insert(field.clone(), field_index);
            is_dirty |= dirty;
        }

        // Put updated payload schemas back into the config
        self.config.indices = indices;

        if is_dirty {
            self.save_config()?;
        }

        self.field_indexes = field_indexes;
        Ok(())
    }

    #[cfg_attr(not(feature = "rocksdb"), allow(clippy::needless_pass_by_ref_mut))]
    fn load_from_db(
        &mut self,
        field: PayloadKeyTypeRef,
        // TODO: refactor this and remove the &mut reference.
        payload_schema: &mut PayloadFieldSchemaWithIndexType,
        create_if_missing: bool,
    ) -> OperationResult<(Vec<FieldIndex>, bool)> {
        let total_point_count = self.id_tracker.borrow().total_point_count();
        let mut rebuild = false;
        let mut is_dirty = false;

        let mut indexes = if payload_schema.types.is_empty() {
            let indexes = self.selector(&payload_schema.schema).new_index(
                field,
                &payload_schema.schema,
                create_if_missing,
            )?;

            if let Some(mut indexes) = indexes {
                debug_assert!(
                    !indexes
                        .iter()
                        .any(|index| matches!(index, FieldIndex::NullIndex(_))),
                    "index selector is not expected to provide null index",
                );

                // Special null index complements every index.
                if let Some(null_index) = IndexSelector::new_null_index(
                    &self.path,
                    field,
                    total_point_count,
                    create_if_missing,
                )? {
                    indexes.push(null_index);
                }

                // Persist exact payload index types
                is_dirty = true;
                payload_schema.types = indexes.iter().map(|i| i.get_full_index_type()).collect();

                indexes
            } else {
                rebuild = true;
                vec![]
            }
        } else {
            payload_schema
                .types
                .iter()
                // Load each index
                .map(|index| {
                    self.selector_with_type(index).and_then(|selector| {
                        selector.new_index_with_type(
                            field,
                            &payload_schema.schema,
                            index,
                            &self.path,
                            total_point_count,
                            create_if_missing,
                        )
                    })
                })
                // Interrupt loading indices if one fails to load
                // Set rebuild flag if any index fails to load
                .take_while(|index| {
                    let is_loaded = index.as_ref().is_ok_and(|index| index.is_some());
                    rebuild |= !is_loaded;
                    is_loaded
                })
                .filter_map(|index| index.transpose())
                .collect::<OperationResult<Vec<_>>>()?
        };

        // Actively migrate away from RocksDB indices
        // Naively implemented by just rebuilding the indices from scratch
        #[cfg(feature = "rocksdb")]
        if common::flags::feature_flags().migrate_rocksdb_payload_indices
            && indexes.iter().any(|index| index.is_rocksdb())
        {
            log::info!("Migrating away from RocksDB indices for field `{field}`");

            rebuild = true;
            is_dirty = true;

            // Change storage type, set skip RocksDB flag and persist
            // Needed to not use RocksDB when rebuilding indices below
            match self.storage_type {
                StorageType::RocksDbAppendable(_) => {
                    self.storage_type = StorageType::GridstoreAppendable;
                }
                StorageType::GridstoreAppendable => {}
                StorageType::RocksDbNonAppendable(_) => {
                    self.storage_type = StorageType::GridstoreNonAppendable;
                }
                StorageType::GridstoreNonAppendable => {}
            }
            self.config.skip_rocksdb.replace(true);

            // Clean-up all existing indices
            for index in indexes.drain(..) {
                index.cleanup().map_err(|err| {
                    OperationError::service_error(format!(
                        "Failed to clean up payload index for field `{field}` before rebuild: {err}"
                    ))
                })?;
            }
        }

        // If index is not properly loaded or when migrating, rebuild indices
        if rebuild {
            log::debug!("Rebuilding payload index for field `{field}`...");
            indexes = self.build_field_indexes(
                field,
                &payload_schema.schema,
                &HardwareCounterCell::disposable(), // Internal operation
            )?;

            // Persist exact payload index types of newly built indices
            is_dirty = true;
            payload_schema.types = indexes.iter().map(|i| i.get_full_index_type()).collect();
        }

        Ok((indexes, is_dirty))
    }

    pub fn open(
        payload: Arc<AtomicRefCell<PayloadStorageEnum>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
        path: &Path,
        is_appendable: bool,
        create: bool,
    ) -> OperationResult<Self> {
        fs::create_dir_all(path)?;
        let config_path = PayloadConfig::get_config_path(path);
        let config = if config_path.exists() {
            PayloadConfig::load(&config_path)?
        } else {
            #[cfg(feature = "rocksdb")]
            {
                let mut new_config = PayloadConfig::default();
                let skip_rocksdb = if is_appendable {
                    common::flags::feature_flags().payload_index_skip_mutable_rocksdb
                } else {
                    common::flags::feature_flags().payload_index_skip_rocksdb
                };
                if skip_rocksdb {
                    new_config.skip_rocksdb = Some(true);
                }
                new_config
            }

            #[cfg(not(feature = "rocksdb"))]
            {
                PayloadConfig::default()
            }
        };

        #[cfg(feature = "rocksdb")]
        let mut db = None;
        let storage_type = if is_appendable {
            #[cfg(feature = "rocksdb")]
            {
                let skip_rocksdb = config.skip_rocksdb.unwrap_or(false);
                if !skip_rocksdb {
                    let rocksdb = crate::common::rocksdb_wrapper::open_db_with_existing_cf(path)
                        .map_err(|err| {
                            OperationError::service_error(format!("RocksDB open error: {err}"))
                        })?;
                    db.replace(rocksdb.clone());
                    StorageType::RocksDbAppendable(rocksdb)
                } else {
                    StorageType::GridstoreAppendable
                }
            }
            #[cfg(not(feature = "rocksdb"))]
            {
                StorageType::GridstoreAppendable
            }
        } else {
            #[cfg(feature = "rocksdb")]
            {
                let skip_rocksdb = config.skip_rocksdb.unwrap_or(false);
                if !skip_rocksdb {
                    let rocksdb = crate::common::rocksdb_wrapper::open_db_with_existing_cf(path)
                        .map_err(|err| {
                            OperationError::service_error(format!("RocksDB open error: {err}"))
                        })?;
                    db.replace(rocksdb.clone());
                    StorageType::RocksDbNonAppendable(rocksdb)
                } else {
                    StorageType::GridstoreNonAppendable
                }
            }
            #[cfg(not(feature = "rocksdb"))]
            {
                StorageType::GridstoreNonAppendable
            }
        };

        // Also prematurely open RocksDB if any index is still using it
        #[cfg(feature = "rocksdb")]
        if db.is_none() && config.indices.any_is_rocksdb() {
            log::debug!("Opening RocksDB to load old payload index");
            let rocksdb =
                crate::common::rocksdb_wrapper::open_db_with_existing_cf(path).map_err(|err| {
                    OperationError::service_error(format!("RocksDB open error: {err}"))
                })?;
            db.replace(rocksdb);
        }

        let mut index = StructPayloadIndex {
            payload,
            id_tracker,
            vector_storages,
            field_indexes: Default::default(),
            config,
            path: path.to_owned(),
            visited_pool: Default::default(),
            storage_type,
            #[cfg(feature = "rocksdb")]
            db,
        };

        if !index.config_path().exists() {
            // Save default config
            index.save_config()?;
        }

        index.load_all_fields(create)?;

        // If we have a RocksDB instance, but no index using it, completely delete it here
        #[cfg(feature = "rocksdb")]
        if !index.storage_type.is_rocksdb()
            && !index.config.indices.any_is_rocksdb()
            && let Some(db) = index.db.take()
        {
            match Arc::try_unwrap(db) {
                Ok(db) => {
                    log::trace!(
                        "Deleting RocksDB for payload indices, no payload index uses it anymore"
                    );

                    // Close RocksDB instance
                    let db = db.into_inner();
                    drop(db);

                    // Destroy all RocksDB files
                    let options = crate::common::rocksdb_wrapper::make_db_options();
                    match rocksdb::DB::destroy(&options, &index.path) {
                        Ok(_) => log::debug!("Deleted RocksDB for payload indices"),
                        Err(err) => {
                            log::warn!("Failed to delete RocksDB for payload indices: {err}")
                        }
                    }
                }
                // Here we don't have exclusive ownership of RocksDB, which prevents us from
                // controlling and closing the instance. Because of it, we cannot destroy the
                // RocksDB files, and leave them behind. We don't consider this a problem, because
                // a future optimization run will get rid of these files.
                Err(db) => {
                    log::warn!(
                        "RocksDB for payload indices could not be deleted, does not have exclusive ownership"
                    );
                    index.db.replace(db);
                }
            }
        }

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

        // Special null index complements every index.
        let null_index = IndexSelector::null_builder(&self.path, field)?;
        builders.push(null_index);

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
                let condition = FieldCondition::new_is_empty(field.key.clone(), true);

                self.estimate_field_condition(&condition, nested_path, hw_counter)
                    .unwrap_or_else(|| CardinalityEstimation::unknown(available_points))
            }
            Condition::IsNull(IsNullCondition { is_null: field }) => {
                let available_points = self.available_point_count();
                let condition = FieldCondition::new_is_null(field.key.clone(), true);

                self.estimate_field_condition(&condition, nested_path, hw_counter)
                    .unwrap_or_else(|| CardinalityEstimation::unknown(available_points))
            }
            Condition::HasId(has_id) => {
                let point_ids = has_id.has_id.clone();
                let id_tracker = self.id_tracker.borrow();
                let resolved_point_offsets: Vec<PointOffsetType> = point_ids
                    .iter()
                    .filter_map(|external_id| id_tracker.internal_id(*external_id))
                    .collect();
                let num_ids = resolved_point_offsets.len();
                CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Ids(ResolvedHasId {
                        point_ids,
                        resolved_point_offsets,
                    })],
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

            Condition::CustomIdChecker(cond) => cond
                .0
                .estimate_cardinality(self.id_tracker.borrow().available_point_count()),
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

    #[cfg(feature = "rocksdb")]
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

    pub fn is_tenant(&self, field: &PayloadKeyType) -> bool {
        self.config
            .indices
            .get(field)
            .map(|indexed_field| indexed_field.schema.is_tenant())
            .unwrap_or(false)
    }

    pub fn iter_filtered_points<'a>(
        &'a self,
        filter: &'a Filter,
        id_tracker: &'a IdTrackerSS,
        query_cardinality: &'a CardinalityEstimation,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        if query_cardinality.primary_clauses.is_empty() {
            let full_scan_iterator = id_tracker.iter_internal();
            let struct_filtered_context = self.struct_filtered_context(filter, hw_counter);
            // Worst case: query expected to return few matches, but index can't be used
            let matched_points =
                full_scan_iterator.filter(move |i| struct_filtered_context.check(*i));

            EitherVariant::A(matched_points)
        } else {
            // CPU-optimized strategy here: points are made unique before applying other filters.
            let mut visited_list = self.visited_pool.get(id_tracker.total_point_count());

            // If even one iterator is None, we should replace the whole thing with
            // an iterator over all ids.
            let primary_clause_iterators: Option<Vec<_>> = query_cardinality
                .primary_clauses
                .iter()
                .map(move |clause| self.query_field(clause, hw_counter))
                .collect();

            if let Some(primary_iterators) = primary_clause_iterators {
                let all_conditions_are_primary = filter
                    .iter_conditions()
                    .all(|condition| query_cardinality.is_primary(condition));

                let joined_primary_iterator = primary_iterators.into_iter().flatten();

                return if all_conditions_are_primary {
                    // All conditions are primary clauses,
                    // We can avoid post-filtering
                    let iter = joined_primary_iterator
                        .filter(move |&id| !visited_list.check_and_update_visited(id));
                    EitherVariant::B(iter)
                } else {
                    // Some conditions are primary clauses, some are not
                    let struct_filtered_context = self.struct_filtered_context(filter, hw_counter);
                    let iter = joined_primary_iterator.filter(move |&id| {
                        !visited_list.check_and_update_visited(id)
                            && struct_filtered_context.check(id)
                    });
                    EitherVariant::C(iter)
                };
            }

            // We can't use primary conditions, so we fall back to iterating over all ids
            // and applying full filter.
            let struct_filtered_context = self.struct_filtered_context(filter, hw_counter);

            let iter = id_tracker
                .iter_internal()
                .measure_hw_with_cell(hw_counter, size_of::<PointOffsetType>(), |i| {
                    i.cpu_counter()
                })
                .filter(move |&id| {
                    !visited_list.check_and_update_visited(id) && struct_filtered_context.check(id)
                });

            EitherVariant::D(iter)
        }
    }

    /// Select which type of PayloadIndex to use for the field
    fn selector(&self, payload_schema: &PayloadFieldSchema) -> IndexSelector<'_> {
        let is_on_disk = payload_schema.is_on_disk();

        match &self.storage_type {
            #[cfg(feature = "rocksdb")]
            StorageType::RocksDbAppendable(db) => IndexSelector::RocksDb(IndexSelectorRocksDb {
                db,
                is_appendable: true,
            }),
            StorageType::GridstoreAppendable => {
                IndexSelector::Gridstore(IndexSelectorGridstore { dir: &self.path })
            }
            #[cfg(feature = "rocksdb")]
            StorageType::RocksDbNonAppendable(db) => {
                // legacy logic: we keep rocksdb, but load mmap indexes
                if !is_on_disk {
                    return IndexSelector::RocksDb(IndexSelectorRocksDb {
                        db,
                        is_appendable: false,
                    });
                }

                IndexSelector::Mmap(IndexSelectorMmap {
                    dir: &self.path,
                    is_on_disk,
                })
            }
            StorageType::GridstoreNonAppendable => IndexSelector::Mmap(IndexSelectorMmap {
                dir: &self.path,
                is_on_disk,
            }),
        }
    }

    fn selector_with_type(
        &self,
        index_type: &FullPayloadIndexType,
    ) -> OperationResult<IndexSelector<'_>> {
        let selector = match index_type.storage_type {
            payload_config::StorageType::Gridstore => {
                IndexSelector::Gridstore(IndexSelectorGridstore { dir: &self.path })
            }
            payload_config::StorageType::RocksDb => {
                #[cfg(feature = "rocksdb")]
                {
                    let db = match (&self.storage_type, &self.db) {
                        (
                            StorageType::RocksDbAppendable(db)
                            | StorageType::RocksDbNonAppendable(db),
                            _,
                        ) => db,
                        (
                            StorageType::GridstoreAppendable | StorageType::GridstoreNonAppendable,
                            Some(db),
                        ) => db,
                        (
                            StorageType::GridstoreAppendable | StorageType::GridstoreNonAppendable,
                            None,
                        ) => {
                            return Err(OperationError::service_error(
                                "Loading payload index failed: Configured storage type and payload schema mismatch!",
                            ));
                        }
                    };

                    return Ok(IndexSelector::RocksDb(IndexSelectorRocksDb {
                        db,
                        is_appendable: self.storage_type.is_appendable(),
                    }));
                }

                #[cfg(not(feature = "rocksdb"))]
                return Err(OperationError::service_error(
                    "Loading payload index failed: Index is RocksDB but RocksDB feature is disabled.",
                ));
            }
            payload_config::StorageType::Mmap { is_on_disk } => {
                IndexSelector::Mmap(IndexSelectorMmap {
                    dir: &self.path,
                    is_on_disk,
                })
            }
        };

        Ok(selector)
    }

    pub fn get_facet_index(&self, key: &JsonPath) -> OperationResult<FacetIndexEnum<'_>> {
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
        self.config.indices.to_schemas()
    }

    fn build_index(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildIndexResult> {
        if let Some(prev_schema) = self.config.indices.get(field) {
            // the field is already indexed with the same schema
            // no need to rebuild index and to save the config
            return if prev_schema.schema == *payload_schema {
                Ok(BuildIndexResult::AlreadyBuilt)
            } else {
                Ok(BuildIndexResult::IncompatibleSchema)
            };
        }
        let indexes = self.build_field_indexes(field, payload_schema, hw_counter)?;
        Ok(BuildIndexResult::Built(indexes))
    }

    fn apply_index(
        &mut self,
        field: PayloadKeyType,
        payload_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<()> {
        let index_types: Vec<_> = field_index
            .iter()
            .map(|i| i.get_full_index_type())
            .collect();
        self.field_indexes.insert(field.clone(), field_index);

        self.config.indices.insert(
            field,
            PayloadFieldSchemaWithIndexType::new(payload_schema, index_types),
        );

        self.save_config()?;

        Ok(())
    }

    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_schema: impl Into<PayloadFieldSchema>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let payload_schema = payload_schema.into();

        self.drop_index_if_incompatible(field, &payload_schema)?;

        let field_index = match self.build_index(field, &payload_schema, hw_counter)? {
            BuildIndexResult::Built(field_index) => field_index,
            BuildIndexResult::AlreadyBuilt => {
                // Index already built, no need to do anything
                return Ok(());
            }
            BuildIndexResult::IncompatibleSchema => {
                // We should have fixed it by now explicitly
                // If it is not fixed, it is a bug
                return Err(OperationError::service_error(format!(
                    "Incompatible schema for field `{field}`. Please drop the index first."
                )));
            }
        };

        self.apply_index(field.to_owned(), payload_schema, field_index)?;

        Ok(())
    }

    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<bool> {
        let removed_config = self.config.indices.remove(field);
        let removed_indexes = self.field_indexes.remove(field);

        let is_removed = removed_config.is_some() || removed_indexes.is_some();

        if let Some(indexes) = removed_indexes {
            for index in indexes {
                index.cleanup()?;
            }
        }

        self.save_config()?;

        Ok(is_removed)
    }

    fn drop_index_if_incompatible(
        &mut self,
        field: PayloadKeyTypeRef,
        new_payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool> {
        let Some(current_schema) = self.config.indices.get(field) else {
            return Ok(false);
        };

        // the field is already indexed with the same schema
        // no need to rebuild index and to save the config
        if current_schema.schema == *new_payload_schema {
            return Ok(false);
        }

        self.drop_index(field)
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
            None => Box::new(std::iter::empty()),
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

    fn get_payload_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.payload.borrow().get_sequential(point_id, hw_counter)
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
        // Most field indices have either 2 or 3 indices (including null), we also have an extra
        // payload storage flusher. Overallocate to save potential reallocations.
        let mut flushers = Vec::with_capacity(self.field_indexes.len() * 3 + 1);

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
                        debug_assert!(
                            false,
                            "Missing column family should not happen during testing",
                        );
                    }
                    Err(err) => {
                        return Err(OperationError::service_error(format!(
                            "Failed to flush payload_index: {err}",
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    #[cfg(feature = "rocksdb")]
    fn take_database_snapshot(&self, path: &Path) -> OperationResult<()> {
        match &self.storage_type {
            StorageType::RocksDbAppendable(db) | StorageType::RocksDbNonAppendable(db) => {
                let db_guard = db.read();
                crate::rocksdb_backup::create(&db_guard, path)
            }
            StorageType::GridstoreAppendable | StorageType::GridstoreNonAppendable => Ok(()),
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

    fn immutable_files(&self) -> Vec<(PayloadKeyType, PathBuf)> {
        self.field_indexes
            .iter()
            .flat_map(|(key, indexes)| {
                indexes.iter().flat_map(|index| {
                    index
                        .immutable_files()
                        .into_iter()
                        .map(|file| (key.clone(), file))
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::AtomicBool;

    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::only_default_vector;
    use crate::entry::SegmentEntry;
    use crate::index::payload_config::{IndexMutability, PayloadIndexType};
    use crate::segment_constructor::load_segment;
    use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
    use crate::types::{Distance, PayloadSchemaType};

    #[test]
    fn test_load_payload_index() {
        let data = r#"
               {
                   "name": "John Doe"
               }"#;

        let dir = Builder::new().prefix("payload_dir").tempdir().unwrap();
        let dim = 2;

        let hw_counter = HardwareCounterCell::new();

        let key = JsonPath::from_str("name").unwrap();

        let full_segment_path = {
            let mut segment = build_simple_segment(dir.path(), dim, Distance::Dot).unwrap();
            segment
                .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
                .unwrap();

            let payload: Payload = serde_json::from_str(data).unwrap();

            segment
                .set_full_payload(0, 0.into(), &payload, &hw_counter)
                .unwrap();

            segment
                .create_field_index(
                    0,
                    &key,
                    Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
                    &HardwareCounterCell::new(),
                )
                .unwrap();

            segment.current_path.clone()
        };

        let check_index_types = |index_types: &[FullPayloadIndexType]| -> bool {
            index_types.len() == 2
                && index_types[0].index_type == PayloadIndexType::KeywordIndex
                && index_types[0].mutability == IndexMutability::Mutable
                && index_types[1].index_type == PayloadIndexType::NullIndex
                && index_types[1].mutability == IndexMutability::Mutable
        };

        let payload_config_path = full_segment_path.join("payload_index/config.json");
        let mut payload_config = PayloadConfig::load(&payload_config_path).unwrap();

        assert_eq!(payload_config.indices.len(), 1);

        let schema = payload_config.indices.get_mut(&key).unwrap();
        check_index_types(&schema.types);

        // Clear index types to check loading from an old segment.
        schema.types.clear();
        payload_config.save(&payload_config_path).unwrap();
        drop(payload_config);

        // Load once and drop.
        {
            load_segment(&full_segment_path, &AtomicBool::new(false))
                .unwrap()
                .unwrap();
        }

        // Check that index type has been written to disk again.
        // Proves we'll always persist the exact index type if it wasn't known yet at that time
        let payload_config = PayloadConfig::load(&payload_config_path).unwrap();
        assert_eq!(payload_config.indices.len(), 1);

        let schema = payload_config.indices.get(&key).unwrap();
        check_index_types(&schema.types);
    }
}
