mod build;
mod payload_index;
mod read_view;

pub use read_view::StructPayloadIndexReadView;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::defaults::log_load_timing;
use fs_err as fs;

use super::field_index::FieldIndex;
use super::field_index::facet_index::FacetIndexEnum;
use super::field_index::index_selector::{
    IndexSelector, IndexSelectorGridstore, IndexSelectorMmap,
};
use super::payload_config::{FullPayloadIndexType, PayloadFieldSchemaWithIndexType};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::IndexesMap;
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::payload_config::{self, PayloadConfig};
use crate::index::visited_pool::VisitedPool;
use crate::json_path::JsonPath;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::types::{PayloadFieldSchema, PayloadKeyType, VectorNameBuf};
use crate::vector_storage::VectorStorageEnum;

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum StorageType {
    GridstoreAppendable,
    GridstoreNonAppendable,
}

/// `PayloadIndex` implementation, which actually uses index structures for providing faster search
#[derive(Debug)]
pub struct StructPayloadIndex {
    /// Payload storage
    pub(super) payload: Arc<AtomicRefCell<PayloadStorageEnum>>,
    /// Used for `has_id` condition and estimating cardinality
    pub(super) id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    /// Vector storages for each field, used for `has_vector` condition
    pub(super) vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
    /// Indexes, associated with fields
    pub field_indexes: IndexesMap,
    config: PayloadConfig,
    /// Root of index persistence dir
    path: PathBuf,
    /// Used to select unique point ids
    pub(super) visited_pool: VisitedPool,
    /// Desired storage type for payload indices, used in builder to pick correct type
    storage_type: StorageType,
}

impl StructPayloadIndex {
    fn config_path(&self) -> PathBuf {
        PayloadConfig::get_config_path(&self.path)
    }

    pub(super) fn save_config(&self) -> OperationResult<()> {
        let config_path = self.config_path();
        self.config.save(&config_path)
    }

    fn load_all_fields(&mut self, create_if_missing: bool) -> OperationResult<()> {
        let mut field_indexes: IndexesMap = Default::default();

        let mut indices = std::mem::take(&mut self.config.indices);
        let mut is_dirty = false;

        for (field, payload_schema) in indices.iter_mut() {
            let started = Instant::now();
            let (field_index, dirty) =
                self.load_from_db(field, payload_schema, create_if_missing)?;
            log_load_timing(&self.path, &format!("field `{field}`"), started);
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

    fn load_from_db(
        &self,
        field: &PayloadKeyType,
        // TODO: refactor this and remove the &mut reference.
        payload_schema: &mut PayloadFieldSchemaWithIndexType,
        create_if_missing: bool,
    ) -> OperationResult<(Vec<FieldIndex>, bool)> {
        let id_tracker_borrow = self.id_tracker.borrow();
        let deleted_points = id_tracker_borrow.deleted_point_bitslice();
        let mut rebuild = false;
        let mut is_dirty = false;

        let mut indexes = if payload_schema.types.is_empty() {
            let selector = self.selector(&payload_schema.schema);
            let indexes = selector.new_index(
                field,
                &payload_schema.schema,
                create_if_missing,
                deleted_points,
            )?;

            if let Some(mut indexes) = indexes {
                debug_assert!(
                    !indexes
                        .iter()
                        .any(|index| matches!(index, FieldIndex::NullIndex(_))),
                    "index selector is not expected to provide null index",
                );

                // Special null index complements every index.
                if let Some(null_index) = selector.new_null_index(
                    field,
                    create_if_missing,
                    &id_tracker_borrow,
                    selector.default_mutability(),
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
                            create_if_missing,
                            &id_tracker_borrow,
                            deleted_points,
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

        // TODO(rocksdb): review leftover code in this function

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
        id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
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
            PayloadConfig::default()
        };

        let storage_type = if is_appendable {
            StorageType::GridstoreAppendable
        } else {
            StorageType::GridstoreNonAppendable
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

        index.load_all_fields(create)?;

        Ok(index)
    }

    /// Register a vector storage for the `has_vector` filtering condition.
    ///
    /// Must be called whenever a new named vector is added to the segment after the
    /// payload index has been opened, otherwise `has_vector` queries will see stale
    /// state (no matches for the new vector) until the segment is reloaded.
    pub fn register_vector_storage(
        &mut self,
        vector_name: VectorNameBuf,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    ) {
        self.vector_storages.insert(vector_name, vector_storage);
    }

    /// Drop a vector storage from the `has_vector` lookup map.
    ///
    /// Must be called whenever a named vector is removed from the segment, otherwise
    /// `has_vector` queries will keep matching points against the deleted storage
    /// until the segment is reloaded.
    pub fn unregister_vector_storage(&mut self, vector_name: &str) {
        self.vector_storages.remove(vector_name);
    }

    /// Number of available points
    ///
    /// - excludes soft deleted points
    pub fn available_point_count(&self) -> usize {
        self.id_tracker.borrow().available_point_count()
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

    /// Select which type of PayloadIndex to use for the field
    pub(super) fn selector(&self, payload_schema: &PayloadFieldSchema) -> IndexSelector<'_> {
        let is_on_disk = payload_schema.is_on_disk();

        match &self.storage_type {
            StorageType::GridstoreAppendable => {
                IndexSelector::Gridstore(IndexSelectorGridstore { dir: &self.path })
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
        for field_indexes in self.field_indexes.values() {
            for index in field_indexes {
                index.populate()?;
            }
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        for field_indexes in self.field_indexes.values() {
            for index in field_indexes {
                index.clear_cache()?;
            }
        }
        Ok(())
    }

    pub fn clear_cache_if_on_disk(&self) -> OperationResult<()> {
        for field_indexes in self.field_indexes.values() {
            for index in field_indexes {
                if index.is_on_disk() {
                    index.clear_cache()?;
                }
            }
        }
        Ok(())
    }

    /// Run a closure with a borrowed read-only view of this index.
    ///
    /// The view borrows `id_tracker` (and the payload `Arc`) for the
    /// duration of the closure, collapsing the per-method
    /// `AtomicRefCell::borrow()` cost into a single up-front borrow.
    /// All `PayloadIndexRead` methods are available on the view.
    pub fn with_view<R>(
        &self,
        f: impl FnOnce(
            StructPayloadIndexReadView<'_, PayloadStorageEnum, IdTrackerEnum, VectorStorageEnum>,
        ) -> R,
    ) -> R {
        let id_tracker = self.id_tracker.borrow();
        let view = StructPayloadIndexReadView {
            payload: &self.payload,
            id_tracker: &*id_tracker,
            vector_storages: &self.vector_storages,
            field_indexes: &self.field_indexes,
            config: &self.config,
            visited_pool: &self.visited_pool,
        };
        f(view)
    }
}
