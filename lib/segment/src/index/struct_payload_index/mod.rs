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
use super::field_index::index_selector::{
    IndexSelector, IndexSelectorGridstore, IndexSelectorMmap,
};
use super::payload_config::{FullPayloadIndexType, PayloadFieldSchemaWithIndexType};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::IndexesMap;
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::payload_config::{self, PayloadConfig};
use crate::index::visited_pool::VisitedPool;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::types::{PayloadFieldSchema, PayloadKeyType, VectorNameBuf};
use crate::vector_storage::VectorStorageEnum;

#[derive(Debug)]
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

    /// Attempt the in-place on_disk swap fast path for a field whose
    /// schema differs from the previous one only in `on_disk`.
    ///
    /// Returns:
    /// - `Ok(true)`: swap succeeded (or was a config-only update on an
    ///   appendable Gridstore segment, where the storage layer doesn't
    ///   honor `on_disk`). The new schema has been persisted.
    /// - `Ok(false)`: swap is not applicable (at least one `FieldIndex`
    ///   variant didn't support it). Any siblings that were already
    ///   swapped have been rolled back. Caller should fall through to the
    ///   legacy drop-and-rebuild path.
    /// - `Err(_)`: swap failed mid-way. Successfully swapped siblings
    ///   were rolled back on a best-effort basis before the error
    ///   propagated.
    pub fn try_swap_on_disk(
        &mut self,
        field: &PayloadKeyType,
        new_on_disk: bool,
        new_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool> {
        // Appendable Gridstore segments don't honor `on_disk` at the
        // storage layer (the index selector is Gridstore, which has no
        // `is_on_disk` flag). The new value gets persisted in the schema
        // so the optimizer-produced non-appendable segment will pick it
        // up at conversion time.
        if matches!(self.storage_type, StorageType::GridstoreAppendable) {
            self.update_indexed_schema(field, new_schema.clone())?;
            return Ok(true);
        }

        let outcome = {
            let Some(indexes) = self.field_indexes.get_mut(field) else {
                return Ok(false);
            };

            let mut swapped_count = 0usize;
            let mut not_applicable = false;
            let mut error: Option<OperationError> = None;

            for index in indexes.iter_mut() {
                match index.swap_on_disk(new_on_disk) {
                    Ok(true) => swapped_count += 1,
                    Ok(false) => {
                        not_applicable = true;
                        break;
                    }
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }

            if not_applicable || error.is_some() {
                let prev_on_disk = !new_on_disk;
                for index in indexes.iter_mut().take(swapped_count) {
                    // Best-effort rollback. We propagate the original
                    // error (or the not-applicable outcome) regardless of
                    // whether rollback itself errors — there's no good
                    // recovery here, and a rollback error is strictly
                    // less informative than the original failure.
                    let _ = index.swap_on_disk(prev_on_disk);
                }
                if let Some(e) = error {
                    return Err(e);
                }
                false
            } else {
                true
            }
        };

        if outcome {
            self.update_indexed_schema(field, new_schema.clone())?;
        }
        Ok(outcome)
    }

    /// Re-persist the schema for an already-existing field without
    /// rebuilding its index. The on-disk index files are untouched; only
    /// `config.indices[field]` is updated to reflect the new schema and
    /// (re-derived) per-index types.
    fn update_indexed_schema(
        &mut self,
        field: &PayloadKeyType,
        new_schema: PayloadFieldSchema,
    ) -> OperationResult<()> {
        let index_types: Vec<_> = self
            .field_indexes
            .get(field)
            .map(|indexes| indexes.iter().map(|i| i.get_full_index_type()).collect())
            .unwrap_or_default();
        self.config.indices.insert(
            field.clone(),
            PayloadFieldSchemaWithIndexType::new(new_schema, index_types),
        );
        self.save_config()?;
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
            StructPayloadIndexReadView<
                '_,
                PayloadStorageEnum,
                IdTrackerEnum,
                VectorStorageEnum,
                FieldIndex,
            >,
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
